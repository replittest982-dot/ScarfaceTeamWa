import asyncio
import logging
import sys
import os
import re
import csv
import io
import time
from datetime import datetime, timedelta, timezone

# --- LIBS ---
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, 
    BufferedInputFile, ReactionTypeEmoji
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- REDIS SETUP (FIXED) ---
try:
    from aiogram.fsm.storage.redis import RedisStorage
    from redis.asyncio import Redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

# --- CONFIG ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    sys.exit("❌ FATAL: BOT_TOKEN is missing")

ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "scarface_v31.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

# --- UTILS ---
def get_utc_now():
    return datetime.now(timezone.utc).isoformat()

def get_local_time_str(iso_str):
    # Конвертация UTC -> GMT+5 (Актобе/КЗ)
    if not iso_str: return "-"
    try:
        dt = datetime.fromisoformat(iso_str)
        local_dt = dt + timedelta(hours=5) 
        return local_dt.strftime("%Y-%m-%d %H:%M (GMT+5)")
    except: return iso_str

def calc_duration(start_iso, end_iso):
    if not start_iso or not end_iso: return "?"
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        minutes = int((e - s).total_seconds() / 60)
        return f"{minutes} мин."
    except: return "?"

def clean_phone(phone: str):
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10 and clean.isdigit(): clean = '7' + clean
    if not re.match(r'^7\d{10}$|^77\d{9,10}$', clean): return None
    return '+' + clean

def mask_phone(phone, user_id):
    if user_id == ADMIN_ID: return phone
    try:
        return f"{phone[:4]}****{phone[-3:]}"
    except: return phone

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class SupportState(StatesGroup):
    waiting_question = State()
    waiting_reply = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_tariff_val = State()

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            method TEXT, tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, 
            status TEXT, worker_id INTEGER DEFAULT 0, code_received TEXT,
            start_time TIMESTAMP, end_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, 
            price TEXT, hold_info TEXT, work_start TEXT DEFAULT '00:00', work_end TEXT DEFAULT '23:59')""")
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs (name, price, hold_info) VALUES ('WhatsApp', '50', '1h'), ('MAX', '150', '2h')")
        
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        await db.commit()
        logger.info("✅ DB INITIALIZED")

# --- KEYBOARDS ---
def main_menu_kb(user_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Сдать номер", callback_data="select_tariff")
    builder.button(text="👤 Профиль", callback_data="menu_profile")
    builder.button(text="ℹ️ Помощь", callback_data="menu_guide") # Новая кнопка
    
    if ADMIN_ID and user_id == ADMIN_ID:
        builder.button(text="⚡️ Админ панель", callback_data="admin_panel_start")
        
    builder.adjust(1, 2, 1)
    return builder.as_markup()

def worker_kb(num_id, tariff_name):
    kb = InlineKeyboardBuilder()
    if "MAX" in str(tariff_name).upper():
        kb.button(text="Встал ✅", callback_data=f"w_act_{num_id}")
        kb.button(text="Пропуск ⏭", callback_data=f"w_skip_{num_id}")
    else:
        kb.button(text="Встал ✅", callback_data=f"w_act_{num_id}")
        kb.button(text="Ошибка ❌", callback_data=f"w_err_{num_id}")
    return kb.as_markup()

def worker_active_kb(num_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📉 Слет", callback_data=f"w_drop_{num_id}")
    return kb.as_markup()

# ==========================================
# 1. КОМАНДЫ (САМЫЙ ВЫСОКИЙ ПРИОРИТЕТ)
# ==========================================

@router.message(CommandStart())
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id=?", (uid,)) as c: res = await c.fetchone()
        
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", 
                            (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>Запрос доступа:</b> {uid} (@{m.from_user.username})", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте подтверждения доступа.</b>", parse_mode="HTML")

        if res[0]:
            await m.answer(f"👋 Привет, <b>{m.from_user.first_name}</b>!", reply_markup=main_menu_kb(uid), parse_mode="HTML")
        else:
            await m.answer("⏳ <b>Заявка на рассмотрении...</b>", parse_mode="HTML")

# --- ADMIN COMMANDS ---
@router.message(Command("startwork"))
async def start_work(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=r[0], callback_data=f"bind_{r[0]}")
    await m.answer("⚙️ <b>Выберите тариф для топика:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.message(Command("stopwork"))
async def stop_work(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    key = f"topic_cfg_{m.chat.id}_{m.message_thread_id if m.is_topic_message else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM config WHERE key=?", (key,))
        await db.commit()
    await m.reply("🛑 Топик отвязан.")

# --- WORKER COMMANDS ---
@router.message(Command("num"))
async def worker_get_num(m: types.Message, bot: Bot):
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    
    async with aiosqlite.connect(DB_NAME, timeout=10) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (f"topic_cfg_{cid}_{tid}",)) as cur: 
            conf = await cur.fetchone()
        if not conf: return
        t_name = conf[0]
        
        # Блокировка (LIMIT 1)
        async with db.execute("SELECT id, phone, tariff_price, tariff_hold, user_id FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (t_name,)) as cur:
            row = await cur.fetchone()
            
        if not row: return await m.reply("📭 <b>Очередь пуста!</b>", parse_mode="HTML")
        nid, ph, price, hold, uid = row
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", (m.from_user.id, get_utc_now(), nid))
        await db.commit()

    # Worker View
    txt = (f"🚀 <b>ВЫ ВЗЯЛИ НОМЕР</b>\n"
           f"━━━━━━━━━━━━━\n"
           f"📱 <code>{ph}</code>\n"
           f"━━━━━━━━━━━━━")
    
    if "MAX" in t_name.upper():
        txt += "\nℹ️ <i>Это MAX. Используй /code +номер для запроса кода</i>"
    else:
        txt += f"\nКод: <code>/sms {ph} текст</code>"

    await m.answer(txt, reply_markup=worker_kb(nid, t_name), parse_mode="HTML")
    
    # Notify User
    try: await bot.send_message(uid, f"⚡ <b>Ваш номер взяли в работу!</b>\n📱 {mask_phone(ph, uid)}\nОжидайте код.", parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def worker_req_code(m: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Формат: <code>/code +7...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, id, worker_id FROM numbers WHERE phone=? AND status='work'", (ph,)) as cur: res = await cur.fetchone()
    
    if not res: return await m.reply("❌ Номер не в работе")
    uid, nid, wid = res
    if wid != m.from_user.id: return await m.reply("❌ Чужой номер!")
    
    try:
        await bot.send_message(uid, 
            f"🔔 <b>Офис запросил номер!</b>\n📱 {mask_phone(ph, uid)}\n\n👇 <b>Ответьте на это сообщение кодом (или фото)</b>", 
            parse_mode="HTML")
        await m.reply(f"✅ Запрос отправлен юзеру.")
    except:
        await m.reply("❌ Не удалось отправить пуш юзеру.")

# ==========================================
# 2. ФОТО (ДЛЯ СДАЧИ КОДОВ)
# ==========================================
@router.message(F.photo)
async def photo_router(m: types.Message, bot: Bot):
    # Логика SMS через фото
    if m.caption and "/sms" in m.caption.lower():
        try:
            parts = m.caption.strip().split()
            cmd_idx = -1
            for i, p in enumerate(parts):
                if p.lower().startswith("/sms"):
                    cmd_idx = i
                    break
            
            if cmd_idx == -1 or len(parts) < cmd_idx + 2:
                return await m.reply("⚠️ Формат: <code>/sms номер текст</code>", parse_mode="HTML")

            ph_raw = parts[cmd_idx+1]
            tx_raw = " ".join(parts[cmd_idx+2:]) if len(parts) > cmd_idx+2 else "Код на фото"
            ph = clean_phone(ph_raw)
            if not ph: return await m.reply("❌ Кривой номер")

            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT user_id, id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: res = await cur.fetchone()
            
            if res:
                uid, nid = res
                await bot.send_photo(chat_id=uid, photo=m.photo[-1].file_id, 
                                     caption=f"🔔 <b>SMS / Код</b>\n━━━━━━━━━━━━━\n📱 <code>{ph}</code>\n💬 <tg-spoiler>{tx_raw}</tg-spoiler>", parse_mode="HTML")
                await m.react([types.ReactionTypeEmoji(emoji="🔥")])
            else:
                await m.reply("🚫 Номер не в работе")
        except: pass
        return

    # Логика ответа юзера на MAX запрос (Фото кода)
    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем, есть ли у юзера активный MAX заказ
        async with db.execute("SELECT worker_id, phone FROM numbers WHERE user_id=? AND status='work' AND tariff_name LIKE '%MAX%'", (m.from_user.id,)) as cur:
            res = await cur.fetchone()
            
    if res:
        wid, ph = res
        if wid != 0:
            try:
                await bot.send_photo(chat_id=wid, photo=m.photo[-1].file_id, caption=f"📩 <b>Фото от юзера (MAX)</b>\n📱 {ph}", parse_mode="HTML")
                await m.answer("✅ Фото отправлено офису.")
            except: pass

# ==========================================
# 3. ТЕКСТ (ПОДДЕРЖКА И SMS КОМАНДЫ)
# ==========================================
@router.message(F.text)
async def text_router(m: types.Message, state: FSMContext, bot: Bot):
    # Команды обрабатываются выше, но если /sms написано текстом:
    if m.text.lower().startswith("/sms"):
        try:
            parts = m.text.split(None, 2)
            if len(parts) < 3: return await m.reply("⚠️ <code>/sms номер код</code>", parse_mode="HTML")
            ph = clean_phone(parts[1])
            tx = parts[2]
            
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: res = await cur.fetchone()
            
            if res:
                await bot.send_message(res[0], f"🔔 <b>SMS / Код</b>\n━━━━━━━━━━━━━\n📱 <code>{ph}</code>\n💬 <tg-spoiler>{tx}</tg-spoiler>", parse_mode="HTML")
                await m.react([types.ReactionTypeEmoji(emoji="🔥")])
            else:
                await m.reply("🚫 Нет в работе")
        except: pass
        return

    # Логика Поддержки (Юзер задает вопрос)
    curr_state = await state.get_state()
    if curr_state == SupportState.waiting_question:
        if m.text.lower() in ["отмена", "/start"]:
            await state.clear()
            return await m.answer("Отменено.")
        
        if ADMIN_ID:
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="↩️ Ответить", callback_data=f"reply_{m.from_user.id}")]])
            await bot.send_message(ADMIN_ID, f"📩 <b>Тикет от</b> @{m.from_user.username} (ID: {m.from_user.id}):\n\n{m.text}", reply_markup=kb, parse_mode="HTML")
            await m.answer("✅ <b>Запрос отправлен!</b>\nАдминистратор скоро ответит.", reply_markup=main_menu_kb(m.from_user.id), parse_mode="HTML")
        else:
            await m.answer("❌ Админ не настроен.")
        await state.clear()
        return

    # Логика Поддержки (Админ отвечает)
    if curr_state == SupportState.waiting_reply:
        data = await state.get_data()
        target = data.get('target_id')
        try:
            await bot.send_message(target, f"👨‍💻 <b>Ответ поддержки:</b>\n\n{m.text}", parse_mode="HTML")
            await m.answer("✅ Ответ ушел.")
        except: await m.answer("❌ Не доставлено.")
        await state.clear()
        return

    # Логика MAX (Юзер присылает код текстом)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, phone FROM numbers WHERE user_id=? AND status='work' AND tariff_name LIKE '%MAX%'", (m.from_user.id,)) as cur:
            res = await cur.fetchone()
    if res:
        wid, ph = res
        if wid != 0:
            try:
                await bot.send_message(wid, f"📩 <b>Код от юзера (MAX)</b>\n📱 {ph}\n💬 <code>{m.text}</code>", parse_mode="HTML")
                await m.react([types.ReactionTypeEmoji(emoji="👍")])
            except: pass

# ==========================================
# 4. CALLBACKS
# ==========================================

@router.callback_query(F.data == "menu_guide")
async def show_help(c: CallbackQuery):
    txt = ("📲 <b>Что делает бот</b>\n"
           "Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
           "📦 <b>Требования к номерам</b>\n"
           "✔️ Активный и чистый номер\n"
           "✔️ Доступ к SMS\n"
           "❌ Виртуальные, заблокированные номера не принимаются\n\n"
           "⏳ <b>Холд и выплаты</b>\n"
           "Холд — время проверки номера.\n"
           "💰 Выплата производится после успешного завершения холда.\n\n"
           "⚠️ <i>Отправляя номер, вы подтверждаете согласие с правилами.</i>")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🆘 Написать в поддержку", callback_data="support_start")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]
    ])
    await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "support_start")
async def supp_init(c: CallbackQuery, state: FSMContext):
    await c.message.answer("📝 <b>Опишите вашу проблему или вопрос:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="nav_main")]]), parse_mode="HTML")
    await state.set_state(SupportState.waiting_question)
    await c.answer()

@router.callback_query(F.data.startswith("reply_"))
async def adm_rep_init(c: CallbackQuery, state: FSMContext):
    uid = int(c.data.split("_")[1])
    await state.update_data(target_id=uid)
    await c.message.answer(f"✍️ <b>Введите ответ для ID {uid}:</b>", parse_mode="HTML")
    await state.set_state(SupportState.waiting_reply)
    await c.answer()

@router.callback_query(F.data == "select_tariff")
async def sel_trf(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, price FROM tariffs") as cur: rows = await cur.fetchall()
    
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=f"{r[0]} | {r[1]}₽", callback_data=f"pick_{r[0]}")
    kb.button(text="🔙 Назад", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text("💰 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("pick_"))
async def pick_trf(c: CallbackQuery, state: FSMContext):
    t = c.data.split("pick_")[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t,)) as cur: res = await cur.fetchone()
    if not res: return await c.answer("Удалено", show_alert=True)
    
    await state.update_data(tariff=t, price=res[0], hold=res[1])
    kb = InlineKeyboardBuilder()
    kb.button(text="💬 SMS", callback_data="m_sms")
    kb.button(text="📷 QR", callback_data="m_qr")
    kb.button(text="🔙", callback_data="select_tariff")
    kb.adjust(2, 1)
    await c.message.edit_text(f"💎 Тариф: <b>{t}</b>\n⏳ Холд: {res[1]}", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.in_({"m_sms", "m_qr"}))
async def inp_method(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == 'm_sms' else 'qr')
    await c.message.edit_text("📱 <b>Введите номера (списком):</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]]), parse_mode="HTML")
    await state.set_state(UserState.waiting_for_number)
    await c.answer()

@router.message(UserState.waiting_for_number)
async def proc_nums(m: types.Message, state: FSMContext):
    d = await state.get_data()
    raw = m.text.split(',')
    valid = []
    for x in raw:
        cl = clean_phone(x.strip())
        if cl: valid.append(cl)
    
    if not valid: return await m.answer("❌ Нет номеров")
    
    async with aiosqlite.connect(DB_NAME) as db:
        for ph in valid:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)", 
                             (m.from_user.id, ph, d['method'], d['tariff'], d['price'], d['hold'], get_utc_now()))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ <b>Загружено:</b> {len(valid)} шт.", reply_markup=main_menu_kb(m.from_user.id), parse_mode="HTML")

@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("👋 <b>Главное меню</b>", reply_markup=main_menu_kb(c.from_user.id), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "menu_profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (uid,)) as cur: reg = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,)) as cur: total = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='finished'", (uid,)) as cur: done = (await cur.fetchone())[0]
    
    reg_fmt = get_local_time_str(reg).split()[0]
    txt = (f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━\n"
           f"📅 Регистрация: {reg_fmt}\n"
           f"📦 Всего загружено: {total}\n"
           f"✅ Успешно: {done}\n━━━━━━━━━━━━━")
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]]), parse_mode="HTML")
    await c.answer()

# --- WORKER CALLBACKS ---
@router.callback_query(F.data.startswith("bind_"))
async def bind_topic(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return await c.answer()
    t = c.data.split("_")[1]
    key = f"topic_cfg_{c.message.chat.id}_{c.message.message_thread_id if c.message.is_topic_message else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t))
        await db.commit()
    
    guide = (f"✅ <b>Чат привязан!</b> Тариф: {t}\n\n"
             "👨‍💻 <b>Гайд по использованию:</b>\n"
             "1️⃣ Пиши /num -> Получишь номер.\n"
             "2️⃣ Вбей номер в WhatsApp Web.\n"
             "3️⃣ Если просят QR: Сфоткай и скинь: <code>/sms +77... Сканируй</code>\n"
             "4️⃣ Если просят Код: Сфоткай и скинь: <code>/sms +77... Код</code>\n"
             "5️⃣ Когда зашел -> жми ✅ Встал.\n"
             "6️⃣ Когда номер слетел -> жми 📉 Слет.")
    await c.message.edit_text(guide, parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, phone, tariff_name, user_id FROM numbers WHERE id=?", (nid,)) as cur: res = await cur.fetchone()
    
    if not res: return await c.answer("Ошибка")
    wid, ph, tname, uid = res
    if wid != c.from_user.id: return await c.answer("❌ Чужой номер", show_alert=True)
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
        
    await c.message.edit_text(f"🟢 <b>Номер встал</b>\n📱 {ph}", reply_markup=worker_active_kb(nid), parse_mode="HTML")
    try: await bot.send_message(uid, f"✅ <b>Номер успешно встал!</b>\nНачинается холд.", parse_mode="HTML")
    except: pass
    await c.answer()

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def w_fin(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    status = "finished" if is_drop else "dead"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, phone, user_id, start_time FROM numbers WHERE id=?", (nid,)) as cur: res = await cur.fetchone()
        
    if not res: return
    wid, ph, uid, start = res
    if wid != c.from_user.id: return await c.answer("❌ Чужой номер", show_alert=True)
    
    end = get_utc_now()
    dur = calc_duration(start, end)
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, end, nid))
        await db.commit()
        
    if is_drop:
        msg_user = f"📉 <b>Ваш номер слетел</b>\n⏰ Время работы: {dur}"
        msg_work = f"📉 <b>Номер слетел</b>\n📱 {ph}\n⏰ {dur}"
    else:
        msg_user = f"❌ <b>Ошибка</b>\n📱 {mask_phone(ph, uid)}"
        msg_work = f"❌ <b>Ошибка</b>\n📱 {ph}"
        
    try: await bot.send_message(uid, msg_user, parse_mode="HTML")
    except: pass
    await c.message.edit_text(msg_work, parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, user_id FROM numbers WHERE id=?", (nid,)) as cur: res = await cur.fetchone()
    
    if not res: return
    wid, uid = res
    if wid != c.from_user.id: return await c.answer("❌ Чужой номер")
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text("⏭ <b>Пропуск</b>", parse_mode="HTML")
    try: await bot.send_message(uid, "⚠️ Офис пропустил ваш номер, он вернулся в очередь.")
    except: pass
    await c.answer()

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_menu(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Изменить тарифы", callback_data="adm_edittrf")
    kb.button(text="📄 Отчеты", callback_data="adm_reps")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙 Меню", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "adm_edittrf")
async def adm_edittrf(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, price, work_start, work_end FROM tariffs") as cur: rows = await cur.fetchall()
    
    text = "📋 <b>Тарифы:</b>\n"
    for r in rows: text += f"🔹 {r[1]}: {r[2]}₽ ({r[3]}-{r[4]})\n"
    
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=f"✏️ {r[1]}", callback_data=f"trfedit_{r[1]}")
    kb.button(text="🔙", callback_data="admin_panel_start")
    kb.adjust(2, 1)
    await c.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("trfedit_"))
async def adm_trf_sel(c: CallbackQuery, state: FSMContext):
    t = c.data.split("_")[1]
    await state.update_data(target=t)
    kb = InlineKeyboardBuilder()
    kb.button(text="💵 Прайс", callback_data="set_price")
    kb.button(text="⏰ Время", callback_data="set_time")
    await c.message.edit_text(f"⚙️ <b>{t}</b>\nЧто меняем?", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("set_"))
async def adm_set_field(c: CallbackQuery, state: FSMContext):
    f = c.data.split("_")[1]
    await state.update_data(field=f)
    await state.set_state(AdminState.edit_tariff_val)
    msg = "Введите цену:" if f == "price" else "Введите время (09:00-21:00):"
    await c.message.edit_text(msg)
    await c.answer()

@router.message(AdminState.edit_tariff_val)
async def adm_save_val(m: types.Message, state: FSMContext):
    d = await state.get_data()
    t = d['target']
    f = d['field']
    v = m.text
    async with aiosqlite.connect(DB_NAME) as db:
        if f == "price": await db.execute("UPDATE tariffs SET price=? WHERE name=?", (v, t))
        else:
            try:
                s, e = v.split("-")
                await db.execute("UPDATE tariffs SET work_start=?, work_end=? WHERE name=?", (s.strip(), e.strip(), t))
            except: return await m.reply("Ошибка формата!")
        await db.commit()
    await state.clear()
    await m.answer(f"✅ Тариф {t} обновлен.", reply_markup=main_menu_kb(m.from_user.id))

@router.callback_query(F.data == "adm_reps")
async def adm_reps(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="24 ч", callback_data="rep_24")
    kb.button(text="48 ч", callback_data="rep_48")
    kb.button(text="120 ч", callback_data="rep_120")
    kb.button(text="Все", callback_data="rep_all")
    kb.button(text="🔙", callback_data="admin_panel_start")
    kb.adjust(3, 1, 1)
    await c.message.edit_text("📊 <b>Период отчета:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("rep_"))
async def adm_gen_rep(c: CallbackQuery):
    h = c.data.split("_")[1]
    
    async with aiosqlite.connect(DB_NAME) as db:
        if h == "all":
            sql = "SELECT id, phone, status, tariff_name, created_at FROM numbers ORDER BY id DESC"
            params = ()
        else:
            cut = (datetime.now(timezone.utc) - timedelta(hours=int(h))).isoformat()
            sql = "SELECT id, phone, status, tariff_name, created_at FROM numbers WHERE created_at >= ? ORDER BY id DESC"
            params = (cut,)
            
        async with db.execute(sql, params) as cur: rows = await cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Phone', 'Status', 'Tariff', 'Date (GMT+5)'])
    for r in rows:
        writer.writerow([r[0], r[1], r[2], r[3], get_local_time_str(r[4])])
    
    output.seek(0)
    doc = BufferedInputFile(output.getvalue().encode(), filename=f"report_{h}.csv")
    await c.message.answer_document(doc)
    await c.answer()

@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📢 Введите сообщение:", parse_mode="HTML")
    await state.set_state(AdminState.waiting_broadcast)
    await c.answer()

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: types.Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳ ...")
    cnt = 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cur: usrs = await cur.fetchall()
    for u in usrs:
        try:
            await m.copy_to(u[0])
            cnt += 1
            await asyncio.sleep(0.05)
        except: pass
    await msg.edit_text(f"✅ Отправлено: {cnt}")

# --- MAIN ---
async def main():
    await init_db()
    if HAS_REDIS and os.getenv("REDIS_URL"):
        storage = RedisStorage.from_url(os.getenv("REDIS_URL"))
    else:
        storage = MemoryStorage()
    
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 STARTED v31.0 FIXED")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"POLL ERR: {e}")

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
