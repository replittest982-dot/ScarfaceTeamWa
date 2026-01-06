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
from aiogram.filters import Command, CommandStart, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, 
    BufferedInputFile, ReactionTypeEmoji, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# --- REDIS SETUP ---
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
    sys.exit("❌ FATAL: BOT_TOKEN is missing in .env")

ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "fast_team_v30.db"

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log", mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
router = Router()

# Кэш
user_cooldowns = {}
TARIFF_CACHE = []  # Added cache for tariffs

# --- HELPERS ---
def get_utc_now():
    return datetime.now(timezone.utc).isoformat()

def format_dt(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        local_dt = dt + timedelta(hours=5) 
        return local_dt.strftime("%Y-%m-%d %H:%M")
    except:
        return iso_str

def calculate_duration(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        diff = e - s
        minutes = int(diff.total_seconds() / 60)
        return f"{minutes} мин."
    except:
        return "?"

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
        if len(phone) < 8: return phone
        return f"{phone[:4]}****{phone[-3:]}"
    except: return phone

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class SupportState(StatesGroup):
    waiting_question = State()
    waiting_reply = State() # Admin only

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    edit_tariff_select = State()
    edit_tariff_field = State()
    edit_tariff_value = State()

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            method TEXT, tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, 
            status TEXT, worker_id INTEGER DEFAULT 0, code_received TEXT,
            start_time TIMESTAMP, end_time TIMESTAMP, worker_msg_id INTEGER, 
            position INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st ON numbers(status)")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, 
            price TEXT, hold_info TEXT, work_start TEXT DEFAULT '00:00', work_end TEXT DEFAULT '23:59'
        )""")
        await db.execute("INSERT OR IGNORE INTO tariffs (name, price, hold_info) VALUES ('WhatsApp', '50', '1h'), ('MAX', '150', '2h')")
        
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        await db.commit()
        logger.info("✅ DB v30.0 INITIALIZED")

# --- KEYBOARDS ---
def main_menu_kb(user_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Сдать номер", callback_data="select_tariff")
    builder.button(text="👤 Профиль", callback_data="menu_profile")
    builder.button(text="ℹ️ Помощь", callback_data="menu_guide")
    builder.button(text="🆘 Поддержка", callback_data="support_start")
    
    if ADMIN_ID and user_id == ADMIN_ID:
        builder.button(text="⚡️ Админ панель", callback_data="admin_panel_start")
        
    builder.adjust(1, 2, 1, 1) 
    return builder.as_markup()

def worker_kb(num_id, tariff_name="Std"):
    kb = InlineKeyboardBuilder()
    if "MAX" in tariff_name.upper():
        kb.button(text="Встал ✅", callback_data=f"w_act_{num_id}")
        kb.button(text="Пропуск ⏭", callback_data=f"w_skip_{num_id}")
    else:
        kb.button(text="Встал ✅", callback_data=f"w_act_{num_id}")
        kb.button(text="Ошибка ❌", callback_data=f"w_err_{num_id}")
    return kb.as_markup()

def worker_active_kb(num_id, tariff_name="Std"):
    kb = InlineKeyboardBuilder()
    kb.button(text="📉 Слет", callback_data=f"w_drop_{num_id}")
    return kb.as_markup()

# ==========================================
# 1. ОБРАБОТЧИКИ КОМАНД (ВЫСОКИЙ ПРИОРИТЕТ)
# ==========================================

@router.message(Command("start"))
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
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Allow", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫 Ban", callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>New User:</b> {uid}", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте доступа</b>", parse_mode="HTML")

        if res[0]:
            await m.answer(f"👋 Привет, <b>{m.from_user.first_name}</b>!", reply_markup=main_menu_kb(uid), parse_mode="HTML")
        else:
            await m.answer("⏳ <b>На рассмотрении...</b>", parse_mode="HTML")

@router.message(Command("code"))
async def cmd_code_worker(m: types.Message, command: CommandObject, bot: Bot):
    if not command.args: 
        return await m.reply("⚠️ Формат: <code>/code +7999...</code>", parse_mode="HTML")
    
    ph = clean_phone(command.args.split()[0])
    if not ph: return await m.reply("❌ Номер?")

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, id, tariff_name, worker_id FROM numbers WHERE phone=? AND status='work'", (ph,)) as cur:
            res = await cur.fetchone()
    
    if not res: return await m.reply("❌ Номер не в работе или не ваш.")
    uid, nid, t_name, wid = res
    
    if wid != m.from_user.id: return await m.reply("❌ Чужой номер!")

    try:
        await bot.send_message(uid, f"🔔 <b>Офис запросил номер!</b>\n📱 {mask_phone(ph, uid)}\n\n👇 <b>Ответьте на это сообщение кодом (или пришлите фото)</b>", parse_mode="HTML")
        await m.reply(f"✅ Запрос отправлен юзеру на номер {ph}")
    except:
        await m.reply("❌ Не удалось отправить запрос юзеру")

# ==========================================
# 2. АДМИН ПАНЕЛЬ (СТАТУСЫ) - ПЕРЕД TEXT ROUTER
# ==========================================

@router.message(AdminState.edit_tariff_value)
async def adm_save_val(m: types.Message, state: FSMContext):
    data = await state.get_data()
    t_name = data.get('target_tariff')
    field = data.get('field')
    val = m.text.strip()
    
    async with aiosqlite.connect(DB_NAME) as db:
        if field == "price":
            await db.execute("UPDATE tariffs SET price=? WHERE name=?", (val, t_name))
        else:
            try:
                # Ожидается формат Start-End
                if '-' not in val: return await m.answer("❌ Ошибка формата! Используйте: 09:00-21:00")
                s, e = val.split('-')
                await db.execute("UPDATE tariffs SET work_start=?, work_end=? WHERE name=?", (s.strip(), e.strip(), t_name))
            except:
                return await m.answer("❌ Ошибка формата времени!")
        await db.commit()
    
    await m.answer(f"✅ Тариф <b>{t_name}</b> обновлен!", parse_mode="HTML")
    await state.clear()
    # Возврат в админку не обязателен, но можно добавить

# ==========================================
# 3. ПОДДЕРЖКА (СТАТУСЫ)
# ==========================================

@router.message(SupportState.waiting_question)
async def support_receive_q(m: types.Message, state: FSMContext, bot: Bot):
    if m.text in ["Отмена", "/start"]:
        await state.clear()
        return await m.answer("Отменено.", reply_markup=main_menu_kb(m.from_user.id))
    
    if ADMIN_ID:
        kb = InlineKeyboardBuilder()
        kb.button(text="↩️ Ответить", callback_data=f"reply_{m.from_user.id}")
        await bot.send_message(
            ADMIN_ID, 
            f"📩 <b>Новый запрос</b> от ID {m.from_user.id} (@{m.from_user.username})\n\n{m.text}", 
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
        await m.answer("✅ <b>Сообщение отправлено!</b>\nАдминистратор ответит вам в ближайшее время.", reply_markup=main_menu_kb(m.from_user.id), parse_mode="HTML")
        await state.clear()
    else:
        await m.answer("❌ Админ не настроен.")

@router.message(SupportState.waiting_reply)
async def support_send_reply(m: types.Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    target_uid = data.get('target_uid')
    try:
        await bot.send_message(target_uid, f"👨‍💻 <b>Ответ поддержки:</b>\n\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ отправлен.")
    except Exception as e:
        await m.answer(f"❌ Не удалось отправить: {e}")
    await state.clear()

@router.message(UserState.waiting_for_number)
async def proc_num(m: types.Message, state: FSMContext):
    d = await state.get_data()
    raw = m.text.split(',')
    valid = []
    for i in raw:
        cl = clean_phone(i.strip())
        if cl: valid.append(cl)
    
    if not valid: return await m.answer("❌ Нет валидных номеров.")
    
    async with aiosqlite.connect(DB_NAME) as db:
        for ph in valid:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)",
                             (m.from_user.id, ph, d['method'], d['tariff'], d['price'], d['hold'], get_utc_now()))
        await db.commit()
    await state.clear()
    await m.answer(f"✅ Загружено: {len(valid)}", reply_markup=main_menu_kb(m.from_user.id), parse_mode="HTML")

# ==========================================
# 4. ФОТО (СМС/QR)
# ==========================================
@router.message(F.photo)
async def sms_photo_handler(m: types.Message, bot: Bot):
    if not m.caption: return
    
    if "/sms" in m.caption.lower():
        # Обработка фото с подписью /sms
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
            if not ph: return await m.reply(f"❌ Номер не распознан")

            async with aiosqlite.connect(DB_NAME, timeout=10) as db:
                async with db.execute("SELECT user_id, id, tariff_name FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur:
                    res = await cur.fetchone()
            
            if res:
                uid, nid, t_name = res
                await bot.send_photo(
                    chat_id=uid, 
                    photo=m.photo[-1].file_id, 
                    caption=f"🔔 <b>SMS / Код</b>\n━━━━━━━━━━━━━\n📱 <code>{ph}</code>\n💬 <tg-spoiler>{tx_raw}</tg-spoiler>\n━━━━━━━━━━━━━", 
                    parse_mode="HTML"
                )
                await m.react([types.ReactionTypeEmoji(emoji="🔥")])
                
                async with aiosqlite.connect(DB_NAME) as db:
                    await db.execute("UPDATE numbers SET code_received=? WHERE id=?", (tx_raw, nid))
                    await db.commit()
            else:
                await m.reply(f"🚫 Номер {ph} не в работе.")
        except Exception as e:
            logger.error(f"Photo Err: {e}")

# ==========================================
# 5. ОБЩИЙ ТЕКСТОВЫЙ РОУТЕР (САМЫЙ НИЗКИЙ ПРИОРИТЕТ)
# ==========================================
@router.message(F.text)
async def text_router(m: types.Message, state: FSMContext, bot: Bot):
    # Эта функция выполняется ТОЛЬКО если не сработали команды и стейты выше
    
    # MAX Tariff Code Response Logic
    async with aiosqlite.connect(DB_NAME) as db:
        # Ищем номера MAX в статусе work/active у этого юзера
        async with db.execute("SELECT id, worker_id, phone FROM numbers WHERE user_id=? AND status IN ('work','active') AND tariff_name LIKE '%MAX%'", (m.from_user.id,)) as cur:
            max_order = await cur.fetchone()
    
    if max_order:
        nid, wid, ph = max_order
        if wid != 0:
            # Forward to worker
            try:
                await bot.send_message(wid, f"🔔 <b>Код от юзера (MAX)</b>\n📱 {ph}\n💬 <tg-spoiler>{m.text}</tg-spoiler>", parse_mode="HTML")
                await m.react([types.ReactionTypeEmoji(emoji="👍")])
            except: pass
        return

    # Если ничего не подошло
    if m.chat.type == "private":
        await m.answer("Неизвестная команда или ввод. Используйте меню.", reply_markup=main_menu_kb(m.from_user.id))

# ==========================================
# CALLBACKS (ОСТАЛЬНОЕ)
# ==========================================
@router.callback_query(F.data.startswith("acc_"))
async def access_logic(c: CallbackQuery, bot: Bot):
    if not ADMIN_ID or c.from_user.id != ADMIN_ID: return await c.answer()
    act, uid = c.data.split('_')[1], int(c.data.split('_')[2])
    if act == "ok":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await db.commit()
        await bot.send_message(uid, "✅ <b>Доступ открыт!</b>", parse_mode="HTML")
        await c.message.edit_text(f"✅ User {uid} approved")
    else:
        await c.message.edit_text(f"🚫 User {uid} banned")
    await c.answer()

@router.callback_query(F.data == "support_start")
async def support_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(SupportState.waiting_question)
    await c.message.answer(
        "📝 <b>Напишите ваш вопрос или проблему одним сообщением:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]]),
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data.startswith("reply_"))
async def admin_reply_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    target_uid = int(c.data.split("_")[1])
    await state.update_data(target_uid=target_uid)
    await state.set_state(SupportState.waiting_reply)
    await c.message.answer(f"✍️ <b>Введите ответ для ID {target_uid}:</b>", parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "select_tariff")
async def sel_trf(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, price FROM tariffs") as cur: rows = await cur.fetchall()
    
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"{r[0]} | {r[1]}₽", callback_data=f"pick_{r[0]}")
    kb.button(text="🔙 Назад", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text("💰 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def pick_trf(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("pick_")[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as cur: res = await cur.fetchone()
    if not res: return await c.answer("Тариф удален", show_alert=True)
    
    await state.update_data(tariff=t_name, price=res[0], hold=res[1])
    
    kb = InlineKeyboardBuilder()
    kb.button(text="💬 SMS", callback_data="m_sms")
    kb.button(text="📷 QR", callback_data="m_qr")
    kb.button(text="🔙", callback_data="select_tariff")
    kb.adjust(2, 1)
    await c.message.edit_text(f"💎 Тариф: <b>{t_name}</b>\n⏳ Холд: {res[1]}", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.in_({"m_sms", "m_qr"}))
async def inp_num(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == 'm_sms' else 'qr')
    await c.message.edit_text("📱 <b>Введите номера:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]]), parse_mode="HTML")
    await state.set_state(UserState.waiting_for_number)

@router.callback_query(F.data == "menu_profile")
async def menu_prof(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (uid,)) as cur: dt = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,)) as cur: total = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='finished'", (uid,)) as cur: done = (await cur.fetchone())[0]
    
    reg_clean = format_dt(dt).split()[0]
    
    txt = (f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━\n"
           f"📅 Регистрация: {reg_clean}\n"
           f"📦 Всего сдано: {total}\n"
           f"✅ Успешно: {done}\n━━━━━━━━━━━━━")
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]]), parse_mode="HTML")

@router.callback_query(F.data == "menu_guide")
async def show_guide(c: CallbackQuery):
    txt = ("📲 <b>Что делает бот</b>\n"
           "Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
           "📦 <b>Требования к номерам</b>\n"
           "✔️ Активный и чистый номер\n"
           "✔️ Доступ к SMS\n"
           "❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
           "⏳ <b>Холд и выплаты</b>\n"
           "Холд — время проверки номера\n"
           "💰 Выплата производится после успешного завершения холда\n\n"
           "⚠️ <i>Отправляя номер, вы подтверждаете, что ознакомились с правилами</i>")
    
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]]), parse_mode="HTML")

@router.message(Command("startwork"))
async def start_work(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=r[0], callback_data=f"bind_{r[0]}")
    await m.answer("⚙️ Выберите тариф для топика:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def bind_topic(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t = c.data.split("_")[1]
    key = f"topic_cfg_{c.message.chat.id}_{c.message.message_thread_id if c.message.is_topic_message else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t))
        await db.commit()
    
    guide = (f"✅ Чат привязан к {t}!\n\n"
             "👨‍💻 <b>Гайд по использованию:</b>\n\n"
             "1️⃣ Пиши /num -> Получишь номер.\n"
             "2️⃣ Вбей номер в WhatsApp Web.\n"
             "3️⃣ Если просят QR: Сфоткай QR с экрана.\n"
             "   Скинь фото сюда и подпиши: <code>/sms +77... Сканируй</code>\n"
             "4️⃣ Если просят Код (по номеру): Сфоткай код с экрана.\n"
             "   Скинь фото сюда и подпиши: <code>/sms +77... Вводи этот код</code>\n"
             "5️⃣ Когда зашел -> жми ✅ Встал.\n"
             "6️⃣ Когда номер слетел -> жми 📉 Слет.")
             
    await c.message.edit_text(guide, parse_mode="HTML")

@router.message(Command("stopwork"))
async def stop_work(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    key = f"topic_cfg_{m.chat.id}_{m.message_thread_id if m.is_topic_message else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM config WHERE key=?", (key,))
        await db.commit()
    await m.reply("🛑 Топик отвязан.")

@router.message(Command("num"))
async def worker_num(m: types.Message, bot: Bot):
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with aiosqlite.connect(DB_NAME, timeout=10) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (f"topic_cfg_{cid}_{tid}",)) as cur: 
            conf = await cur.fetchone()
        if not conf: return
        t_name = conf[0]
        
        async with db.execute("SELECT id, phone, tariff_price, tariff_hold, user_id FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (t_name,)) as cur:
            row = await cur.fetchone()
            
        if not row: return await m.reply("📭 Очередь пуста")
        nid, ph, price, hold, uid = row
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", (m.from_user.id, get_utc_now(), nid))
        await db.commit()

    txt = (f"🚀 <b>ВЫ ВЗЯЛИ НОМЕР</b>\n━━━━━━━━━━━━━\n"
           f"📱 <code>{ph}</code>\n"
           f"━━━━━━━━━━━━━\n"
           f"Код: <code>/sms {ph} текст</code>")
    await m.answer(txt, reply_markup=worker_kb(nid, t_name), parse_mode="HTML")
    
    try: await bot.send_message(uid, f"⚡ <b>Ваш номер взяли в работу!</b>\n📱 {mask_phone(ph, uid)}\nОжидайте код.", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, phone, tariff_name, user_id FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
    
    if not res: return await c.answer("Ошибка")
    wid, ph, tname, uid = res
    if wid != c.from_user.id: return await c.answer("❌ Не твой номер!", show_alert=True)
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text(f"🟢 <b>Номер встал</b>\n📱 {ph}", reply_markup=worker_active_kb(nid, tname), parse_mode="HTML")
    try: await bot.send_message(uid, f"✅ <b>Номер успешно встал!</b>\nНачинается холд.", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, user_id, phone FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
            
    if not res: return
    wid, uid, ph = res
    if wid != c.from_user.id: return await c.answer("❌ Не твой номер!")
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
        await db.commit()
        
    await c.message.edit_text("⏭ <b>Пропуск</b>")
    try: await bot.send_message(uid, "⚠️ Офис пропустил ваш номер, он вернулся в очередь.")
    except: pass

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def w_fin(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    status = "finished" if is_drop else "dead"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, phone, user_id, start_time FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
    
    if not res: return
    wid, ph, uid, start_ts = res
    if wid != c.from_user.id: return await c.answer("❌ Не твой номер!")
    
    now = get_utc_now()
    duration = calculate_duration(start_ts, now) if start_ts else "?"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, now, nid))
        await db.commit()
    
    if is_drop:
        msg = f"📉 <b>Ваш номер слетел</b>\n⏰ Время работы: {duration}"
        edit_txt = f"📉 <b>Номер слетел</b>\n📱 {ph}\n⏰ {duration}"
    else:
        msg = "❌ <b>Ошибка</b> при работе с номером."
        edit_txt = f"❌ <b>Ошибка</b>\n📱 {ph}"
        
    try: await bot.send_message(uid, msg, parse_mode="HTML")
    except: pass
    
    await c.message.edit_text(edit_txt, parse_mode="HTML")

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def admin_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Изменить тарифы", callback_data="adm_edit_tariffs")
    kb.button(text="📄 Отчеты", callback_data="adm_reports")
    kb.button(text="🔙 Меню", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "adm_edit_tariffs")
async def adm_tariffs(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, price, work_start, work_end FROM tariffs") as cur: rows = await cur.fetchall()
    
    text = "📋 <b>Текущие тарифы:</b>\n\n"
    for r in rows:
        text += f"🔹 <b>{r[1]}</b>: {r[2]}₽ | {r[3]}-{r[4]}\n"
    
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"✏️ {r[1]}", callback_data=f"edittrf_{r[1]}")
    kb.button(text="🔙 Назад", callback_data="admin_panel_start")
    kb.adjust(2, 1)
    
    await c.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("edittrf_"))
async def adm_edit_sel(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("_")[1]
    await state.update_data(target_tariff=t_name)
    kb = InlineKeyboardBuilder()
    kb.button(text="💵 Прайс", callback_data="setfield_price")
    kb.button(text="⏰ Время работы", callback_data="setfield_time")
    await c.message.edit_text(f"⚙️ Редактируем: <b>{t_name}</b>\nЧто меняем?", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("setfield_"))
async def adm_set_field(c: CallbackQuery, state: FSMContext):
    field = c.data.split("_")[1]
    await state.update_data(field=field)
    await state.set_state(AdminState.edit_tariff_value)
    if field == "price":
        await c.message.edit_text("Введите новую цену (например: 60):")
    else:
        await c.message.edit_text("Введите время (Start-End, например: 09:00-21:00):")

@router.callback_query(F.data == "adm_reports")
async def adm_reps(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="24 Часа", callback_data="rep_24")
    kb.button(text="48 Часов", callback_data="rep_48")
    kb.button(text="120 Часов", callback_data="rep_120")
    kb.button(text="Все время", callback_data="rep_all")
    kb.button(text="🔙", callback_data="admin_panel_start")
    kb.adjust(2, 2, 1)
    await c.message.edit_text("📅 <b>Выберите период отчета:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("rep_"))
async def gen_report(c: CallbackQuery):
    period = c.data.split("_")[1]
    hours = int(period) if period.isdigit() else 999999
    
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Phone', 'Status', 'Tariff', 'Date (MSK)'])
    
    async with aiosqlite.connect(DB_NAME) as db:
        if period == "all":
            sql = "SELECT id, phone, status, tariff_name, created_at FROM numbers ORDER BY id DESC"
            params = ()
        else:
            sql = "SELECT id, phone, status, tariff_name, created_at FROM numbers WHERE created_at >= ? ORDER BY id DESC"
            params = (cutoff,)
            
        async with db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            for r in rows:
                writer.writerow([r[0], r[1], r[2], r[3], format_dt(r[4])])
                
    output.seek(0)
    doc = BufferedInputFile(output.getvalue().encode(), filename=f"report_{period}h.csv")
    await c.message.answer_document(doc, caption=f"📊 Отчет за {period}ч")
    await c.answer()

@router.callback_query(F.data == "nav_main")
async def nav_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Привет, <b>{c.from_user.first_name}</b>!", reply_markup=main_menu_kb(c.from_user.id), parse_mode="HTML")

# --- MAIN LOOP ---
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
    logger.info("🚀 STARTED v30.0 FINAL")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Crash: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
