import asyncio
import logging
import sys
import os
import re
import csv
import io
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

# --- REDIS CHECK ---
try:
    from aiogram.fsm.storage.redis import RedisStorage
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
DB_NAME = "fast_team_v42.db"

# Настройки AFK (в минутах)
AFK_WARN_MIN = 5
AFK_KICK_MIN = 8

# Лимиты
BATCH_LIMIT = 50

# Глобальный кэш тарифов
TARIFF_CACHE = []

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

# --- UTILS ---
def get_utc_now():
    return datetime.now(timezone.utc).isoformat()

def get_local_time_str(iso_str):
    """Формат: YYYY-MM-DD HH:MM (без секунд)"""
    if not iso_str: return "-"
    try:
        dt = datetime.fromisoformat(iso_str)
        local_dt = dt + timedelta(hours=5) 
        return local_dt.strftime("%Y-%m-%d %H:%M")
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
        if len(phone) < 8: return phone
        return f"{phone[:4]}****{phone[-3:]}"
    except: return phone

# --- CACHE MANAGER ---
async def refresh_tariffs_cache():
    global TARIFF_CACHE
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT name, price, hold_info FROM tariffs") as cur:
                TARIFF_CACHE = await cur.fetchall()
    except Exception as e:
        logger.error(f"Cache Error: {e}")

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()
    waiting_support_msg = State() # Юзер пишет в поддержку

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_tariff_val = State()
    replying_support = State() # Админ отвечает юзеру

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, 
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            method TEXT, tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, 
            status TEXT, worker_id INTEGER DEFAULT 0, 
            worker_chat_id INTEGER DEFAULT 0, worker_thread_id INTEGER DEFAULT 0,
            start_time TIMESTAMP, end_time TIMESTAMP, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, 
            price TEXT, hold_info TEXT)""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY, value TEXT)""")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_u_t ON numbers(status, user_id, tariff_name)")
        await db.execute("INSERT OR IGNORE INTO tariffs (name, price, hold_info) VALUES ('WhatsApp', '50', '1h'), ('MAX', '150', '2h')")
        await db.commit()
        await refresh_tariffs_cache()
        logger.info("✅ DB INITIALIZED v42.0")

# --- KEYBOARDS ---
def main_menu_kb(user_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Сдать номер", callback_data="select_tariff")
    builder.button(text="👤 Профиль", callback_data="menu_profile")
    builder.button(text="ℹ️ Помощь", callback_data="menu_guide") # Тут запрос в ТП
    
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
        kb.button(text="Ошибка ❌", callback_data=f"w_err_{num_id}") # Текст "Ошибка" по ТЗ
    return kb.as_markup()

def worker_active_kb(num_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📉 Слет", callback_data=f"w_drop_{num_id}")
    return kb.as_markup()

# ==========================================
# 1. AFK MONITOR (ФОНОВАЯ ЗАДАЧА)
# ==========================================
async def queue_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            async with aiosqlite.connect(DB_NAME) as db:
                sql = "SELECT id, start_time, worker_chat_id, worker_thread_id, phone, user_id FROM numbers WHERE status='work'"
                async with db.execute(sql) as cur: rows = await cur.fetchall()
                
                for r in rows:
                    nid, start_iso, chat_id, thread_id, phone, uid = r
                    if not start_iso: continue
                    start_dt = datetime.fromisoformat(start_iso)
                    diff = (now - start_dt).total_seconds() / 60
                    
                    if AFK_WARN_MIN <= diff < AFK_WARN_MIN + 1:
                        try: await bot.send_message(chat_id, f"⚠️ <b>AFK CHECK</b>\nНомер {phone} висит {AFK_WARN_MIN} мин!", message_thread_id=thread_id, parse_mode="HTML")
                        except: pass
                        
                    elif diff >= AFK_KICK_MIN:
                        await db.execute("UPDATE numbers SET status='queue', worker_id=0, start_time=NULL WHERE id=?", (nid,))
                        await db.commit()
                        try:
                            await bot.send_message(chat_id, f"💤 <b>AFK KICK</b>\nНомер {phone} отобран.", message_thread_id=thread_id, parse_mode="HTML")
                            await bot.send_message(uid, "♻️ Ваш номер вернулся в очередь (воркер уснул).")
                        except: pass
        except Exception as e:
            logger.error(f"Monitor: {e}")
            await asyncio.sleep(60)

# ==========================================
# 2. СТАРТ И РЕГИСТРАЦИЯ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id=?", (uid,)) as c: res = await c.fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>Запрос доступа:</b> {uid} (@{m.from_user.username})", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте подтверждения.</b>", parse_mode="HTML")
        if res[0]: await m.answer(f"👋 Привет, <b>{m.from_user.first_name}</b>!", reply_markup=main_menu_kb(uid), parse_mode="HTML")
        else: await m.answer("⏳ <b>На рассмотрении.</b>", parse_mode="HTML")

@router.callback_query(F.data.startswith("acc_"))
async def access_logic(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split('_')[1], int(c.data.split('_')[2])
    if act == "ok":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await db.commit()
        await bot.send_message(uid, "✅ <b>Доступ открыт!</b>\nЖми /start", parse_mode="HTML")
        await c.message.edit_text(f"✅ User {uid} approved")
    else: await c.message.edit_text(f"🚫 User {uid} banned")

@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    try: await c.message.delete()
    except: pass
    await c.message.answer("👋 <b>Главное меню</b>", reply_markup=main_menu_kb(c.from_user.id), parse_mode="HTML")

# ==========================================
# 3. СДАЧА НОМЕРА
# ==========================================
@router.callback_query(F.data == "select_tariff")
async def sel_trf(c: CallbackQuery):
    if not TARIFF_CACHE: await refresh_tariffs_cache()
    kb = InlineKeyboardBuilder()
    for r in TARIFF_CACHE: kb.button(text=f"{r[0]}", callback_data=f"pick_{r[0]}")
    kb.button(text="🔙 Назад", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text("📂 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def pick_trf(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("pick_")[1]
    res = next((x for x in TARIFF_CACHE if x[0] == t_name), None)
    if not res: return await c.answer("Тариф удален", show_alert=True)
    await state.update_data(tariff=t_name, price=res[1], hold=res[2])
    
    txt = f"💎 Тариф: <b>{t_name}</b>\n💵 Оплата: <b>{res[1]}</b>\n⏳ Холд: <b>{res[2]}</b>"
    kb = InlineKeyboardBuilder()
    kb.button(text="💬 СМС", callback_data="m_sms")
    kb.button(text="📷 QR", callback_data="m_qr")
    kb.button(text="🔙 Назад", callback_data="select_tariff")
    kb.adjust(2, 1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.in_({"m_sms", "m_qr"}))
async def inp_method(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == 'm_sms' else 'qr')
    await c.message.edit_text("📱 <b>Введите номера (списком):</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]]), parse_mode="HTML")
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def proc_nums(m: types.Message, state: FSMContext):
    if not m.text: return await m.reply("❌ Пришлите текст")
    d = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = []
    for x in raw:
        if not x.strip(): continue
        cl = clean_phone(x.strip())
        if cl: valid.append(cl)
    
    if not valid: return await m.reply("❌ Номера не распознаны")
    if len(valid) > BATCH_LIMIT: return await m.reply(f"❌ Лимит {BATCH_LIMIT} шт.")
    
    cnt = 0
    async with aiosqlite.connect(DB_NAME) as db:
        for ph in valid:
            async with db.execute("SELECT 1 FROM numbers WHERE phone=? AND status IN ('queue','work','active')", (ph,)) as cur:
                if not await cur.fetchone():
                    await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)", 
                                     (m.from_user.id, ph, d['method'], d['tariff'], d['price'], d['hold'], get_utc_now()))
                    cnt += 1
        await db.commit()
    await state.clear()
    await m.answer(f"✅ <b>Загружено:</b> {cnt} номеров", reply_markup=main_menu_kb(m.from_user.id), parse_mode="HTML")

# ==========================================
# 4. ВОРКЕР (ЛОГИКА)
# ==========================================
@router.message(Command("startwork"))
async def start_work(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    if not TARIFF_CACHE: await refresh_tariffs_cache()
    kb = InlineKeyboardBuilder()
    for r in TARIFF_CACHE: kb.button(text=r[0], callback_data=f"bind_{r[0]}")
    await m.answer("⚙️ <b>Привязка топика:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.message(Command("stopwork"))
async def stop_work(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    tid = m.message_thread_id if m.is_topic_message else 0
    key = f"topic_cfg_{m.chat.id}_{tid}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM config WHERE key=?", (key,))
        await db.commit()
    await m.answer("🛑 <b>Работа в топике остановлена.</b>", parse_mode="HTML")

@router.callback_query(F.data.startswith("bind_"))
async def bind_cb(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t = c.data.split("_")[1]
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_cfg_{c.message.chat.id}_{tid}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t))
        await db.commit()
    
    # ГАЙД ПО ТЗ
    guide = (f"✅ Чат привязан! Тариф: {t}\n\n"
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

@router.message(Command("num"))
async def worker_num(m: types.Message, bot: Bot):
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (f"topic_cfg_{cid}_{tid}",)) as cur: conf = await cur.fetchone()
        if not conf: return
        t_name = conf[0]
        async with db.execute("SELECT id, phone, user_id FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (t_name,)) as cur: row = await cur.fetchone()
        
        if not row: return await m.reply("📭 <b>Пусто</b>", parse_mode="HTML")
        nid, ph, uid = row
        await db.execute("UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?", (m.from_user.id, cid, tid, get_utc_now(), nid))
        await db.commit()
    
    # Текст по ТЗ
    await m.answer(f"⚡ <b>Номер в работе!</b>\n📱 <code>{ph}</code>\nОжидайте код.", reply_markup=worker_kb(nid, t_name), parse_mode="HTML")
    try: await bot.send_message(uid, f"⚡ <b>Ваш номер {mask_phone(ph, uid)} взяли!</b>\nОжидайте код.", parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def cmd_code(m: types.Message, command: CommandObject, bot: Bot):
    # ВОРКЕР ПИШЕТ /code +77...
    if not command.args: return await m.reply("⚠️ Формат: <code>/code +7...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, id, worker_id, tariff_name FROM numbers WHERE phone=? AND status='work'", (ph,)) as cur: res = await cur.fetchone()
    
    if not res: return await m.reply("❌ Нет в работе")
    uid, nid, wid, tname = res
    if wid != m.from_user.id: return await m.reply("❌ Не твой номер!")
    if "MAX" not in tname.upper(): return await m.reply("❌ Работает только на тарифе MAX")
    
    try:
        await bot.send_message(uid, f"🔔 <b>Офис запросил номер!</b>\n📱 {mask_phone(ph, uid)}\n👇 <b>Ответьте на это сообщение кодом (Reply)</b>", parse_mode="HTML")
        await m.reply(f"✅ Запрос ушел юзеру на {ph}")
    except: await m.reply("❌ Ошибка отправки юзеру")

# ==========================================
# 5. КНОПКИ ВОРКЕРА (ЗАЩИТА + ЛОГИКА)
# ==========================================
async def check_worker(c: CallbackQuery, nid):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id, phone, user_id, start_time FROM numbers WHERE id=?", (nid,)) as cur: res = await cur.fetchone()
    if not res: await c.answer("Номер не найден"); return None
    if res[0] != c.from_user.id and c.from_user.id != ADMIN_ID:
        await c.answer("❌ Это не ваш номер!", show_alert=True); return None
    return res

@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    res = await check_worker(c, nid)
    if not res: return
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
        
    await c.message.edit_text(f"Номер встал и все", reply_markup=worker_active_kb(nid), parse_mode="HTML")
    try: await bot.send_message(res[2], "✅ <b>Номер встал в работу!</b>", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    res = await check_worker(c, nid)
    if not res: return
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text("⏭ <b>Пропуск</b> (возврат в очередь)", parse_mode="HTML")
    try: await bot.send_message(res[2], "⚠️ Офис пропустил ваш номер, он вернулся в очередь.")
    except: pass

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def w_fin(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    res = await check_worker(c, nid)
    if not res: return
    
    is_drop = "drop" in c.data
    st = "finished" if is_drop else "dead"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (st, get_utc_now(), nid))
        await db.commit()
    
    dur = calc_duration(res[3], get_utc_now())
    # Текст по ТЗ
    if is_drop:
        await c.message.edit_text(f"Ваш номер слетел и его время работы: {dur}", parse_mode="HTML")
        try: await bot.send_message(res[2], f"📉 Ваш номер слетел. Время работы: {dur}", parse_mode="HTML")
        except: pass
    else:
        await c.message.edit_text(f"❌ Ошибка\n📱 {res[1]}", parse_mode="HTML")
        try: await bot.send_message(res[2], "❌ Ошибка номера (отмена).", parse_mode="HTML")
        except: pass

# ==========================================
# 6. СООБЩЕНИЯ (SMS + REPLIES)
# ==========================================
@router.message(F.photo | F.text)
async def msg_handler(m: types.Message, bot: Bot):
    # 1. SMS ОТ ВОРКЕРА (/sms)
    text = m.caption or m.text or ""
    if text.lower().startswith("/sms"):
        p = text.split(None, 2)
        if len(p) >= 3:
            ph, tx = clean_phone(p[1]), p[2]
            if ph:
                async with aiosqlite.connect(DB_NAME) as db:
                    async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: res = await cur.fetchone()
                if res:
                    if m.photo: await bot.send_photo(res[0], m.photo[-1].file_id, caption=f"🔔 <b>SMS</b>\n📱 {ph}\n💬 <tg-spoiler>{tx}</tg-spoiler>", parse_mode="HTML")
                    else: await bot.send_message(res[0], f"🔔 <b>SMS</b>\n📱 {ph}\n💬 <tg-spoiler>{tx}</tg-spoiler>", parse_mode="HTML")
                    await m.react([types.ReactionTypeEmoji(emoji="🔥")])
        return

    # 2. ОТВЕТ ЮЗЕРА НА КОД (MAX)
    if m.reply_to_message and "Офис запросил" in m.reply_to_message.text:
        async with aiosqlite.connect(DB_NAME) as db:
            sql = "SELECT worker_chat_id, worker_thread_id, phone FROM numbers WHERE user_id=? AND status='work' AND tariff_name LIKE '%MAX%'"
            async with db.execute(sql, (m.from_user.id,)) as cur: res = await cur.fetchone()
        if res:
            try:
                content = m.caption or m.text or "[Файл]"
                header = f"📩 <b>ОТВЕТ ОТ ЮЗЕРА (MAX)</b>\n📱 {res[2]}\n💬 {content}"
                if m.photo: await bot.send_photo(res[0], m.photo[-1].file_id, caption=header, message_thread_id=res[1], parse_mode="HTML")
                else: await bot.send_message(res[0], header, message_thread_id=res[1], parse_mode="HTML")
                await m.answer("✅ Отправлено воркеру.")
            except: pass

# ==========================================
# 7. ПОМОЩЬ И ПОДДЕРЖКА (TICKET SYSTEM)
# ==========================================
@router.callback_query(F.data == "menu_guide")
async def guide_menu(c: CallbackQuery):
    txt = ("📲 <b>Что делает бот</b>\n"
           "Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
           "📦 <b>Требования к номерам</b>\n"
           "✔️ Активный и чистый номер\n"
           "✔️ Доступ к SMS\n"
           "❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
           "⏳ <b>Холд и выплаты</b>\n"
           "Холд — время проверки номера\n"
           "💰 Выплата производится после успешного завершения холда\n\n"
           "⚠️ Отправляя номер, вы подтверждаете, что ознакомились с правилами")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🆘 Задать вопрос (Поддержка)", callback_data="ask_support")
    kb.button(text="🔙 Назад", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "ask_support")
async def ask_supp(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("✍️ <b>Напишите свой вопрос или проблему одним сообщением:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="menu_guide")]]), parse_mode="HTML")
    await state.set_state(UserState.waiting_support_msg)

@router.message(UserState.waiting_support_msg)
async def send_supp(m: types.Message, state: FSMContext, bot: Bot):
    if not ADMIN_ID: return await m.reply("❌ Поддержка не настроена")
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Ответить", callback_data=f"reply_{m.from_user.id}")
    try:
        await bot.send_message(ADMIN_ID, f"🆘 <b>НОВЫЙ ЗАПРОС</b>\n👤: {m.from_user.id} (@{m.from_user.username})\n\n💬 {m.text}", reply_markup=kb.as_markup(), parse_mode="HTML")
        await m.answer("✅ Ваш запрос отправлен администратору. Ожидайте ответа.")
    except: await m.answer("❌ Ошибка отправки")
    await state.clear()

@router.callback_query(F.data.startswith("reply_"))
async def adm_reply_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    uid = c.data.split("_")[1]
    await state.update_data(reply_to=uid)
    await c.message.answer("✍️ <b>Введите ответ пользователю:</b>")
    await state.set_state(AdminState.replying_support)
    await c.answer()

@router.message(AdminState.replying_support)
async def adm_reply_send(m: types.Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    try:
        await bot.send_message(d['reply_to'], f"👨‍💻 <b>Ответ поддержки:</b>\n\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ отправлен.")
    except: await m.answer("❌ Не удалось отправить (юзер заблочил бота?)")
    await state.clear()

# ==========================================
# 8. ПРОФИЛЬ (СТАТИСТИКА)
# ==========================================
@router.callback_query(F.data == "menu_profile")
async def profile(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (c.from_user.id,)) as cur: t = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='finished'", (c.from_user.id,)) as cur: d = (await cur.fetchone())[0]
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Номера за сегодня", callback_data="my_nums")
    kb.button(text="🔙 Назад", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text(f"👤 <b>Профиль</b>\n📦 Всего: {t}\n✅ Выплачено: {d}", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "my_nums")
async def my_nums(c: CallbackQuery):
    cut = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, status, tariff_price, created_at FROM numbers WHERE user_id=? AND created_at >= ? ORDER BY id DESC", (c.from_user.id, cut)) as cur: rows = await cur.fetchall()
    txt = "📝 <b>Ваши номера (24ч):</b>\n\n"
    for r in rows:
        st_icon = "🟢" if r[1]=='active' else "✅" if r[1]=='finished' else "🟡"
        txt += f"{st_icon} {r[0]} | {r[2]} | {get_local_time_str(r[3])}\n"
    kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="menu_profile")
    if len(txt) > 4000: txt = txt[:4000]
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

# ==========================================
# 9. АДМИН ПАНЕЛЬ (ПОЛНЫЙ ФУНКЦИОНАЛ)
# ==========================================
@router.callback_query(F.data == "admin_panel_start")
async def adm_menu(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы", callback_data="adm_edittrf")
    kb.button(text="📦 Очереди", callback_data="adm_queue_list") # НОВОЕ
    kb.button(text="📄 Отчеты", callback_data="adm_reps")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙 Меню", callback_data="nav_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- УПРАВЛЕНИЕ ОЧЕРЕДЬЮ ---
@router.callback_query(F.data == "adm_queue_list")
async def adm_queue(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, phone, tariff_name FROM numbers WHERE status='queue' ORDER BY id DESC LIMIT 10") as cur: rows = await cur.fetchall()
    
    if not rows: return await c.answer("Очередь пуста", show_alert=True)
    
    kb = InlineKeyboardBuilder()
    txt = "📦 <b>Очередь (Последние 10):</b>\n"
    for r in rows:
        txt += f"🆔 {r[0]} | {r[1]} ({r[2]})\n"
        kb.button(text=f"🗑 {r[1]}", callback_data=f"delq_{r[0]}")
    
    kb.button(text="🔙 Назад", callback_data="admin_panel_start")
    kb.adjust(2)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("delq_"))
async def adm_del_queue(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
        await db.commit()
    await c.answer("Удалено!")
    await adm_queue(c) # Обновляем список

# --- ТАРИФЫ ---
@router.callback_query(F.data == "adm_edittrf")
async def adm_edittrf(c: CallbackQuery):
    if not TARIFF_CACHE: await refresh_tariffs_cache()
    txt = "📋 <b>Настройки тарифов:</b>\n"
    kb = InlineKeyboardBuilder()
    for r in TARIFF_CACHE: 
        txt += f"🔹 {r[0]}: Прайс {r[1]} / Холд {r[2]}\n"
        kb.button(text=f"✏️ {r[0]}", callback_data=f"trfedit_{r[0]}")
    kb.button(text="🔙", callback_data="admin_panel_start")
    kb.adjust(2, 1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("trfedit_"))
async def adm_trf_sel(c: CallbackQuery, state: FSMContext):
    await state.update_data(target=c.data.split("_")[1])
    kb = InlineKeyboardBuilder()
    kb.button(text="Изменить Прайс", callback_data="set_price")
    kb.button(text="Время работы (Холд)", callback_data="set_hold")
    kb.button(text="🔙", callback_data="adm_edittrf")
    kb.adjust(1)
    await c.message.edit_text(f"Редактируем: <b>{c.data.split('_')[1]}</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("set_"))
async def adm_set(c: CallbackQuery, state: FSMContext):
    await state.update_data(field=c.data.split("_")[1])
    await state.set_state(AdminState.edit_tariff_val)
    await c.message.edit_text("✍️ Введите новое значение:")

@router.message(AdminState.edit_tariff_val)
async def adm_save(m: types.Message, state: FSMContext):
    d = await state.get_data()
    col = "price" if d['field'] == "price" else "hold_info"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f"UPDATE tariffs SET {col}=? WHERE name=?", (m.text, d['target']))
        await db.commit()
    await refresh_tariffs_cache()
    await state.clear()
    await m.answer("✅ Сохранено", reply_markup=main_menu_kb(m.from_user.id))

# --- ОТЧЕТЫ ---
@router.callback_query(F.data == "adm_reps")
async def adm_reps(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for h in [1, 24, 48, 120]: kb.button(text=f"{h} часов", callback_data=f"rep_{h}")
    kb.button(text="За все время", callback_data="rep_all")
    kb.button(text="🔙", callback_data="admin_panel_start")
    kb.adjust(2, 2, 1)
    await c.message.edit_text("📊 Выберите период:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_"))
async def get_rep(c: CallbackQuery):
    h = c.data.split("_")[1]
    async with aiosqlite.connect(DB_NAME) as db:
        if h == "all":
            sql = "SELECT id, phone, status, tariff_name, created_at FROM numbers ORDER BY id DESC"
            p = ()
        else:
            cut = (datetime.now(timezone.utc) - timedelta(hours=int(h))).isoformat()
            sql = "SELECT id, phone, status, tariff_name, created_at FROM numbers WHERE created_at >= ? ORDER BY id DESC"
            p = (cut,)
        async with db.execute(sql, p) as cur: rows = await cur.fetchall()
    
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(['ID', 'Phone', 'Status', 'Tariff', 'Date'])
    for r in rows: w.writerow([r[0], r[1], r[2], r[3], get_local_time_str(r[4])])
    out.seek(0)
    await c.message.answer_document(BufferedInputFile(out.getvalue().encode(), filename=f"rep_{h}.csv"), caption=f"📊 Отчет за {h}")
    await c.answer()

# --- РАССЫЛКА (FIXED) ---
@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📢 Отправьте сообщение для рассылки (Текст/Фото):"); 
    await state.set_state(AdminState.waiting_broadcast)

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: types.Message, state: FSMContext, bot: Bot):
    await state.clear()
    msg = await m.answer("⏳ Рассылка запущена...")
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cur: usrs = await cur.fetchall()
    
    cnt = 0
    blocked = 0
    for u in usrs:
        try:
            await m.copy_to(u[0])
            cnt += 1
            await asyncio.sleep(0.05) # Anti-flood
        except Exception:
            blocked += 1
    
    await msg.edit_text(f"✅ Рассылка завершена!\nДоставлено: {cnt}\nНедоступно: {blocked}")

# --- MAIN ---
async def main():
    await init_db()
    if HAS_REDIS and os.getenv("REDIS_URL"): storage = RedisStorage.from_url(os.getenv("REDIS_URL"))
    else: storage = MemoryStorage()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 STARTED v42.0 FINAL")
    asyncio.create_task(queue_monitor(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
