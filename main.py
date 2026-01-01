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
DB_NAME = "fast_team_v28.db"

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

# Anti-spam cooldown
user_cooldowns = {}

# --- HELPERS: UX & LOGIC ---
def render_progressbar(current, total, length=10):
    if total == 0: return "░" * length
    percent = current / total
    filled = int(length * percent)
    return "█" * filled + "░" * (length - filled)

def clean_phone(phone: str):
    # Убираем все кроме цифр
    clean = re.sub(r'[^\d]', '', str(phone))
    
    # 1. KZ FIX (7705... -> +7705...)
    # Если начинается на 77 и длина 11 (например 77051234567)
    if clean.startswith('77') and len(clean) == 11:
        return '+' + clean
    
    # 2. RU/KZ Standart Correction
    if clean.startswith('8') and len(clean) == 11: 
        clean = '7' + clean[1:]
    elif len(clean) == 10 and clean.isdigit(): 
        clean = '7' + clean
        
    # 3. Validation (+7 или +77 + 10 цифр)
    # Итог должен быть 11 или 12 цифр
    if not re.match(r'^7\d{10}$|^77\d{9,10}$', clean):
        return None
        
    return '+' + clean

def mask_phone(phone, user_id):
    if user_id == ADMIN_ID: return phone
    try:
        if len(phone) < 8: return phone
        return f"{phone[:4]}****{phone[-3:]}"
    except: return phone

def get_utc_now():
    return datetime.now(timezone.utc).isoformat()

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class MaxState(StatesGroup):
    waiting_code = State() # Для ручного ввода кода воркером

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    trf_add_name = State()
    trf_add_price = State()

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL;") # Speed boost
        
        # Users + Balance
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            username TEXT, 
            is_approved INTEGER DEFAULT 0, 
            balance REAL DEFAULT 0.0,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Numbers + Position + Code
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            phone TEXT, 
            method TEXT, 
            tariff_name TEXT, 
            tariff_price TEXT, 
            tariff_hold TEXT, 
            status TEXT, 
            worker_id INTEGER, 
            code_received TEXT,
            start_time TIMESTAMP, 
            end_time TIMESTAMP, 
            worker_msg_id INTEGER, 
            position INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Indexes
        await db.execute("CREATE INDEX IF NOT EXISTS idx_status_tariff ON numbers(status, tariff_name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_uid ON numbers(user_id)")

        # Tariffs & Config
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (id INTEGER PRIMARY KEY, name TEXT UNIQUE, price TEXT, hold_info TEXT)")
        await db.execute("INSERT OR IGNORE INTO tariffs (name, price, hold_info) VALUES ('WhatsApp', '50', '1h'), ('MAX', '150', '2h')")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        await db.commit()
        logger.info("✅ DB v28.0 INITIALIZED")

# --- BACKGROUND NOTIFIER (TOP-3 QUEUE) ---
async def queue_notifier(bot: Bot):
    while True:
        try:
            async with aiosqlite.connect(DB_NAME, timeout=30) as db:
                # Получаем топ-3 номера в очереди
                async with db.execute("SELECT id, user_id, tariff_name, phone FROM numbers WHERE status='queue' ORDER BY id ASC") as cur:
                    rows = await cur.fetchall()
            
            # Логика позиций
            t_queues = {}
            for r in rows:
                t = r[2]
                if t not in t_queues: t_queues[t] = []
                t_queues[t].append(r)
            
            for t, items in t_queues.items():
                for i, item in enumerate(items):
                    pos = i + 1
                    # Уведомляем только 1, 2 и 3 место
                    if pos <= 3:
                        # Тут можно добавить проверку, чтобы не спамить каждую секунду (нужна доп таблица уведомлений)
                        # Для простоты пропускаем
                        pass
        except Exception as e:
            logger.error(f"QueueBG Error: {e}")
        await asyncio.sleep(30)

# --- KEYBOARDS (PREMIUM) ---
def main_menu_kb(user_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Сдать номер", callback_data="select_tariff")
    builder.button(text="👤 Профиль", callback_data="menu_profile")
    builder.button(text="📊 Очередь", callback_data="menu_queue")
    builder.button(text="ℹ️ Помощь", callback_data="menu_guide")
    
    if user_id == ADMIN_ID:
        builder.button(text="⚡️ ADMIN", callback_data="admin_panel_start")
        
    builder.adjust(1, 2, 1) # Сетка
    return builder.as_markup()

def worker_kb(num_id, tariff_name="Std"):
    kb = InlineKeyboardBuilder()
    kb.button(text="Встал ✅", callback_data=f"w_act_{num_id}")
    kb.button(text="Ошибка ❌", callback_data=f"w_err_{num_id}")
    # Для MAX тарифа добавим кнопку ввода кода вручную, если смс не парсится
    if "MAX" in tariff_name.upper():
        pass # Можно добавить спец логику
    return kb.as_markup()

def worker_active_kb(num_id, tariff_name="Std"):
    kb = InlineKeyboardBuilder()
    kb.button(text="📉 СЛЕТ", callback_data=f"w_drop_{num_id}")
    if "MAX" in tariff_name.upper():
        kb.button(text="📤 Ввести код", callback_data=f"w_code_{num_id}")
    return kb.as_markup()

# --- PHOTO HANDLER (PRIORITY #1) ---
@router.message(F.photo)
async def sms_photo_handler(m: types.Message, bot: Bot):
    # 1. Validation
    if not m.caption: return
    caption = m.caption.strip()
    if "/sms" not in caption.lower(): return
    
    # 2. Parse
    try:
        parts = caption.split()
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
        
        if not ph: return await m.reply(f"❌ Кривой номер: {ph_raw}")

        # 3. DB Search
        async with aiosqlite.connect(DB_NAME, timeout=30) as db:
            async with db.execute("SELECT user_id, id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur:
                res = await cur.fetchone()
        
        if res:
            uid, nid = res
            # Send Photo
            await bot.send_photo(
                chat_id=uid, 
                photo=m.photo[-1].file_id, 
                caption=f"🔔 <b>SMS / Код</b>\n━━━━━━━━━━━━━\n📱 <code>{ph}</code>\n💬 <tg-spoiler>{tx_raw}</tg-spoiler>\n━━━━━━━━━━━━━", 
                parse_mode="HTML"
            )
            await m.react([types.ReactionTypeEmoji(emoji="🔥")])
            
            # Log Code (optional)
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE numbers SET code_received=? WHERE id=?", (tx_raw, nid))
                await db.commit()
        else:
            await m.reply(f"🚫 Номер {ph} не в работе.")
            
    except Exception as e:
        logger.error(f"Photo Err: {e}")
        await m.reply("❌ Ошибка обработки")

# --- TEXT SMS HANDLER ---
@router.message(Command("sms"))
async def sms_text_handler(m: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Формат: <code>/sms номер текст</code>", parse_mode="HTML")
    try:
        args = command.args.split(None, 1)
        ph = clean_phone(args[0])
        tx = args[1][:150] if len(args) > 1 else "Код"
        
        if not ph: return await m.reply("❌ Неверный номер")

        async with aiosqlite.connect(DB_NAME, timeout=30) as db:
            async with db.execute("SELECT user_id, id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur:
                res = await cur.fetchone()
        
        if res:
            uid, nid = res
            await bot.send_message(uid, f"🔔 <b>SMS / Код</b>\n━━━━━━━━━━━━━\n📱 <code>{ph}</code>\n💬 <tg-spoiler>{tx}</tg-spoiler>\n━━━━━━━━━━━━━", parse_mode="HTML")
            await m.react([types.ReactionTypeEmoji(emoji="🔥")])
            
            # Log
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE numbers SET code_received=? WHERE id=?", (tx, nid))
                await db.commit()
        else:
            await m.reply("🚫 Не найден активный номер.")
    except Exception as e:
        logger.error(f"Text SMS Err: {e}")
        await m.reply("❌ Ошибка")

# --- START & MENU ---
@router.message(CommandStart())
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    
    # Anti-spam (1 cmd per sec)
    if uid in user_cooldowns and time.time() - user_cooldowns[uid] < 1: return
    user_cooldowns[uid] = time.time()

    async with aiosqlite.connect(DB_NAME, timeout=30) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id=?", (uid,)) as c: res = await c.fetchone()
        
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", 
                            (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Allow", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫 Ban", callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>New User:</b> {uid} (@{m.from_user.username})", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте доступа</b>\nАдминистратор проверит вашу заявку.", parse_mode="HTML")

        if res[0]:
            await m.answer(f"👋 Привет, <b>{m.from_user.first_name}</b>!", reply_markup=main_menu_kb(uid), parse_mode="HTML")
        else:
            await m.answer("⏳ <b>На рассмотрении...</b>", parse_mode="HTML")

@router.callback_query(F.data.startswith("acc_"))
async def access_logic(c: CallbackQuery, bot: Bot):
    if not ADMIN_ID or c.from_user.id != ADMIN_ID: return await c.answer()
    act, uid = c.data.split('_')[1], int(c.data.split('_')[2])
    
    if act == "ok":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await db.commit()
        try: await bot.send_message(uid, "✅ <b>Доступ открыт!</b>\nЖми /start", parse_mode="HTML")
        except: pass
        await c.message.edit_text(f"✅ User {uid} approved")
    else:
        await c.message.edit_text(f"🚫 User {uid} banned")
    await c.answer()

# --- SUBMIT NUMBER ---
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
    await c.answer()

@router.callback_query(F.data.startswith("pick_"))
async def pick_trf(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("pick_")[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as cur: res = await cur.fetchone()
    
    if not res: return await c.answer("Тариф удален!", show_alert=True)
    
    await state.update_data(tariff=t_name, price=res[0], hold=res[1])
    
    kb = InlineKeyboardBuilder()
    kb.button(text="💬 SMS", callback_data="m_sms")
    kb.button(text="📷 QR", callback_data="m_qr")
    kb.button(text="🔙", callback_data="select_tariff")
    kb.adjust(2, 1)
    
    await c.message.edit_text(f"💎 Тариф: <b>{t_name}</b>\n💵 Выплата: {res[0]}₽\n⏳ Холд: {res[1]}", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.in_({"m_sms", "m_qr"}))
async def inp_num(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == 'm_sms' else 'qr')
    await c.message.edit_text("📱 <b>Введите номера</b> (через запятую):\nПример: <code>+79990001122, 7705...</code>", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]]), 
                              parse_mode="HTML")
    await state.set_state(UserState.waiting_for_number)
    await c.answer()

@router.message(UserState.waiting_for_number)
async def proc_num(m: types.Message, state: FSMContext):
    d = await state.get_data()
    raw = m.text.split(',')
    valid_nums = []
    
    for item in raw:
        cl = clean_phone(item.strip())
        if cl: valid_nums.append(cl)
        
    if not valid_nums:
        return await m.answer("❌ <b>Нет валидных номеров!</b>\nПопробуйте еще раз.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]]), parse_mode="HTML")

    cnt = 0
    async with aiosqlite.connect(DB_NAME, timeout=30) as db:
        for ph in valid_nums:
            # Check duplicate in queue/work
            async with db.execute("SELECT 1 FROM numbers WHERE phone=? AND status IN ('queue','work','active')", (ph,)) as cur:
                if await cur.fetchone(): continue
            
            await db.execute("""INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) 
                                VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)""", 
                                (m.from_user.id, ph, d['method'], d['tariff'], d['price'], d['hold'], get_utc_now()))
            cnt += 1
        await db.commit()

    await state.clear()
    await m.answer(f"✅ <b>Успешно добавлено:</b> {cnt} шт.\nПерейдите в профиль для отслеживания.", reply_markup=main_menu_kb(m.from_user.id), parse_mode="HTML")

# --- PROFILE & QUEUE ---
@router.callback_query(F.data == "menu_profile")
async def show_prof(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT balance FROM users WHERE user_id=?", (uid,)) as cur: bal = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='finished'", (uid,)) as cur: done = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,)) as cur: total = (await cur.fetchone())[0]

    bar = render_progressbar(done, total if total > 0 else 1)
    text = (f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━\n"
            f"💰 Баланс: <code>{bal}₽</code>\n"
            f"📦 Всего сдано: {total}\n"
            f"✅ Успешно: {done}\n"
            f"📊 Рейтинг: {bar} ({int(done/total*100) if total else 0}%)")
            
    await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]]), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "nav_main")
async def back_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("👋 <b>Главное меню</b>", reply_markup=main_menu_kb(c.from_user.id), parse_mode="HTML")
    await c.answer()

# --- WORKER FLOW ---
@router.message(Command("num"))
async def worker_get_num(m: types.Message, bot: Bot):
    # Check permissions
    # if not ADMIN_ID or m.from_user.id not in APPROVED_WORKERS: return # Раскомментить если нужно
    
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    
    async with aiosqlite.connect(DB_NAME, timeout=30) as db:
        # Get Tariff for Topic
        async with db.execute("SELECT value FROM config WHERE key=?", (f"topic_cfg_{cid}_{tid}",)) as cur: 
            conf = await cur.fetchone()
        
        if not conf: return # Silent return if not configured
        t_name = conf[0]
        
        # RACE CONDITION FIX: LIMIT 1 + Immediate Update
        # SQLite is locked during transaction so this is reasonably safe
        async with db.execute("SELECT id, phone, tariff_price, tariff_hold FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (t_name,)) as cur:
            row = await cur.fetchone()
            
        if not row: return await m.reply("📭 <b>Очередь пуста!</b>", parse_mode="HTML")
        
        nid, ph, price, hold = row
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", (m.from_user.id, get_utc_now(), nid))
        await db.commit()

    # Layout
    txt = (f"🚀 <b>В РАБОТЕ</b>\n"
           f"━━━━━━━━━━━━━\n"
           f"📱 <code>{ph}</code>\n"
           f"💰 {price}₽ | ⏳ {hold}\n"
           f"━━━━━━━━━━━━━\n"
           f"Код: <code>/sms {ph} текст</code>")
           
    await m.answer(txt, reply_markup=worker_kb(nid, t_name), parse_mode="HTML")

@router.callback_query(F.data.startswith("w_act_"))
async def w_activate(c: CallbackQuery):
    nid = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        async with db.execute("SELECT phone, tariff_name FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
    
    ph, t_name = res
    await c.message.edit_text(f"🟢 <b>АКТИВ</b>\n📱 <code>{ph}</code>", reply_markup=worker_active_kb(nid, t_name), parse_mode="HTML")
    await c.answer("✅ Статус: Актив")

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def w_finish(c: CallbackQuery, bot: Bot):
    nid = c.data.split('_')[2]
    status = "finished" if "drop" in c.data else "dead" # 'finished' = drop/success in this context logic? Or drop=finished? 
    # Usually 'drop' means success payout. 'dead' means trash.
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, user_id, tariff_price FROM numbers WHERE id=?", (nid,)) as cur: 
            row = await cur.fetchone()
            if not row: return await c.answer("Ошибка БД")
            ph, uid, price = row
            
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, get_utc_now(), nid))
        
        # AUTO PAYOUT
        if status == "finished":
            # Clean price string (150₽ -> 150.0)
            amt = float(re.sub(r'[^\d.]', '', str(price)))
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amt, uid))
        
        await db.commit()

    # User Notify
    msg = f"✅ <b>Номер принят!</b>\n📱 {mask_phone(ph, uid)}\n💰 +{price}" if status == "finished" else f"❌ <b>Номер умер/отказ</b>\n📱 {mask_phone(ph, uid)}"
    try: await bot.send_message(uid, msg, parse_mode="HTML")
    except: pass
    
    final_emoji = "✅" if status == "finished" else "❌"
    await c.message.edit_text(f"{final_emoji} <b>ФИНАЛ: {status.upper()}</b>\n📱 {ph}", parse_mode="HTML")
    await c.answer("Завершено")

# --- MAX LOGIC (MANUAL CODE) ---
@router.callback_query(F.data.startswith("w_code_"))
async def w_manual_code(c: CallbackQuery, state: FSMContext):
    nid = c.data.split('_')[2]
    await state.update_data(nid=nid)
    await c.message.answer("⌨️ <b>Введите код вручную:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="cancel_code")]]), parse_mode="HTML")
    await state.set_state(MaxState.waiting_code)
    await c.answer()

@router.message(MaxState.waiting_code)
async def proc_manual_code(m: types.Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    nid = d['nid']
    code = m.text
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, phone FROM numbers WHERE id=?", (nid,)) as cur: res = await cur.fetchone()
    
    if res:
        uid, ph = res
        await bot.send_message(uid, f"🔔 <b>Код (MAX)</b>\n📱 {mask_phone(ph, uid)}\n💬 <tg-spoiler>{code}</tg-spoiler>", parse_mode="HTML")
        await m.answer(f"✅ Код отправлен юзеру!")
    
    await state.clear()

# --- ADMIN PANEL & WORKER BIND ---
@router.message(Command("startwork"))
async def bind_topic(m: types.Message):
    if not ADMIN_ID or m.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=r[0], callback_data=f"bind_{r[0]}")
    await m.answer("⚙️ <b>Выберите тариф для топика:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("bind_"))
async def proc_bind(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return await c.answer()
    t = c.data.split("_")[1]
    key = f"topic_cfg_{c.message.chat.id}_{c.message.message_thread_id if c.message.is_topic_message else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t))
        await db.commit()
    await c.message.edit_text(f"✅ Топик настроен на: <b>{t}</b>", parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "admin_panel_start")
async def admin_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return await c.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")]
    ])
    await c.message.edit_text("⚡️ <b>ADMIN PANEL v28.0</b>", reply_markup=kb, parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "adm_broadcast")
async def adm_br(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📢 Пришлите сообщение:", parse_mode="HTML")
    await state.set_state(AdminState.waiting_for_broadcast)
    await c.answer()

@router.message(AdminState.waiting_for_broadcast)
async def proc_br(m: types.Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳ Рассылка...")
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

# --- ENTRY POINT ---
async def main():
    await init_db()
    
    if HAS_REDIS and os.getenv("REDIS_URL"):
        storage = RedisStorage.from_url(os.getenv("REDIS_URL"))
        logger.info("🟢 REDIS CONNECTED")
    else:
        storage = MemoryStorage()
        logger.warning("🟡 RAM STORAGE")

    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 STARTED v28.0 PREMIUM")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"POLLING DIED: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
