import asyncio
import logging
import sys
import os
import re
import csv
import io
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

try:
    import aiosqlite
    from aiogram import Bot, Dispatcher, Router, F
    from aiogram.filters import Command, CommandStart, CommandObject
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, ReactionTypeEmoji, BufferedInputFile
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from aiogram.exceptions import TelegramForbiddenError
except ImportError:
    sys.exit("❌ pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))
DB_NAME = "bot_mega.db"

# ИСПРАВЛЕННЫЕ ТАЙМЕРЫ
AFK_CHECK_MINUTES = 15  # Проверка каждые 15 минут
AFK_KICK_MINUTES = 10   # Удаление через 10 минут
CODE_WAIT_MINUTES = 5   # Ожидание кода 5 минут

SEP = "━━━━━━━━━━━━━━━━━━━━"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

# ==========================================
# БАЗА ДАННЫХ
# ==========================================
@asynccontextmanager
async def get_db():
    conn = await aiosqlite.connect(DB_NAME, timeout=30)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    try: 
        yield conn
    finally: 
        await conn.close()

async def init_db():
    async with get_db() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_approved INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                last_afk_check TEXT,
                afk_warning_sent INTEGER DEFAULT 0,
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                phone TEXT,
                phone_hash TEXT,
                tariff_name TEXT,
                tariff_price TEXT,
                work_time TEXT,
                status TEXT DEFAULT 'queue',
                worker_id INTEGER DEFAULT 0,
                worker_chat_id INTEGER DEFAULT 0,
                worker_thread_id INTEGER DEFAULT 0,
                start_time TEXT,
                end_time TEXT,
                last_ping TEXT,
                wait_code_start TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.execute("CREATE INDEX IF NOT EXISTS idx_active_numbers ON numbers(phone_hash, status) WHERE status IN ('queue', 'work', 'active')")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY,
                price TEXT,
                work_time TEXT
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_num INTEGER PRIMARY KEY,
                chat_id INTEGER,
                title TEXT
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '24/7')")
        
        await db.commit()
    logger.info("✅ Database initialized - FIXED VERSION")

# ==========================================
# УТИЛИТЫ
# ==========================================
def clean_phone(phone):
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11: 
        return '+' + clean
    if clean.startswith('8') and len(clean) == 11: 
        clean = '7' + clean[1:]
    elif len(clean) == 10: 
        clean = '7' + clean
    return '+' + clean if re.match(r'^7\d{10}$', clean) else None

def get_phone_hash(phone):
    return re.sub(r'[^\d]', '', str(phone))

def mask_phone(phone, user_id):
    if user_id == ADMIN_ID: 
        return phone
    try:
        if len(phone) < 9: 
            return phone
        return f"{phone[:5]}***{phone[-4:]}"
    except: 
        return phone

def get_now():
    return datetime.now(timezone.utc).isoformat()

def format_time(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        return (dt + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M МСК")
    except: 
        return "-"

def calc_duration(start_iso, end_iso):
    try:
        if not start_iso or not end_iso: 
            return "0 мин"
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: 
        return "0 мин"

# ==========================================
# FSM СОСТОЯНИЯ
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_help = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_price = State()
    edit_time = State()
    help_reply = State()
    report_hours = State()

# ==========================================
# КЛАВИАТУРЫ
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Информация", callback_data="guide")
    kb.button(text="🆘 Помощь", callback_data="ask_help")
    if user_id == ADMIN_ID:
        kb.button(text="⚡ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 1, 1)
    return kb.as_markup()

def worker_kb_whatsapp(nid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
    kb.button(text="❌ Ошибка", callback_data=f"w_err_{nid}")
    return kb.as_markup()

def worker_kb_max(nid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
    kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{nid}")
    return kb.as_markup()

def worker_active_kb(nid):
    return InlineKeyboardBuilder().button(text="📉 Слет", callback_data=f"w_drop_{nid}").as_markup()

# ==========================================
# КОМАНДЫ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"),
                    InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                ]])
                try:
                    await m.bot.send_message(ADMIN_ID, f"👤 Запрос доступа: {uid} (@{m.from_user.username})", reply_markup=kb)
                except:
                    pass
            
            return await m.answer("🔒 Доступ ограничен.\nОжидайте одобрения.")
        
        if res['is_banned']:
            return await m.answer("🚫 Вы заблокированы.")
        
        if res['is_approved']:
            await m.answer(f"👋 Привет, {m.from_user.first_name}!\n{SEP}", reply_markup=main_kb(uid))
        else:
            await m.answer("⏳ Заявка на рассмотрении.")

@router.message(Command("bindgroup"))
async def cmd_bindgroup(m: Message, command: CommandObject):
    if m.from_user.id != ADMIN_ID:
        return
    
    if not command.args:
        return await m.reply("❌ Использование: /bindgroup 1")
    
    try:
        group_num = int(command.args.strip())
        if group_num not in [1, 2, 3]:
            raise ValueError
    except:
        return await m.reply("❌ Номер группы: 1, 2 или 3")
    
    chat_id = m.chat.id
    title = m.chat.title or f"Chat {chat_id}"
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO groups (group_num, chat_id, title) VALUES (?, ?, ?)", (group_num, chat_id, title))
        await db.commit()
    
    await m.answer(f"✅ Чат привязан к группе {group_num}!\n\n📋 Инструкция:\n\n1️⃣ /num → Получить номер\n2️⃣ Вбить в WhatsApp Web\n3️⃣ Код → /sms +7... текст\n4️⃣ Встал → ✅ Встал\n5️⃣ Слетел → 📉 Слет")

@router.message(Command("startwork"))
async def cmd_startwork(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
    
    async with get_db() as db:
        tariffs = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in tariffs:
        kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    kb.adjust(1)
    
    await m.answer(f"⚙️ Настройка воркера\n{SEP}\nВыберите тариф:", reply_markup=kb.as_markup())

@router.message(Command("stopwork"))
async def cmd_stopwork(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
    
    chat_id = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (f"topic_{chat_id}_{tid}",))
        await db.commit()
    
    await m.reply("🛑 Топик отключен.")

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        
        if not conf:
            return await m.reply("❌ Топик не настроен (/startwork)")
        
        tariff_name = conf['value']
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (tariff_name,))).fetchone()
        
        if not row:
            return await m.reply("📭 Очередь пуста")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?", (m.from_user.id, cid, tid, get_now(), row['id']))
        await db.commit()
    
    if "MAX" in tariff_name.upper():
        msg = f"🚀 Вы взяли номер\n{SEP}\n📱 {row['phone']}\n💰 {row['tariff_price']}\n\nПользователь запросит push/QR\nКод: /code {row['phone']}"
        kb = worker_kb_max(row['id'])
    else:
        msg = f"🚀 Вы взяли номер\n{SEP}\n📱 {row['phone']}\n💰 {row['tariff_price']}\n\nКод: /sms {row['phone']} текст"
        kb = worker_kb_whatsapp(row['id'])
    
    await m.answer(msg, reply_markup=kb)
    
    try:
        await bot.send_message(row['user_id'], f"⚡ Ваш номер взяли в работу\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}\n⏳ Ожидайте код")
    except:
        pass

@router.message(Command("sms"))
async def cmd_sms(m: Message, command: CommandObject, bot: Bot):
    if not command.args:
        return await m.reply("⚠️ Формат: /sms +7999... текст кода")
    
    parts = command.args.split(maxsplit=1)
    if len(parts) < 2:
        return await m.reply("⚠️ Укажите код после номера")
    
    ph = clean_phone(parts[0])
    code_text = parts[1]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Не ваш номер")
    
    try:
        await bot.send_message(row['user_id'], f"📩 Код: {code_text}\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}\n\nОтправьте код дропу")
        await m.reply(f"✅ Код отправлен пользователю")
    except:
        await m.reply("❌ Ошибка доставки")

@router.message(Command("code"))
async def cmd_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args:
        return await m.reply("⚠️ Пример: /code +7999...")
    
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Не ваш номер")
    
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now(), row['id']))
        await db.commit()
    
    try:
        await bot.send_message(row['user_id'], f"🔔 Офис запросил код\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}\n\nОтветьте сообщением с кодом")
        await m.reply("✅ Запрос отправлен")
    except:
        await m.reply("❌ Ошибка доставки")
        # ПРОДОЛЖЕНИЕ - CALLBACK ХЭНДЛЕРЫ

@router.callback_query(F.data == "guide")
async def cb_guide(c: CallbackQuery):
    await c.message.edit_text(
        f"📲 Что делает бот\n{SEP}\nБот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n📦 Требования к номерам\n✔️ Активный и чистый номер\n✔️ Доступ к SMS\n❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n⏳ Холд и выплаты\nХолд — время проверки номера\n💰 Выплата производится после успешного завершения холда\n\n⚠️ ОДИН НОМЕР можно сдать ОДИН РАЗ\nПовторная отправка одного номера заблокирована системой\n\nПоддержка: @whitte_work",
        reply_markup=InlineKeyboardBuilder().button(text="🔙 Меню", callback_data="back_main").as_markup()
    )

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status IN ('work', 'active')", (uid,))).fetchone())[0]
        in_queue = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='queue'", (uid,))).fetchone())[0]
        
        my_first = await (await db.execute("SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1", (uid,))).fetchone()
        
        q_pos = 0
        if my_first:
            q_pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?", (my_first[0],))).fetchone())[0] + 1
        
        recent = await (await db.execute("SELECT phone, status, tariff_price FROM numbers WHERE user_id=? ORDER BY id DESC LIMIT 3", (uid,))).fetchall()
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 История", callback_data="my_nums")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    txt = f"👤 Личный кабинет\n{SEP}\n🆔 ID: {uid}\n📦 Всего сдано: {total}\n🟡 В очереди: {in_queue}\n🔥 В работе: {active}\n\n"
    
    if q_pos:
        txt += f"🕒 Ваша позиция: {q_pos}\n\n"
    
    if recent:
        txt += f"📱 Последние номера:\n"
        for r in recent:
            icon = "🟡" if r['status'] == 'queue' else "🟢" if r['status'] in ('work', 'active') else "✅" if r['status'] == 'finished' else "❌"
            txt += f"{icon} {mask_phone(r['phone'], uid)} | {r['tariff_price']}\n"
    
    await c.message.edit_text(txt, reply_markup=kb.as_markup())

@router.callback_query(F.data == "my_nums")
async def cb_my_nums(c: CallbackQuery):
    uid = c.from_user.id
    
    async with get_db() as db:
        rows = await (await db.execute("SELECT id, phone, status, tariff_price FROM numbers WHERE user_id=? ORDER BY id DESC LIMIT 10", (uid,))).fetchall()
    
    kb = InlineKeyboardBuilder()
    txt = f"📝 История номеров\n{SEP}\n"
    
    if not rows:
        txt += "📭 Пусто"
    else:
        for r in rows:
            icon = "🟡" if r['status'] == 'queue' else "🟢" if r['status'] in ('work', 'active') else "✅" if r['status'] == 'finished' else "❌"
            txt += f"{icon} {mask_phone(r['phone'], uid)} | {r['tariff_price']}\n"
            
            if r['status'] == 'queue':
                kb.button(text=f"🗑 Удалить {mask_phone(r['phone'], uid)}", callback_data=f"del_{r['id']}")
    
    kb.button(text="🔙 Назад", callback_data="profile")
    kb.adjust(1)
    
    await c.message.edit_text(txt, reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("del_"))
async def cb_del(c: CallbackQuery):
    nid = c.data.split("_")[1]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT status FROM numbers WHERE id=? AND user_id=?", (nid, c.from_user.id))).fetchone()
        
        if row and row['status'] == 'queue':
            await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
            await db.commit()
            await c.answer("✅ Номер удален")
            await cb_my_nums(c)
        else:
            await c.answer("❌ Номер уже в работе!", show_alert=True)

@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    if not tariffs:
        return await c.message.edit_text("❌ Тарифы не настроены!", reply_markup=main_kb(c.from_user.id))
    
    kb = InlineKeyboardBuilder()
    for t in tariffs:
        kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text(f"📂 Выберите тариф\n{SEP}", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    
    async with get_db() as db:
        t = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (tn,))).fetchone()
    
    await state.update_data(tariff=tn, price=t['price'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)
    
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    
    await c.message.edit_text(
        f"💎 Тариф: {tn}\n{SEP}\n💰 Прайс: {t['price']}\n⏰ Время работы: {t['work_time']}\n\n📱 Отправьте номера списком или по одному\n⚠️ ОДИН НОМЕР = ОДНА ОТПРАВКА",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data == "ask_help")
async def cb_ask_help(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_help)
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text(f"🆘 Помощь\n{SEP}\nНапишите свой запрос:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind(c: CallbackQuery):
    tn = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", tn))
        await db.commit()
    
    await c.message.edit_text(f"✅ Топик привязан! Тариф: {tn}\nПиши /num чтобы взять номер")

@router.callback_query(F.data.startswith("w_act_"))
async def cb_w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        await db.execute("UPDATE numbers SET status='active', last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    
    await c.message.edit_text(f"✅ Номер встал\n{SEP}\n📱 {row['phone']}", reply_markup=worker_active_kb(nid))
    
    try:
        await bot.send_message(row['user_id'], f"✅ Номер встал\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}")
    except:
        pass

@router.callback_query(F.data.startswith("w_skip_"))
async def cb_w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        await db.execute("UPDATE numbers SET status='queue', worker_id=0, worker_chat_id=0, worker_thread_id=0, start_time=NULL WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text(f"⏭ Пропуск\n{SEP}\n📱 {row['phone']}")
    
    try:
        await bot.send_message(row['user_id'], f"⏭ Номер вернули в очередь\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}")
    except:
        pass

@router.callback_query(F.data.startswith("w_err_"))
async def cb_w_err(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        await db.execute("UPDATE numbers SET status='error', end_time=? WHERE id=?", (get_now(), nid))
        await db.commit()
    
    await c.message.edit_text(f"❌ Ошибка зафиксирована\n{SEP}\n📱 {row['phone']}")
    
    try:
        await bot.send_message(row['user_id'], f"❌ Ошибка на номере\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}")
    except:
        pass

@router.callback_query(F.data.startswith("w_drop_"))
async def cb_w_drop(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        await db.execute("UPDATE numbers SET status='dropped', end_time=? WHERE id=?", (get_now(), nid))
        await db.commit()
    
    await c.message.edit_text(f"📉 Слет зафиксирован\n{SEP}\n📱 {row['phone']}")
    
    try:
        await bot.send_message(row['user_id'], f"📉 Номер слетел\n{SEP}\n📱 {mask_phone(row['phone'], row['user_id'])}")
    except:
        pass

@router.callback_query(F.data == "back_main")
async def cb_back_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"🏠 Главное меню\n{SEP}", reply_markup=main_kb(c.from_user.id))

@router.callback_query(F.data.startswith("acc_ok_"))
async def cb_acc_ok(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID:
        return
    
    uid = int(c.data.split("_")[2])
    
    async with get_db() as db:
        await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
        await db.commit()
    
    await c.message.edit_text(f"✅ Пользователь {uid} одобрен")
    
    try:
        await bot.send_message(uid, f"✅ Доступ одобрен!\n{SEP}", reply_markup=main_kb(uid))
    except:
        pass

@router.callback_query(F.data.startswith("acc_no_"))
async def cb_acc_no(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID:
        return
    
    uid = int(c.data.split("_")[2])
    
    async with get_db() as db:
        await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
        await db.commit()
    
    await c.message.edit_text(f"🚫 Пользователь {uid} заблокирован")
    
    try:
        await bot.send_message(uid, "🚫 Доступ запрещен")
    except:
        pass

# ОБРАБОТКА СООБЩЕНИЙ С НОМЕРАМИ
@router.message(UserState.waiting_numbers)
async def handle_numbers(m: Message, state: FSMContext, bot: Bot):
    uid = m.from_user.id
    data = await state.get_data()
    tariff = data.get('tariff')
    price = data.get('price')
    work_time = data.get('work_time')
    
    phones = [clean_phone(p) for p in re.findall(r'[\d\+\s\-\(\)]+', m.text) if clean_phone(p)]
    
    if not phones:
        return await m.reply("❌ Номера не найдены")
    
    added = 0
    duplicates = []
    
    async with get_db() as db:
        for ph in phones:
            ph_hash = get_phone_hash(ph)
            
            # ПРОВЕРКА НА ДУБЛЬ
            exists = await (await db.execute("SELECT id FROM numbers WHERE phone_hash=? AND status IN ('queue', 'work', 'active')", (ph_hash,))).fetchone()
            
            if exists:
                duplicates.append(ph)
                continue
            
            await db.execute(
                "INSERT INTO numbers (user_id, phone, phone_hash, tariff_name, tariff_price, work_time) VALUES (?, ?, ?, ?, ?, ?)",
                (uid, ph, ph_hash, tariff, price, work_time)
            )
            added += 1
        
        await db.commit()
    
    msg = f"✅ Добавлено: {added}\n"
    
    if duplicates:
        msg += f"\n❌ Дубли (уже в системе): {len(duplicates)}\n"
        for dup in duplicates[:5]:
            msg += f"• {mask_phone(dup, uid)}\n"
    
    await m.reply(msg)
    await state.clear()

@router.message(UserState.waiting_help)
async def handle_help(m: Message, state: FSMContext, bot: Bot):
    uid = m.from_user.id
    text = m.text
    
    await state.clear()
    
    try:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📝 Ответить", callback_data=f"help_reply_{uid}")
        ]])
        await bot.send_message(ADMIN_ID, f"🆘 Запрос помощи от {uid}:\n\n{text}", reply_markup=kb)
        await m.reply("✅ Запрос отправлен администратору")
    except:
        await m.reply("❌ Ошибка отправки")

# BACKGROUND TASKS
async def afk_checker(bot: Bot):
    while True:
        await asyncio.sleep(AFK_CHECK_MINUTES * 60)
        
        async with get_db() as db:
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=AFK_KICK_MINUTES)).isoformat()
            
            afk_nums = await (await db.execute(
                "SELECT id, user_id, phone, worker_id, worker_chat_id, worker_thread_id FROM numbers WHERE status='active' AND last_ping < ?",
                (cutoff,)
            )).fetchall()
            
            for n in afk_nums:
                await db.execute("DELETE FROM numbers WHERE id=?", (n['id'],))
                
                try:
                    await bot.send_message(n['user_id'], f"❌ Все номера удалены (AFK)\n{SEP}\n📱 {mask_phone(n['phone'], n['user_id'])}")
                except:
                    pass
            
            if afk_nums:
                await db.commit()
                logger.info(f"🗑 AFK: удалено {len(afk_nums)} номеров")

async def code_timeout_checker(bot: Bot):
    while True:
        await asyncio.sleep(60)
        
        async with get_db() as db:
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=CODE_WAIT_MINUTES)).isoformat()
            
            expired = await (await db.execute(
                "SELECT id, user_id, phone FROM numbers WHERE wait_code_start IS NOT NULL AND wait_code_start < ?",
                (cutoff,)
            )).fetchall()
            
            for n in expired:
                await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (n['id'],))
            
            if expired:
                await db.commit()
                logger.info(f"⏰ Code timeout: {len(expired)} запросов")

async def main():
    await init_db()
    
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    asyncio.create_task(afk_checker(bot))
    asyncio.create_task(code_timeout_checker(bot))
    
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
