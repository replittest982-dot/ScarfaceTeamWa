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
DB_NAME = "bot_final.db"

# Таймеры
AFK_CHECK_MINUTES = 8
AFK_KICK_MINUTES = 3
CODE_WAIT_MINUTES = 4

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
        # Юзеры
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_approved INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Номера
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                phone TEXT,
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
                code_type TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Тарифы
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY,
                price TEXT,
                work_time TEXT
            )
        """)
        
        # Группы
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_num INTEGER PRIMARY KEY,
                chat_id INTEGER,
                title TEXT
            )
        """)
        
        # Конфиг
        await db.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '24/7')")
        
        await db.commit()
    logger.info("✅ Database initialized")

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
    kb.button(text="ℹ️ Помощь", callback_data="guide")
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

def code_type_kb(nid):
    kb = InlineKeyboardBuilder()
    kb.button(text="📱 QR код", callback_data=f"code_qr_{nid}")
    kb.button(text="🔢 Обычный код", callback_data=f"code_sms_{nid}")
    return kb.as_markup()

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
            await db.execute(
                "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (uid, m.from_user.username, m.from_user.first_name)
            )
            await db.commit()
            
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"),
                    InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                ]])
                try:
                    await m.bot.send_message(
                        ADMIN_ID,
                        f"👤 Запрос доступа: {uid} (@{m.from_user.username})",
                        reply_markup=kb
                    )
                except:
                    pass
            
            return await m.answer("🔒 Доступ ограничен.\nОжидайте одобрения.")
        
        if res['is_banned']:
            return await m.answer("🚫 Вы заблокированы.")
        
        if res['is_approved']:
            await m.answer(
                f"👋 Привет, {m.from_user.first_name}!\n{SEP}",
                reply_markup=main_kb(uid)
            )
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
        await db.execute(
            "INSERT OR REPLACE INTO groups (group_num, chat_id, title) VALUES (?, ?, ?)",
            (group_num, chat_id, title)
        )
        await db.commit()
    
    await m.answer(
        f"✅ Чат привязан!\n\n"
        f"👨‍💻 Гайд по использованию:\n\n"
        f"1️⃣ Пиши /num -> Получишь номер.\n\n"
        f"2️⃣ Вбей номер в WhatsApp Web.\n\n"
        f"3️⃣ Если просят QR: Сфоткай QR с экрана.\n"
        f"   Скинь фото сюда и подпиши: /sms +77... Сканируй\n\n"
        f"4️⃣ Если просят Код (по номеру): Сфоткай код с экрана.\n"
        f"   Скинь фото сюда и подпиши: /sms +77... Вводи этот код\n\n"
        f"5️⃣ Когда зашел -> жми ✅ Встал.\n"
        f"6️⃣ Когда номер слетел -> жми 📉 Слет.\n\n"
        f"⚠️ Не забудьте подтвердить номер когда он встанет!"
    )

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
    
    await m.answer(
        "⚙️ Настройка воркера\n{SEP}\nВыберите тариф:",
        reply_markup=kb.as_markup()
    )

@router.message(Command("stopwork"))
async def cmd_stopwork(m: Message, bot: Bot):
    if m.from_user.id != ADMIN_ID:
        return
    
    chat_id = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute(
            "DELETE FROM config WHERE key=?",
            (f"topic_{chat_id}_{tid}",)
        )
        await db.commit()
    
    await m.reply("🛑 Топик отключен.")

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        conf = await (await db.execute(
            "SELECT value FROM config WHERE key=?",
            (f"topic_{cid}_{tid}",)
        )).fetchone()
        
        if not conf:
            return await m.reply("❌ Топик не настроен")
        
        tariff_name = conf['value']
        
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1",
            (tariff_name,)
        )).fetchone()
        
        if not row:
            return await m.reply("📭 Очередь пуста")
        
        await db.execute(
            "UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?",
            (m.from_user.id, cid, tid, get_now(), row['id'])
        )
        await db.commit()
    
    # Сообщение воркеру
    if "MAX" in tariff_name.upper():
        msg = (
            f"🚀 Вы взяли номер\n{SEP}\n"
            f"📱 {row['phone']}\n\n"
            f"Пользователь запросит push-уведомление/QR\n"
            f"Используйте /code {row['phone']} для запроса кода"
        )
        kb = worker_kb_max(row['id'])
    else:
        msg = (
            f"🚀 Вы взяли номер\n{SEP}\n"
            f"📱 {row['phone']}\n\n"
            f"Код: /sms {row['phone']} текст"
        )
        kb = worker_kb_whatsapp(row['id'])
    
    await m.answer(msg, reply_markup=kb)
    
    # Сообщение юзеру
    try:
        await bot.send_message(
            row['user_id'],
            f"⚡ Ваш номер взяли\n{SEP}\n"
            f"📱 {mask_phone(row['phone'], row['user_id'])}\n"
            f"Ожидайте код"
        )
    except:
        pass

@router.message(Command("code"))
async def cmd_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args:
        return await m.reply("⚠️ Пример: /code +7999...")
    
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')",
            (ph,)
        )).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Не ваш номер")
    
    async with get_db() as db:
        await db.execute(
            "UPDATE numbers SET wait_code_start=? WHERE id=?",
            (get_now(), row['id'])
        )
        await db.commit()
    
    try:
        await bot.send_message(
            row['user_id'],
            f"🔔 Офис запросил код\n{SEP}\n"
            f"📱 {mask_phone(row['phone'], row['user_id'])}\n\n"
            f"Ответьте ниже сообщением чтобы дать код"
        )
        await m.reply("✅ Запрос отправлен")
    except:
        await m.reply("❌ Ошибка доставки")

# ==========================================
# CALLBACK ХЭНДЛЕРЫ
# ==========================================
@router.callback_query(F.data == "guide")
async def cb_guide(c: CallbackQuery):
    await c.message.edit_text(
        f"📲 Что делает бот\n"
        f"Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
        f"📦 Требования к номерам\n"
        f"✔️ Активный и чистый номер\n"
        f"✔️ Доступ к SMS\n"
        f"❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
        f"⏳ Холд и выплаты\n"
        f"Холд — время проверки номера\n"
        f"💰 Выплата производится после успешного завершения холда\n\n"
        f"⚠️ Отправляя номер, вы подтверждаете, что ознакомились с правилами\n\n"
        f"Поддержка: @whitte_work",
        reply_markup=main_kb(c.from_user.id)
    )

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    
    async with get_db() as db:
        total = (await (await db.execute(
            "SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,)
        )).fetchone())[0]
        
        active = (await (await db.execute(
            "SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='active'", (uid,)
        )).fetchone())[0]
        
        # Позиция в очереди
        my_first = await (await db.execute(
            "SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1",
            (uid,)
        )).fetchone()
        
        q_pos = 0
        if my_first:
            q_pos = (await (await db.execute(
                "SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?",
                (my_first[0],)
            )).fetchone())[0] + 1
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 История", callback_data="my_nums")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text(
        f"👤 Личный кабинет\n{SEP}\n"
        f"🆔 ID: {uid}\n"
        f"📦 Всего сдано: {total}\n"
        f"🔥 В работе: {active}\n"
        f"🕒 Позиция в очереди: {q_pos if q_pos else '-'}",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data == "my_nums")
async def cb_my_nums(c: CallbackQuery):
    uid = c.from_user.id
    
    async with get_db() as db:
        rows = await (await db.execute(
            "SELECT id, phone, status, tariff_price FROM numbers WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (uid,)
        )).fetchall()
    
    kb = InlineKeyboardBuilder()
    txt = f"📝 Последние 10 номеров\n{SEP}\n"
    
    if not rows:
        txt += "📭 История пуста"
    else:
        for r in rows:
            icon = "🟡" if r['status'] == 'queue' else "🟢" if r['status'] == 'active' else "✅" if r['status'] == 'finished' else "❌"
            txt += f"{icon} {mask_phone(r['phone'], uid)} | {r['tariff_price']}\n"
            
            if r['status'] == 'queue':
                kb.button(
                    text=f"🗑 Удалить {mask_phone(r['phone'], uid)}",
                    callback_data=f"del_{r['id']}"
                )
    
    kb.button(text="🔙 Назад", callback_data="profile")
    kb.adjust(1)
    
    await c.message.edit_text(txt, reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("del_"))
async def cb_del(c: CallbackQuery):
    nid = c.data.split("_")[1]
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT status FROM numbers WHERE id=? AND user_id=?",
            (nid, c.from_user.id)
        )).fetchone()
        
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
        return await c.message.edit_text(
            "❌ Тарифы не настроены!",
            reply_markup=main_kb(c.from_user.id)
        )
    
    kb = InlineKeyboardBuilder()
    for t in tariffs:
        kb.button(
            text=f"{t['name']} | {t['price']}",
            callback_data=f"pick_{t['name']}"
        )
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text(
        f"📂 Выберите тариф\n{SEP}",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    
    async with get_db() as db:
        t = await (await db.execute(
            "SELECT * FROM tariffs WHERE name=?",
            (tn,)
        )).fetchone()
    
    await state.update_data(tariff=tn, price=t['price'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)
    
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    
    await c.message.edit_text(
        f"💎 Тариф: {tn}\n{SEP}\n"
        f"💰 Прайс: {t['price']}\n"
        f"⏰ Время работы: {t['work_time']}\n\n"
        f"📱 Отправьте номера списком или по одному",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data == "ask_help")
async def cb_ask_help(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_help)
    
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    
    await c.message.edit_text(
        "🆘 Помощь\n{SEP}\nНапишите свой запрос:",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind(c: CallbackQuery):
    tn = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (f"topic_{cid}_{tid}", tn)
        )
        await db.commit()
    
    await c.message.edit_text(
        f"✅ Топик привязан! Тариф: {tn}\n"
        f"Пиши /num чтобы взять номер"
    )

@router.callback_query(F.data.startswith("w_act_"))
async def cb_w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE id=?",
            (nid,)
        )).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        await db.execute(
            "UPDATE numbers SET status='active' WHERE id=?",
            (nid,)
        )
        await db.commit()
    
    await c.message.edit_text(
        f"✅ Номер встал",
        reply_markup=worker_active_kb(nid)
    )
    
    try:
        await bot.send_message(
            row['user_id'],
            "✅ Номер встал"
        )
    except:
        pass

@router.callback_query(F.data.startswith("w_skip_"))
async def cb_w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE id=?",
            (nid,)
        )).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        await db.execute(
            "UPDATE numbers SET status='queue', worker_id=0, worker_chat_id=0 WHERE id=?",
            (nid,)
        )
        await db.commit()
    
    await c.message.edit_text("⏭ Пропуск (номер вернулся в очередь)")
    
    try:
        await bot.send_message(
            row['user_id'],
            "⏭ Офис пропустил ваш номер"
        )
    except:
        pass

@router.callback_query(F.data.startswith(("w_drop_", "w_err_")))
async def cb_w_finish(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE id=?",
            (nid,)
        )).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ты брал номер!", show_alert=True)
        
        status = "finished" if is_drop else "dead"
        end_time = get_now()
        duration = calc_duration(row['start_time'], end_time)
        
        await db.execute(
            "UPDATE numbers SET status=?, end_time=? WHERE id=?",
            (status, end_time, nid)
        )
        await db.commit()
    
    if is_drop:
        msg = f"📉 Слет"
        user_msg = f"📉 Ваш номер слетел\n{SEP}\n⏱ Время работы: {duration}"
    else:
        msg = "❌ Ошибка"
        user_msg = "❌ Ошибка"
    
    await c.message.edit_text(msg)
    
    try:
        await bot.send_message(row['user_id'], user_msg)
    except:
        pass

@router.callback_query(F.data == "back_main")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(
        f"👋 Главное меню\n{SEP}",
        reply_markup=main_kb(c.from_user.id)
    )

@router.callback_query(F.data.startswith("acc_"))
async def cb_acc(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID:
        return
    
    action, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    
    async with get_db() as db:
        if action == "ok":
            await db.execute(
                "UPDATE users SET is_approved=1 WHERE user_id=?",
                (uid,)
            )
            await db.commit()
            await c.message.edit_text(f"✅ Юзер {uid} принят")
            
            try:
                await bot.send_message(
                    uid,
                    "✅ Доступ открыт!\nЖмите /start"
                )
            except:
                pass
        else:
            await db.execute(
                "UPDATE users SET is_banned=1 WHERE user_id=?",
                (uid,)
            )
            await db.commit()
            await c.message.edit_text(f"🚫 Юзер {uid} забанен")

@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk(c: CallbackQuery):
    nid = c.data.split("_")[2]
    
    async with get_db() as db:
        await db.execute(
            "UPDATE numbers SET last_ping=? WHERE id=?",
            (get_now(), nid)
        )
        await db.commit()
    
    await c.message.delete()
    await c.answer("✅ Вы в очереди!")

# ==========================================
# АДМИНКА
# ==========================================
@router.callback_query(F.data == "admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Изменить тарифы", callback_data="adm_tariffs")
    kb.button(text="📊 Отчеты", callback_data="adm_reports")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🏢 Группы", callback_data="manage_groups")
    kb.button(text="📋 Общая очередь", callback_data="all_queue")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text(
        "⚡ Админ панель\n{SEP}",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data == "all_queue")
async def cb_all_queue(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return
    
    async with get_db() as db:
        # Все номера в очереди
        queue = await (await db.execute(
            "SELECT id, phone, tariff_name, created_at FROM numbers WHERE status='queue' ORDER BY id ASC"
        )).fetchall()
        
        # Все номера в работе
        active = await (await db.execute(
            "SELECT id, phone, tariff_name, worker_id, start_time FROM numbers WHERE status IN ('work', 'active') ORDER BY id ASC"
        )).fetchall()
    
    txt = f"📋 ОБЩАЯ ОЧЕРЕДЬ\n{SEP}\n\n"
    
    txt += f"🟡 В ОЧЕРЕДИ ({len(queue)}):\n"
    if queue:
        for i, r in enumerate(queue[:20], 1):  # Показываем первые 20
            txt += f"{i}. {r['phone']} | {r['tariff_name']}\n"
        if len(queue) > 20:
            txt += f"...и еще {len(queue) - 20} номеров\n"
    else:
        txt += "Пусто\n"
    
    txt += f"\n🟢 В РАБОТЕ ({len(active)}):\n"
    if active:
        for r in active[:20]:
            txt += f"📱 {r['phone']} | {r['tariff_name']} | Воркер: {r['worker_id']}\n"
        if len(active) > 20:
            txt += f"...и еще {len(active) - 20} номеров\n"
    else:
        txt += "Пусто\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Назад", callback_data="admin_main")
    
    await c.message.edit_text(txt, reply_markup=kb.as_markup())

@router.callback_query(F.data == "manage_groups")
async def cb_mgr(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return
    
    async with get_db() as db:
        groups = await (await db.execute(
            "SELECT * FROM groups ORDER BY group_num"
        )).fetchall()
    
    kb = InlineKeyboardBuilder()
    
    for i in range(1, 4):
        g_name = "Не привязана"
        for g in groups:
            if g['group_num'] == i:
                g_name = g['title']
                break
        
        kb.button(
            text=f"🛑 Стоп: {g_name}",
            callback_data=f"stop_group_{i}"
        )
    
    kb.button(text="📊 Статус", callback_data="groups_status")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    
    await c.message.edit_text(
        "🏢 Управление группами\n{SEP}\nВыберите группу:",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data.startswith("stop_group_"))
async def cb_stop_g(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID:
        return
    
    gn = int(c.data.split("_")[-1])
    stop_time = get_now()
    
    async with get_db() as db:
        g = await (await db.execute(
            "SELECT * FROM groups WHERE group_num=?",
            (gn,)
        )).fetchone()
        
        if not g:
            return await c.answer(
                f"❌ Группа {gn} не привязана!",
                show_alert=True
            )
        
        cid, title = g['chat_id'], g['title']
        
        nums = await (await db.execute("""
            SELECT id, user_id, phone, start_time 
            FROM numbers 
            WHERE status IN ('work','active') AND worker_chat_id=?
        """, (cid,))).fetchall()
        
        stopped = 0
        for num in nums:
            await db.execute(
                "UPDATE numbers SET status=?, end_time=? WHERE id=?",
                (f"finished_group_{gn}", stop_time, num['id'])
            )
            stopped += 1
            
            duration = calc_duration(num['start_time'], stop_time)
            try:
                await bot.send_message(
                    num['user_id'],
                    f"🛑 {title} остановлен\n{SEP}\n"
                    f"📱 {mask_phone(num['phone'], num['user_id'])}\n"
                    f"⏰ {format_time(stop_time)}\n"
                    f"⏱ Работа: {duration}"
                )
            except:
                pass
        
        await db.commit()
    
    await c.message.edit_text(
        f"🛑 Группа {gn} остановлена\n{SEP}\n"
        f"🏢 {title}\n"
        f"⏰ {format_time(stop_time)}\n"
        f"📦 Остановлено: {stopped}"
    )

@router.callback_query(F.data == "groups_status")
async def cb_g_stat(c: CallbackQuery):
    async with get_db() as db:
        stats = {}
        for i in range(1, 4):
            stats[f"Группа {i}"] = (await (await db.execute(
                "SELECT COUNT(*) FROM numbers WHERE status=?",
                (f"finished_group_{i}",)
            )).fetchone())[0]
        
        active = (await (await db.execute(
            "SELECT COUNT(*) FROM numbers WHERE status IN ('work','active')"
        )).fetchone())[0]
        
        queue = (await (await db.execute(
            "SELECT COUNT(*) FROM numbers WHERE status='queue'"
        )).fetchone())[0]
    
    txt = f"📊 СТАТУС\n{SEP}\n"
    for g, cnt in stats.items():
        txt += f"🏁 {g}: {cnt}\n"
    txt += f"\n🔥 Активно: {active}\n🟡 Очередь: {queue}"
    
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="manage_groups")
    
    await c.message.edit_text(txt, reply_markup=kb.as_markup())

@router.callback_query(F.data == "adm_tariffs")
async def cb_adm_t(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return
    
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in ts:
        kb.button(
            text=f"✏️ {t['name']}",
            callback_data=f"ed_{t['name']}"
        )
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    
    await c.message.edit_text(
        "🛠 Выберите тариф:",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data.startswith("ed_"))
async def cb_ed_t(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return
    
    target = c.data.split("_")[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_price)
    
    await c.message.edit_text(
        f"1️⃣ Введите ЦЕНУ для {target}\n"
        f"Пример: 50₽, 10$"
    )

@router.callback_query(F.data == "adm_reports")
async def cb_adm_r(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return
    
    await state.set_state(AdminState.report_hours)
    
    await c.message.edit_text(
        "📊 Выберите за какой период хотите получить отчет\n"
        "(до 120 часов)\n\n"
        "Введите количество часов:"
    )

@router.callback_query(F.data == "adm_cast")
async def cb_cast(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return
    
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 Пришлите пост для рассылки:")

@router.callback_query(F.data.startswith("helpreply_"))
async def cb_helpreply(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return
    
    uid = c.data.split("_")[1]
    await state.update_data(help_uid=uid)
    await state.set_state(AdminState.help_reply)
    
    await c.message.answer(f"✍️ Введите ответ для {uid}:")

# ==========================================
# FSM ХЭНДЛЕРЫ
# ==========================================
@router.message(UserState.waiting_numbers)
async def fsm_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = [clean_phone(x.strip()) for x in raw if clean_phone(x.strip())]
    
    if not valid:
        return await m.reply("❌ Не найдено валидных номеров")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, work_time, last_ping) VALUES (?, ?, ?, ?, ?, ?)",
                (m.from_user.id, ph, data['tariff'], data['price'], data['work_time'], get_now())
            )
        await db.commit()
    
    await state.clear()
    await m.answer(
        f"✅ Принято: {len(valid)} шт\n{SEP}\nДобавлено в очередь",
        reply_markup=main_kb(m.from_user.id)
    )

@router.message(UserState.waiting_help)
async def fsm_help(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    
    kb = InlineKeyboardBuilder().button(
        text="💬 Ответить",
        callback_data=f"helpreply_{m.from_user.id}"
    )
    
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🆘 Новый запрос\n{SEP}\n"
            f"От: {m.from_user.id} (@{m.from_user.username})\n\n"
            f"{m.text}",
            reply_markup=kb.as_markup()
        )
        await m.answer(
            "✅ Запрос отправлен\n"
            "Ответ будет направлен вам",
            reply_markup=main_kb(m.from_user.id)
        )
    except Exception as e:
        logger.error(f"Help error: {e}")
        await m.answer("❌ Ошибка отправки")

@router.message(AdminState.help_reply)
async def fsm_helpreply(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    
    try:
        await bot.send_message(
            data['help_uid'],
            f"👨‍💻 Ответ на ваш запрос:\n{SEP}\n{m.text}"
        )
        await m.answer("✅ Ответ отправлен")
    except:
        await m.answer("❌ Не доставлено")

@router.message(AdminState.waiting_broadcast)
async def fsm_cast(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    
    msg = await m.answer("⏳ Рассылка...")
    
    async with get_db() as db:
        users = await (await db.execute(
            "SELECT user_id FROM users WHERE is_approved=1"
        )).fetchall()
    
    success, fail = 0, 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            success += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            fail += 1
        except:
            fail += 1
    
    await msg.edit_text(
        f"📢 Рассылка завершена\n{SEP}\n"
        f"✅ Доставлено: {success}\n"
        f"❌ Ошибок: {fail}\n"
        f"📊 Всего: {len(users)}"
    )

@router.message(AdminState.edit_price)
async def fsm_ep(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_time)
    await m.answer(
        f"2️⃣ Введите ВРЕМЯ РАБОТЫ\n"
        f"Пример: 10:00-22:00 МСК, 24/7"
    )

@router.message(AdminState.edit_time)
async def fsm_et(m: Message, state: FSMContext):
    data = await state.get_data()
    
    async with get_db() as db:
        await db.execute(
            "UPDATE tariffs SET price=?, work_time=? WHERE name=?",
            (data['price'], m.text, data['target'])
        )
        await db.commit()
    
    await state.clear()
    await m.answer(
        f"✅ Тариф обновлен!\n{SEP}\n"
        f"💰 {data['price']}\n"
        f"⏰ {m.text}"
    )

@router.message(AdminState.report_hours)
async def fsm_rep(m: Message, state: FSMContext):
    await state.clear()
    
    try:
        hours = int(m.text)
        if hours < 1 or hours > 120:
            return await m.answer("❌ Введите число от 1 до 120")
    except:
        return await m.answer("❌ Введите корректное число")
    
    cut_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT n.*, g.title as group_name 
            FROM numbers n 
            LEFT JOIN groups g ON n.worker_chat_id = g.chat_id
            WHERE n.created_at >= ? 
            ORDER BY n.id DESC
        """, (cut_time,))).fetchall()
    
    if not rows:
        return await m.answer("📂 Пусто")
    
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'UserID', 'Phone', 'Status', 'Group', 'Tariff', 'Created', 'Start', 'End', 'Duration'])
    
    for r in rows:
        duration = calc_duration(r['start_time'], r['end_time'])
        gn = r['group_name'] if r['group_name'] else "-"
        w.writerow([
            r['id'], r['user_id'], r['phone'], r['status'], gn,
            r['tariff_name'], format_time(r['created_at']),
            format_time(r['start_time']), format_time(r['end_time']), duration
        ])
    
    out.seek(0)
    await m.answer_document(
        BufferedInputFile(out.getvalue().encode(), filename=f"report_{hours}h.csv"),
        caption=f"📊 Отчет за {hours}ч"
    )

# ==========================================
# ОБРАБОТКА ФОТО
# ==========================================
@router.message(F.photo & F.caption)
async def handle_photo(m: Message, bot: Bot):
    if "/sms" not in m.caption.lower():
        return
    
    ph = clean_phone(m.caption.split()[1]) if len(m.caption.split()) > 1 else None
    if not ph:
        return await m.reply("⚠️ Формат: /sms +7...")
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')",
            (ph,)
        )).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Не ваш номер")
    
    try:
        await bot.send_photo(
            row['user_id'],
            m.photo[-1].file_id,
            caption=f"🔔 СООБЩЕНИЕ ОТ ОФИСА\n{SEP}"
        )
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except:
        await m.reply("❌ Ошибка")

# ==========================================
# ОБЩИЙ ХЭНДЛЕР (ПОСЛЕДНИЙ!)
# ==========================================
@router.message(F.chat.type == "private")
async def handle_msg(m: Message, bot: Bot, state: FSMContext):
    if m.text and m.text.startswith('/'):
        return
    
    if m.from_user.id == ADMIN_ID:
        return
    
    cs = await state.get_state()
    if cs:
        return
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')",
            (m.from_user.id,)
        )).fetchone()
    
    if row and row['worker_chat_id']:
        async with get_db() as db:
            await db.execute(
                "UPDATE numbers SET wait_code_start=NULL WHERE id=?",
                (row['id'],)
            )
            await db.commit()
        
        try:
            tc = row['worker_chat_id']
            tt = row['worker_thread_id'] if row['worker_thread_id'] else None
            hdr = f"📩 ОТВЕТ ЮЗЕРА\n📱 {row['phone']}\n{SEP}\n"
            
            if m.text:
                await bot.send_message(
                    tc,
                    message_thread_id=tt,
                    text=f"{hdr}💬 {m.text}"
                )
            elif m.photo:
                await bot.send_photo(
                    tc,
                    message_thread_id=tt,
                    photo=m.photo[-1].file_id,
                    caption=f"{hdr}📸"
                )
            
            await m.answer("✅ Отправлено")
        except:
            await m.answer("❌ Ошибка")

# ==========================================
# МОНИТОРИНГ
# ==========================================
async def monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            
            async with get_db() as db:
                # Таймаут кода
                waiters = await (await db.execute("""
                    SELECT id, user_id, phone, worker_chat_id, worker_thread_id, wait_code_start 
                    FROM numbers 
                    WHERE status='active' AND wait_code_start IS NOT NULL
                """)).fetchall()
                
                for w in waiters:
                    st = datetime.fromisoformat(w['wait_code_start'])
                    if (now - st).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute(
                            "UPDATE numbers SET status='dead', end_time=?, wait_code_start=NULL WHERE id=?",
                            (get_now(), w['id'])
                        )
                        
                        try:
                            await bot.send_message(
                                w['user_id'],
                                f"⏰ Время вышло\n{w['phone']} отменен"
                            )
                            
                            if w['worker_chat_id']:
                                await bot.send_message(
                                    chat_id=w['worker_chat_id'],
                                    message_thread_id=w['worker_thread_id'] if w['worker_thread_id'] else None,
                                    text="⚠️ Таймаут кода!"
                                )
                        except:
                            pass
                
                # AFK проверка
                qrows = await (await db.execute("""
                    SELECT id, user_id, created_at, last_ping 
                    FROM numbers 
                    WHERE status='queue'
                """)).fetchall()
                
                for r in qrows:
                    las = r['last_ping'] if r['last_ping'] else r['created_at']
                    
                    if str(las).startswith("PENDING_"):
                        pt = datetime.fromisoformat(las.split("_")[1])
                        if (now - pt).total_seconds() / 60 >= AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                            try:
                                await bot.send_message(
                                    r['user_id'],
                                    "❌ Номер удален (AFK)"
                                )
                            except:
                                pass
                    else:
                        la = datetime.fromisoformat(las)
                        if (now - la).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            kb = InlineKeyboardBuilder().button(
                                text="👋 Я тут!",
                                callback_data=f"afk_ok_{r['id']}"
                            ).as_markup()
                            
                            try:
                                await bot.send_message(
                                    r['user_id'],
                                    f"⚠️ Проверка активности!\n{SEP}\nНажмите кнопку",
                                    reply_markup=kb
                                )
                                await db.execute(
                                    "UPDATE numbers SET last_ping=? WHERE id=?",
                                    (f"PENDING_{get_now()}", r['id'])
                                )
                            except:
                                await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                
                await db.commit()
        except Exception as e:
            logger.error(f"Monitor: {e}")
            await asyncio.sleep(5)

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    await init_db()
    
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    
    asyncio.create_task(monitor(bot))
    
    logger.info("🚀 BOT STARTED - FINAL VERSION")
    
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
