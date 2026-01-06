"""
WhatsApp Number Management Bot
Fixed version with all features working
"""

import asyncio
import logging
import sys
import os
import re
import csv
import io
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    ReactionTypeEmoji, BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ===== CONFIG =====
# Вставь токен и ID админа
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "whatsapp_bot.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

router = Router()

# ===== DATABASE =====
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
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                phone TEXT NOT NULL,
                tariff_name TEXT NOT NULL,
                tariff_price TEXT NOT NULL,
                tariff_hold TEXT NOT NULL,
                status TEXT DEFAULT 'queue',
                worker_id INTEGER DEFAULT 0,
                start_time TEXT,
                end_time TEXT,
                code_received TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY,
                price TEXT NOT NULL,
                hold TEXT NOT NULL
            )
        """)
        
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50', '1h')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '150', '2h')")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        await db.commit()
    logger.info("✅ Database initialized")

# ===== HELPERS =====
def clean_phone(phone: str):
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11:
        return '+' + clean
    elif clean.startswith('8') and len(clean) == 11:
        clean = '7' + clean[1:]
    elif len(clean) == 10:
        clean = '7' + clean
    
    if re.match(r'^7\d{10}$', clean):
        return '+' + clean
    return None

def mask_phone(phone: str, user_id: int):
    if user_id == ADMIN_ID:
        return phone
    try:
        return f"{phone[:5]}***{phone[-4:]}"
    except:
        return phone

def get_now():
    return datetime.now().isoformat()

def format_time(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%H:%M %d.%m")
    except:
        return iso_str

def calc_duration(start_iso, end_iso):
    try:
        start = datetime.fromisoformat(start_iso)
        end = datetime.fromisoformat(end_iso)
        diff = end - start
        minutes = int(diff.total_seconds() / 60)
        if minutes < 60:
            return f"{minutes} мин"
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours}ч {mins}мин"
    except:
        return "?"

async def get_user_status(user_id: int):
    async with get_db() as db:
        async with db.execute(
            "SELECT is_approved, is_banned FROM users WHERE user_id=?",
            (user_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return False, False
    return bool(row['is_approved']), bool(row['is_banned'])

# ===== FSM STATES =====
class UserState(StatesGroup):
    waiting_numbers = State()

class SupportState(StatesGroup):
    waiting_question = State()
    waiting_answer = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    waiting_price = State()
    waiting_hold = State()

# ===== KEYBOARDS =====
def main_kb(user_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Сдать номер", callback_data="select_tariff")
    builder.button(text="👤 Профиль", callback_data="menu_profile")
    builder.button(text="ℹ️ Помощь", callback_data="menu_guide")
    builder.button(text="🆘 Задать вопрос", callback_data="support_ask")
    
    if user_id == ADMIN_ID:
        builder.button(text="⚡ Админ", callback_data="admin_panel")
    
    builder.adjust(1, 2, 1, 1)
    return builder.as_markup()

def worker_kb(num_id: int, tariff: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Встал", callback_data=f"w_act_{num_id}")
    
    if "MAX" in tariff.upper():
        builder.button(text="⏭ Пропуск", callback_data=f"w_skip_{num_id}")
    else:
        builder.button(text="❌ Ошибка", callback_data=f"w_err_{num_id}")
    
    builder.adjust(2)
    return builder.as_markup()

def worker_active_kb(num_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="📉 Слет", callback_data=f"w_drop_{num_id}")
    return builder.as_markup()

# ===== COMMANDS =====
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    async with get_db() as db:
        async with db.execute(
            "SELECT is_approved, is_banned FROM users WHERE user_id=?",
            (user_id,)
        ) as cur:
            user_row = await cur.fetchone()
        
        if not user_row:
            await db.execute(
                "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user_id, message.from_user.username, message.from_user.first_name)
            )
            await db.commit()
            
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Одобрить", callback_data=f"acc_ok_{user_id}"),
                    InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{user_id}")
                ]])
                
                try:
                    await message.bot.send_message(
                        ADMIN_ID,
                        f"👤 <b>Новый пользователь</b>\n\n"
                        f"ID: <code>{user_id}</code>\n"
                        f"Username: @{message.from_user.username or 'None'}\n"
                        f"Имя: {message.from_user.first_name}",
                        reply_markup=kb,
                        parse_mode="HTML"
                    )
                except: pass
            
            return await message.answer(
                "🔒 Спасибо за регистрацию!\n\nОжидайте одобрения администратора.",
                parse_mode="HTML"
            )
        
        is_approved = bool(user_row['is_approved'])
        is_banned = bool(user_row['is_banned'])
        
        if is_banned:
            return await message.answer("🚫 Ваш доступ заблокирован")
        
        if is_approved:
            await message.answer(
                f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\nВыберите действие:",
                reply_markup=main_kb(user_id),
                parse_mode="HTML"
            )
        else:
            await message.answer("⏳ Ожидайте одобрения администратора")

@router.message(Command("num"))
async def cmd_num(message: Message, bot: Bot):
    chat_id = message.chat.id
    thread_id = message.message_thread_id if message.is_topic_message else 0
    worker_id = message.from_user.id
    
    async with get_db() as db:
        async with db.execute(
            "SELECT value FROM config WHERE key=?",
            (f"topic_{chat_id}_{thread_id}",)
        ) as cur:
            config_row = await cur.fetchone()
        
        if not config_row:
            return await message.reply("❌ Топик не настроен. /startwork")
        
        tariff_name = config_row['value']
        
        async with db.execute(
            """SELECT id, phone, tariff_price, tariff_hold, user_id
               FROM numbers
               WHERE status='queue' AND tariff_name=?
               ORDER BY id ASC LIMIT 1""",
            (tariff_name,)
        ) as cur:
            num_row = await cur.fetchone()
        
        if not num_row:
            return await message.reply("📭 Очередь пуста")
        
        num_id, phone, price, hold, user_id = num_row
        
        await db.execute(
            "UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?",
            (worker_id, get_now(), num_id)
        )
        await db.commit()
    
    await message.answer(
        f"📱 Вы взяли номер <code>{phone}</code>\n\n"
        f"💰 Цена: {price}\n"
        f"⏳ Холд: {hold}\n\n"
        f"Ожидайте код.",
        reply_markup=worker_kb(num_id, tariff_name),
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(
            user_id,
            f"⚡ Ваш номер взяли в работу!\n\n"
            f"📱 {mask_phone(phone, user_id)}\n"
            f"⏳ Ожидайте запрос кода",
            parse_mode="HTML"
        )
    except:
        pass

@router.message(Command("code"))
async def cmd_code(message: Message, bot: Bot):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply("⚠️ Формат: /code +7999...")
    
    phone = clean_phone(args[1].split()[0])
    if not phone:
        return await message.reply("❌ Неверный номер")
    
    async with get_db() as db:
        async with db.execute(
            "SELECT user_id, id, worker_id FROM numbers WHERE phone=? AND status IN ('work', 'active')",
            (phone,)
        ) as cur:
            num_row = await cur.fetchone()
    
    if not num_row:
        return await message.reply("❌ Номер не найден")
    
    if num_row['worker_id'] != message.from_user.id:
        return await message.reply("❌ Это не ваш номер!")
    
    try:
        await bot.send_message(
            num_row['user_id'],
            f"🔔 <b>Офис запросил код!</b>\n\n"
            f"📱 Номер: {mask_phone(phone, num_row['user_id'])}\n\n"
            f"👇 Ответьте на это сообщение кодом",
            parse_mode="HTML"
        )
        await message.reply("✅ Запрос отправлен")
    except:
        await message.reply("❌ Не удалось отправить")

@router.message(Command("startwork"))
async def cmd_startwork(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    async with get_db() as db:
        async with db.execute("SELECT name FROM tariffs") as cur:
            tariffs = await cur.fetchall()
    
    builder = InlineKeyboardBuilder()
    for t in tariffs:
        builder.button(text=t['name'], callback_data=f"bind_{t['name']}")
    builder.adjust(2)
    
    await message.answer(
        "⚙️ Выберите тариф для топика:",
        reply_markup=builder.as_markup()
    )

@router.message(Command("stopwork"))
async def cmd_stopwork(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    chat_id = message.chat.id
    thread_id = message.message_thread_id if message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute(
            "DELETE FROM config WHERE key=?",
            (f"topic_{chat_id}_{thread_id}",)
        )
        await db.commit()
    
    await message.reply("🛑 Топик отвязан")

# ===== PHOTO/SMS HANDLER =====
@router.message(F.photo)
async def handle_photo(message: Message, bot: Bot):
    if not message.caption or "/sms" not in message.caption.lower():
        return
    
    parts = message.caption.strip().split()
    try:
        cmd_idx = next(i for i, p in enumerate(parts) if p.lower().startswith("/sms"))
        phone_raw = parts[cmd_idx + 1]
        code_text = " ".join(parts[cmd_idx + 2:]) if len(parts) > cmd_idx + 2 else "Код на фото"
    except:
        return await message.reply("⚠️ Формат: /sms +7999... текст")
    
    phone = clean_phone(phone_raw)
    if not phone:
        return await message.reply("❌ Неверный номер")
    
    async with get_db() as db:
        async with db.execute(
            "SELECT user_id, id FROM numbers WHERE phone=? AND status IN ('work', 'active')",
            (phone,)
        ) as cur:
            num_row = await cur.fetchone()
    
    if not num_row:
        return await message.reply("❌ Номер не в работе")
    
    try:
        await bot.send_photo(
            num_row['user_id'],
            message.photo[-1].file_id,
            caption=f"🔔 <b>SMS / Код</b>\n\n📱 {phone}\n💬 {code_text}",
            parse_mode="HTML"
        )
        
        async with get_db() as db:
            await db.execute(
                "UPDATE numbers SET code_received=? WHERE id=?",
                (code_text, num_row['id'])
            )
            await db.commit()
        
        await message.react([ReactionTypeEmoji(emoji="🔥")])
    except:
        await message.reply("❌ Ошибка отправки")

# ===== FSM HANDLERS =====
@router.message(UserState.waiting_numbers)
async def process_numbers(message: Message, state: FSMContext):
    data = await state.get_data()
    
    raw_numbers = message.text.split(',')
    valid = []
    
    for num in raw_numbers[:10]:
        cleaned = clean_phone(num.strip())
        if cleaned:
            valid.append(cleaned)
    
    if not valid:
        return await message.answer("❌ Не найдено валидных номеров")
    
    async with get_db() as db:
        placeholders = ','.join('?' * len(valid))
        async with db.execute(
            f"SELECT phone FROM numbers WHERE phone IN ({placeholders}) AND status NOT IN ('dead', 'finished')",
            valid
        ) as cur:
            existing = [r['phone'] for r in await cur.fetchall()]
    
    if existing:
        return await message.answer(f"❌ Номера уже в системе:\n{', '.join(existing)}")
    
    async with get_db() as db:
        for phone in valid:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_hold) VALUES (?, ?, ?, ?, ?)",
                (message.from_user.id, phone, data['tariff'], data['price'], data['hold'])
            )
        await db.commit()
    
    await state.clear()
    await message.answer(
        f"✅ Принято номеров: {len(valid)}\n\n"
        f"💰 Тариф: {data['tariff']}\n"
        f"💵 Цена: {data['price']}\n"
        f"⏳ Холд: {data['hold']}\n\n"
        f"Номера в очереди.",
        reply_markup=main_kb(message.from_user.id),
        parse_mode="HTML"
    )

@router.message(SupportState.waiting_question)
async def support_question(message: Message, state: FSMContext, bot: Bot):
    if message.text in ["/start", "Отмена"]:
        await state.clear()
        return await message.answer("Отменено", reply_markup=main_kb(message.from_user.id))
    
    if not ADMIN_ID:
        await state.clear()
        return await message.answer("❌ Поддержка недоступна")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="↩️ Ответить", callback_data=f"reply_{message.from_user.id}")
    
    try:
        await bot.send_message(
            ADMIN_ID,
            f"📩 <b>Вопрос от пользователя</b>\n\n"
            f"👤 ID: <code>{message.from_user.id}</code>\n"
            f"Username: @{message.from_user.username or 'None'}\n\n"
            f"{message.text}",
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
        
        await message.answer(
            "✅ Ваше сообщение отправлено!\n\nАдминистратор ответит в ближайшее время.",
            reply_markup=main_kb(message.from_user.id)
        )
    except:
        await message.answer("❌ Не удалось отправить")
    
    await state.clear()

@router.message(SupportState.waiting_answer)
async def support_answer(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    target_uid = data.get('target_uid')
    
    if not target_uid:
        await state.clear()
        return await message.answer("❌ Ошибка")
    
    try:
        await bot.send_message(
            target_uid,
            f"👨‍💻 <b>Ответ от поддержки:</b>\n\n{message.text}",
            parse_mode="HTML"
        )
        await message.answer("✅ Ответ отправлен")
    except:
        await message.answer("❌ Не удалось отправить")
    
    await state.clear()

@router.message(AdminState.waiting_broadcast)
async def admin_broadcast(message: Message, state: FSMContext, bot: Bot):
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM users WHERE is_approved=1") as cur:
            users = await cur.fetchall()
    
    success = 0
    for user in users:
        try:
            await bot.copy_message(
                chat_id=user['user_id'],
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            success += 1
            await asyncio.sleep(0.05)
        except:
            pass
    
    await message.answer(f"✅ Рассылка завершена!\n\nОтправлено: {success}/{len(users)}")
    await state.clear()

@router.message(AdminState.waiting_price)
async def admin_set_price(message: Message, state: FSMContext):
    data = await state.get_data()
    tariff = data.get('tariff')
    
    if not message.text.isdigit():
        return await message.answer("❌ Цена должна быть числом!")
    
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=? WHERE name=?", (message.text, tariff))
        await db.commit()
    
    await state.update_data(price=message.text)
    await state.set_state(AdminState.waiting_hold)
    await message.answer(
        f"✅ Цена установлена: {message.text}\n\n"
        f"Теперь введите ХОЛД (например: 1h, 30m, 2h):"
    )

@router.message(AdminState.waiting_hold)
async def admin_set_hold(message: Message, state: FSMContext):
    data = await state.get_data()
    tariff = data.get('tariff')
    
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET hold=? WHERE name=?", (message.text, tariff))
        await db.commit()
    
    await message.answer(
        f"✅ <b>Тариф {tariff} обновлен!</b>\n\n"
        f"💰 Цена: {data['price']}\n"
        f"⏳ Холд: {message.text}",
        parse_mode="HTML"
    )
    await state.clear()

# ===== TEXT ROUTER (LOWEST PRIORITY) =====
@router.message(F.text)
async def text_router(message: Message, bot: Bot):
    user_id = message.from_user.id
    
    # Check for MAX code response
    async with get_db() as db:
        async with db.execute(
            """SELECT id, worker_id, phone FROM numbers
               WHERE user_id=? AND status IN ('work', 'active') AND tariff_name LIKE '%MAX%'""",
            (user_id,)
        ) as cur:
            max_order = await cur.fetchone()
    
    if max_order and max_order['worker_id'] != 0:
        try:
            await bot.send_message(
                max_order['worker_id'],
                f"🔔 <b>Код от пользователя (MAX)</b>\n\n"
                f"📱 {max_order['phone']}\n"
                f"💬 <tg-spoiler>{message.text}</tg-spoiler>",
                parse_mode="HTML"
            )
            await message.react([ReactionTypeEmoji(emoji="👍")])
        except:
            pass
        return
    
    if message.chat.type == "private":
        await message.answer(
            "❓ Неизвестная команда.\n\nИспользуйте меню:",
            reply_markup=main_kb(user_id)
        )

# ===== CALLBACKS =====
@router.callback_query(F.data.startswith("acc_"))
async def handle_access(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("❌ Только для админа")
    
    action, target_id = callback.data.split('_')[1], int(callback.data.split('_')[2])
    
    async with get_db() as db:
        if action == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (target_id,))
            await db.commit()
            
            try:
                await bot.send_message(
                    target_id,
                    "✅ Доступ предоставлен!\n\nНажмите /start"
                )
            except:
                pass
            
            await callback.message.edit_text(f"✅ Пользователь {target_id} одобрен")
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (target_id,))
            await db.commit()
            
            try:
                await bot.send_message(target_id, "🚫 Доступ заблокирован")
            except:
                pass
            
            await callback.message.edit_text(f"🚫 Пользователь {target_id} заблокирован")
    
    await callback.answer()

@router.callback_query(F.data == "select_tariff")
async def select_tariff(callback: CallbackQuery):
    is_approved, is_banned = await get_user_status(callback.from_user.id)
    
    if is_banned or not is_approved:
        return await callback.answer("❌ Нет доступа", show_alert=True)
    
    async with get_db() as db:
        async with db.execute("SELECT name, price FROM tariffs") as cur:
            tariffs = await cur.fetchall()
    
    builder = InlineKeyboardBuilder()
    for t in tariffs:
        builder.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    builder.button(text="🔙 Назад", callback_data="nav_main")
    builder.adjust(1)
    
    await callback.message.edit_text(
        "💰 Выберите тариф:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@router.callback_query(F.data.startswith("pick_"))
async def pick_tariff(callback: CallbackQuery, state: FSMContext):
    tariff = callback.data.split("_", 1)[1]
    
    async with get_db() as db:
        async with db.execute("SELECT price, hold FROM tariffs WHERE name=?", (tariff,)) as cur:
            t_row = await cur.fetchone()
    
    if not t_row:
        return await callback.answer("❌ Тариф не найден", show_alert=True)
    
    await state.update_data(tariff=tariff, price=t_row['price'], hold=t_row['hold'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Ввести номера", callback_data="input_nums")
    builder.button(text="🔙 Назад", callback_data="select_tariff")
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"💎 <b>Тариф: {tariff}</b>\n\n"
        f"💰 Цена: {t_row['price']}\n"
        f"⏳ Холд: {t_row['hold']}\n\n"
        f"Готовы ввести номера?",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data == "input_nums")
async def input_nums(callback: CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Отмена", callback_data="nav_main")
    
    await callback.message.edit_text(
        "📱 <b>Введите номера</b>\n\n"
        "Формат: +7999... или через запятую\n"
        "Максимум 10 номеров за раз",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(UserState.waiting_numbers)
    await callback.answer()

@router.callback_query(F.data == "menu_profile")
async def show_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    async with get_db() as db:
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (user_id,)) as cur:
            user_row = await cur.fetchone()
        
        async with db.execute("SELECT COUNT(*) as total FROM numbers WHERE user_id=?", (user_id,)) as cur:
            total = (await cur.fetchone())['total']
        
        async with db.execute("SELECT COUNT(*) as done FROM numbers WHERE user_id=? AND status='finished'", (user_id,)) as cur:
            done = (await cur.fetchone())['done']
        
        async with db.execute("SELECT COUNT(*) as queue FROM numbers WHERE user_id=? AND status='queue'", (user_id,)) as cur:
            queue = (await cur.fetchone())['queue']
        
        # Исправленный запрос с закрытыми скобками
        async with db.execute(
            "SELECT COUNT(*) as before FROM numbers WHERE status='queue' AND id < (SELECT MIN(id) FROM numbers WHERE user_id=? AND status='queue')",
            (user_id,)
        ) as cur:
            before_row = await cur.fetchone()
            before_count = before_row['before'] if before_row['before'] is not None else 0
    
    reg_date = format_time(user_row['reg_date']) if user_row else "-"
    
    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"📅 Регистрация: {reg_date}\n"
        f"📦 Всего номеров: {total}\n"
        f"✅ Выплачено: {done}\n\n"
        f"🕒 <b>В очереди:</b>\n"
        f"Ваших номеров: {queue}\n"
        f"Перед вами заявок: {before_count}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Мои номера (24ч)", callback_data="my_nums")
    builder.button(text="🔙 Назад", callback_data="nav_main")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "my_nums")
async def my_nums(callback: CallbackQuery):
    user_id = callback.from_user.id
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
    
    async with get_db() as db:
        async with db.execute(
            "SELECT phone, status, tariff_price, created_at FROM numbers WHERE user_id=? AND created_at >= ? ORDER BY id DESC",
            (user_id, cutoff)
        ) as cur:
            rows = await cur.fetchall()
            
    text = "📝 <b>Ваши номера (24ч):</b>\n\n"
    if not rows:
        text += "Пусто"
    
    for r in rows:
        icon = "🟢" if r['status'] == 'active' else "✅" if r['status'] == 'finished' else "🟡"
        text += f"{icon} {mask_phone(r['phone'], user_id)} | {r['tariff_price']} | {format_time(r['created_at'])}\n"
        
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Назад", callback_data="menu_profile")
    
    if len(text) > 4096:
        text = text[:4000] + "..."
        
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "menu_guide")
async def show_guide(callback: CallbackQuery):
    text = (
        "📲 <b>Что делает бот</b>\n"
        "Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
        "📦 <b>Требования к номерам</b>\n"
        "✔️ Активный и чистый номер\n"
        "✔️ Доступ к SMS\n"
        "❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
        "⏳ <b>Холд и выплаты</b>\n"
        "Холд — время проверки номера\n"
        "💰 Выплата производится после успешного завершения холда\n\n"
        "⚠️ <i>Отправляя номер, вы подтверждаете, что ознакомились с правилами</i>"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Назад", callback_data="nav_main")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "support_ask")
async def support_ask(callback: CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Отмена", callback_data="nav_main")
    
    await callback.message.edit_text(
        "📝 <b>Напишите ваш вопрос или проблему одним сообщением:</b>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await state.set_state(SupportState.waiting_question)
    await callback.answer()

@router.callback_query(F.data.startswith("reply_"))
async def admin_reply(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    target_uid = int(callback.data.split("_")[1])
    await state.update_data(target_uid=target_uid)
    await state.set_state(SupportState.waiting_answer)
    
    await callback.message.answer(f"✍️ Введите ответ для ID {target_uid}:")
    await callback.answer()

# ===== ADMIN CALLBACKS =====
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Тарифы", callback_data="adm_tariffs")
    builder.button(text="📦 Очередь (Файл)", callback_data="adm_queue_file")
    builder.button(text="📢 Рассылка", callback_data="adm_broadcast")
    builder.button(text="🔙 Меню", callback_data="nav_main")
    builder.adjust(1)
    
    await callback.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(callback: CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT name, price, hold FROM tariffs") as cur:
            rows = await cur.fetchall()
    
    text = "📋 <b>Тарифы:</b>\n"
    builder = InlineKeyboardBuilder()
    
    for r in rows:
        text += f"🔹 {r['name']}: {r['price']} | {r['hold']}\n"
        builder.button(text=f"✏️ {r['name']}", callback_data=f"edittrf_{r['name']}")
    
    builder.button(text="🔙 Назад", callback_data="admin_panel")
    builder.adjust(2, 1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data.startswith("edittrf_"))
async def edit_tariff(callback: CallbackQuery, state: FSMContext):
    tariff = callback.data.split("_", 1)[1]
    await state.update_data(tariff=tariff)
    await state.set_state(AdminState.waiting_price)
    
    await callback.message.edit_text(
        f"✏️ Редактируем: <b>{tariff}</b>\n\nВведите новую ЦЕНУ (число):",
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📢 Отправьте сообщение для рассылки (Текст/Фото):")
    await state.set_state(AdminState.waiting_broadcast)
    await callback.answer()

@router.callback_query(F.data == "adm_queue_file")
async def adm_queue_file(callback: CallbackQuery):
    async with get_db() as db:
        async with db.execute(
            "SELECT id, phone, tariff_name, created_at FROM numbers WHERE status='queue' ORDER BY id ASC"
        ) as cur:
            rows = await cur.fetchall()
    
    if not rows:
        return await callback.answer("Очередь пуста", show_alert=True)
    
    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(['ID', 'PHONE', 'TARIFF', 'DATE'])
    
    for r in rows:
        csv_writer.writerow([r['id'], r['phone'], r['tariff_name'], format_time(r['created_at'])])
    
    output.seek(0)
    doc = BufferedInputFile(output.getvalue().encode(), filename="queue.txt")
    
    await callback.message.answer_document(doc, caption=f"📦 В очереди: {len(rows)}")
    await callback.answer()

# ===== WORKER ACTIONS =====
@router.callback_query(F.data.startswith("bind_"))
async def bind_topic(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    tariff = callback.data.split("_", 1)[1]
    chat_id = callback.message.chat.id
    thread_id = callback.message.message_thread_id if callback.message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (f"topic_{chat_id}_{thread_id}", tariff)
        )
        await db.commit()
    
    await callback.message.edit_text(
        f"✅ Чат привязан! Тариф: {tariff}\n\n"
        "👨‍💻 <b>Гайд:</b>\n"
        "1. /num - взять номер\n"
        "2. /sms номер текст - отправить код\n"
        "3. Кнопки под сообщением для статуса",
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("w_act_"))
async def w_act(callback: CallbackQuery, bot: Bot):
    num_id = int(callback.data.split("_")[2])
    
    async with get_db() as db:
        async with db.execute("SELECT worker_id, phone, user_id FROM numbers WHERE id=?", (num_id,)) as cur:
            row = await cur.fetchone()
            
    if not row or row['worker_id'] != callback.from_user.id:
        return await callback.answer("❌ Ошибка доступа", show_alert=True)
        
    async with get_db() as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (num_id,))
        await db.commit()
        
    await callback.message.edit_text(
        f"номер встал и все", 
        reply_markup=worker_active_kb(num_id),
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(row['user_id'], "✅ Номер встал и все")
    except: pass
    await callback.answer()

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(callback: CallbackQuery, bot: Bot):
    num_id = int(callback.data.split("_")[2])
    
    async with get_db() as db:
        async with db.execute("SELECT worker_id, user_id FROM numbers WHERE id=?", (num_id,)) as cur:
            row = await cur.fetchone()
            
    if not row or row['worker_id'] != callback.from_user.id:
        return await callback.answer("❌ Ошибка доступа")
        
    async with get_db() as db:
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (num_id,))
        await db.commit()
        
    await callback.message.edit_text("⏭ Пропуск (номер вернулся в очередь)")
    
    try:
        await bot.send_message(row['user_id'], "⚠️ Офис пропустил ваш номер, он вернулся в очередь.")
    except: pass
    await callback.answer()

@router.callback_query(F.data.startswith(("w_drop_", "w_err_")))
async def w_finish(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split("_")
    action = parts[1]
    num_id = int(parts[2])
    
    async with get_db() as db:
        async with db.execute("SELECT worker_id, phone, user_id, start_time FROM numbers WHERE id=?", (num_id,)) as cur:
            row = await cur.fetchone()
            
    if not row or row['worker_id'] != callback.from_user.id:
        return await callback.answer("❌ Ошибка доступа")
        
    status = "finished" if action == "drop" else "dead"
    end_time = get_now()
    duration = calc_duration(row['start_time'], end_time)
    
    async with get_db() as db:
        await db.execute(
            "UPDATE numbers SET status=?, end_time=? WHERE id=?",
            (status, end_time, num_id)
        )
        await db.commit()
        
    if action == "drop":
        msg_worker = f"📉 Номер слетел. Время: {duration}"
        msg_user = f"📉 ваш номер слетел и его время работы: {duration}"
    else:
        msg_worker = "❌ Ошибка"
        msg_user = "❌ Ошибка"
        
    await callback.message.edit_text(msg_worker)
    try:
        await bot.send_message(row['user_id'], msg_user)
    except: pass
    await callback.answer()

@router.callback_query(F.data == "nav_main")
async def nav_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Привет, <b>{c.from_user.first_name}</b>!", reply_markup=main_kb(c.from_user.id), parse_mode="HTML")

# --- MAIN LOOP ---
async def main():
    await init_db()
    
    storage = MemoryStorage()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
