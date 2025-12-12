import asyncio
import logging
import sys
import os
from datetime import datetime, date
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("BOT_TOKEN")
# Получаем ID админов и чистим от пробелов
raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(x) for x in raw_admins.split(",") if x.strip().isdigit()]

DB_NAME = "bot_database.db"

# --- СОСТОЯНИЯ (FSM) ---
class UserState(StatesGroup):
    waiting_for_number = State() # Ждем номер от юзера
    waiting_for_code = State()   # Ждем код (если нужно)

class AdminState(StatesGroup):
    waiting_for_broadcast = State()

router = Router()

# --- БАЗА ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Таблица пользователей
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Таблица номеров
        # status: 'queue' (очередь), 'work' (в работе), 'finished' (выплата), 'dead' (слет)
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone TEXT,
            status TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Таблица конфига (для воркеров)
        await db.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await db.commit()

# --- КЛАВИАТУРЫ (INLINE) ---

def main_menu_kb():
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="menu_send_number")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile")],
        [InlineKeyboardButton(text="📖 Как сдать номер", callback_data="menu_guide")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def cancel_kb():
    kb = [[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def profile_kb():
    kb = [
        [InlineKeyboardButton(text="📄 Мои отчеты", callback_data="my_reports")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_to_main_kb():
    kb = [[InlineKeyboardButton(text="⬅️ Назад в главное меню", callback_data="nav_main")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def tariff_kb():
    # Как на скриншоте
    kb = [
        [InlineKeyboardButton(text="✅ Обычный код", callback_data="tariff_sms"), 
         InlineKeyboardButton(text="QR-код", callback_data="tariff_qr")],
        [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def admin_kb():
    kb = [
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🧹 Очистить очередь", callback_data="admin_clear_queue")],
        [InlineKeyboardButton(text="✖️ Закрыть", callback_data="admin_close")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- ЛОГИКА ПОЛЬЗОВАТЕЛЯ ---

@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users (user_id, username, first_name) VALUES (?, ?, ?)", 
            (user.id, user.username, user.first_name)
        )
        await db.commit()
    
    await message.answer(
        "👋 **Добро пожаловать в Scarface Team!**\n\n"
        "Здесь ты можешь сдать свой номер и получить выплату.\n"
        "Выбери действие ниже:",
        parse_mode="Markdown",
        reply_markup=main_menu_kb()
    )

# Обработка кнопки "Назад" или "Отмена" (возврат в меню)
@router.callback_query(F.data == "nav_main")
async def nav_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "👋 **Главное меню**\nВыберите действие:",
        reply_markup=main_menu_kb(),
        parse_mode="Markdown"
    )

# --- РАЗДЕЛ ПРОФИЛЬ (КАК НА СКРИНЕ) ---
@router.callback_query(F.data == "menu_profile")
async def show_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    first_name = callback.from_user.first_name
    username = f"@{callback.from_user.username}" if callback.from_user.username else "Не указан"

    async with aiosqlite.connect(DB_NAME) as db:
        # Всего сдал (finished + dead) или только finished? Обычно считают успешные.
        # Пусть считается все, что не в очереди и не в работе.
        
        # Считаем за сегодня
        async with db.execute("""
            SELECT COUNT(*) FROM numbers 
            WHERE user_id = ? AND created_at >= date('now', 'start of day')
        """, (user_id,)) as cursor:
            today_count = (await cursor.fetchone())[0]

        # Считаем всего
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ?", (user_id,)) as cursor:
            total_count = (await cursor.fetchone())[0]

    text = (
        "👤 Профиль\n\n"
        f"🎫 Имя: {first_name}\n"
        f"📎 Логин: {username}\n"
        f"🗓 Сегодня сдал: {today_count}\n"
        f"📦 Всего сдал: {total_count}"
    )
    
    await callback.message.edit_text(text, reply_markup=profile_kb())

@router.callback_query(F.data == "my_reports")
async def show_reports(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT phone, start_time, end_time, status 
            FROM numbers 
            WHERE user_id = ? AND status IN ('finished', 'dead')
            ORDER BY id DESC LIMIT 5
        """, (user_id,)) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        await callback.answer("Отчетов пока нет", show_alert=True)
        return

    report_text = "📄 **Последние 5 номеров:**\n\n"
    for row in rows:
        phone, start_str, end_str, status = row
        status_icon = "✅ Выплата" if status == 'finished' else "💀 Слет"
        
        duration = "—"
        if start_str and end_str:
            try:
                s = datetime.fromisoformat(start_str)
                e = datetime.fromisoformat(end_str)
                duration = str(e - s).split('.')[0]
            except: pass
            
        report_text += f"📱 `{phone}`\n⏳ {duration} | {status_icon}\n\n"

    # Отправляем новым сообщением, чтобы не ломать структуру меню
    await callback.message.answer(report_text, parse_mode="Markdown")
    await callback.answer()

# --- РАЗДЕЛ ИНСТРУКЦИЯ (КАК НА СКРИНЕ) ---
@router.callback_query(F.data == "menu_guide")
async def show_guide(callback: CallbackQuery):
    text = (
        "📖 **Как сдать свой номер:**\n\n"
        "1) Нажми \"📥 Сдать номер\".\n\n"
        "2) Выбери \"Обычный код\".\n\n"
        "3) Отправь свой номер в ответ на сообщение.\n\n"
        "4) Ждёте своей очереди и ждёте код.\n\n"
        "5) Вписываете код в WhatsApp (Три точки вверху > Связанные устройства > "
        "Связать по коду/номеру > И туда пишите код который вам дали) и ваш номер встаёт.\n\n"
        "6) Ждёте слёта и выплаты под конец дня."
    )
    await callback.message.edit_text(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")

# --- СДАЧА НОМЕРА ---
@router.callback_query(F.data == "menu_send_number")
async def ask_tariff(callback: CallbackQuery):
    await callback.message.edit_text(
        "📎 Способ привязки: Обычный код\n"
        "Выбери нужный вариант кнопками ниже.\n\n"
        "‼️ Берем только Казахстанские номера 🇰🇿",
        reply_markup=tariff_kb()
    )

@router.callback_query(F.data == "tariff_sms")
async def ask_phone_input(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "✅ Выбран тариф: Холд (СМС)\n\n"
        "📝 **Чтобы сдать номер(а) — отправь их одним сообщением.**\n"
        "Пример: `+77001234567`\n"
        "Или несколько: `+77001234567, +77001234568`",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    text = message.text.strip()
    # Простая проверка на KZ номера
    if not text.startswith("+77"):
        await message.answer(
            "❌ **Ошибка!** Принимаются только номера Казахстана (+77...).\nПопробуй еще раз.",
            reply_markup=cancel_kb(), parse_mode="Markdown"
        )
        return

    # Разбиваем по запятым, если несколько
    phones = [p.strip() for p in text.split(',')]
    
    async with aiosqlite.connect(DB_NAME) as db:
        for phone in phones:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, status) VALUES (?, ?, ?)", 
                (message.from_user.id, phone, 'queue')
            )
        await db.commit()

    await message.answer(
        f"✅ **Принято номеров: {len(phones)}**\n"
        "Ожидайте очереди. Когда бот запросит код, вам придет уведомление.",
        reply_markup=main_menu_kb(), parse_mode="Markdown"
    )
    await state.clear()

# --- ВОРКЕР ПАНЕЛЬ (IT ОТДЕЛ) ---

@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    # Работает только в группах
    if message.chat.type not in ['group', 'supergroup']:
        return

    thread_id = message.message_thread_id if message.is_topic_message else None
    chat_id = message.chat.id

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_chat_id', ?)", (str(chat_id),))
        if thread_id:
            await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_thread_id', ?)", (str(thread_id),))
        await db.commit()

    await message.answer("✅ **Воркер-панель привязана к этому чату!**")

@router.message(Command("num"))
async def worker_get_num(message: types.Message, bot: Bot):
    async with aiosqlite.connect(DB_NAME) as db:
        # Берем из очереди (статус queue)
        async with db.execute("SELECT id, user_id, phone FROM numbers WHERE status = 'queue' ORDER BY id ASC LIMIT 1") as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await message.answer("📭 Очередь пуста.")
            return

        row_id, user_id, phone = row
        start_time = datetime.now().isoformat()
        
        # Ставим статус work
        await db.execute("UPDATE numbers SET status = 'work', start_time = ? WHERE id = ?", (start_time, row_id))
        await db.commit()

    # Кнопки управления номером для воркера
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💀 Слет", callback_data=f"w_dead_{row_id}"),
         InlineKeyboardButton(text="💰 Выплата", callback_data=f"w_finish_{row_id}")]
    ])
    
    await message.answer(
        f"🔧 **В Работе**\n📱 `{phone}`\n🆔 User: `{user_id}`\n\nКоманды:\n`/sms {phone} Текст`",
        parse_mode="Markdown",
        reply_markup=kb
    )
    
    # Уведомляем юзера
    try:
        await bot.send_message(user_id, f"⚡️ Твой номер `{phone}` взят в работу! Будь готов дать код.", parse_mode="Markdown")
    except: pass

@router.message(Command("sms"))
async def worker_sms(message: types.Message, command: CommandObject, bot: Bot):
    if not command.args:
        await message.answer("Формат: `/sms +77... Текст`")
        return
    
    args = command.args.split(' ', 1)
    if len(args) < 2:
        await message.answer("Не указан текст.")
        return
        
    phone, text = args[0], args[1]
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM numbers WHERE phone = ? AND status = 'work'", (phone,)) as cursor:
            row = await cursor.fetchone()
            
    if not row:
        await message.answer("❌ Номер не в работе.")
        return
        
    user_id = row[0]
    try:
        await bot.send_message(
            user_id, 
            f"🔔 **КОД!**\nДля номера: `{phone}`\n\n📝 Сообщение: **{text}**\n\n👇 Ответь на это сообщение кодом или фото!",
            parse_mode="Markdown"
        )
        await message.answer("📨 Запрос отправлен.")
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {e}")

# Пересылка ответа юзера воркеру
@router.message(F.reply_to_message)
async def forward_reply(message: types.Message, bot: Bot):
    if message.chat.type != 'private': return # Только ЛС с ботом
    
    # Ищем, какой воркер чат активен
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            chat_res = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_thread_id'") as c:
            thread_res = await c.fetchone()
            
    if chat_res:
        chat_id = int(chat_res[0])
        thread_id = int(thread_res[0]) if thread_res else None
        
        await bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=f"📩 **ОТВЕТ ОТ ЮЗЕРА**\nID: {message.from_user.id}\nТекст: {message.text or 'Фото/Медиа'}"
        )
        if message.photo:
            await bot.send_photo(chat_id=chat_id, message_thread_id=thread_id, photo=message.photo[-1].file_id)
        
        await message.answer("✅ Отправлено воркеру.")

# Обработка кнопок воркера (Слет/Выплата)
@router.callback_query(F.data.startswith("w_"))
async def worker_action(callback: CallbackQuery):
    action, num_id = callback.data.split('_')[1], callback.data.split('_')[2]
    status = 'finished' if action == 'finish' else 'dead'
    res_text = "✅ ВЫПЛАТА" if action == 'finish' else "💀 СЛЕТ"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", 
            (status, datetime.now().isoformat(), num_id)
        )
        await db.commit()
        
        # Получаем данные для отчета
        async with db.execute("SELECT phone, start_time FROM numbers WHERE id=?", (num_id,)) as c:
            row = await c.fetchone()
            
    if row:
        phone, start_str = row
        start = datetime.fromisoformat(start_str)
        duration = str(datetime.now() - start).split('.')[0]
        
        await callback.message.edit_text(
            f"🏁 **ЗАВЕРШЕНО**\n📱 `{phone}`\n⏱ {duration}\nИтог: {res_text}",
            parse_mode="Markdown"
        )

# --- АДМИН ПАНЕЛЬ ---
@router.message(Command("admin"))
async def open_admin(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        # Для отладки, если не работает
        print(f"Попытка входа в админку: {message.from_user.id} (Не в списке {ADMIN_IDS})") 
        return
    await message.answer("🔧 Админ панель:", reply_markup=admin_kb())

@router.callback_query(F.data == "admin_clear_queue")
async def admin_clear(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM numbers WHERE status = 'queue'")
        await db.commit()
    await callback.answer("Очередь очищена!", show_alert=True)

@router.callback_query(F.data == "admin_broadcast")
async def admin_br_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("✍️ Введи текст рассылки:")
    await state.set_state(AdminState.waiting_for_broadcast)
    await callback.answer()

@router.message(AdminState.waiting_for_broadcast)
async def admin_br_send(message: types.Message, state: FSMContext, bot: Bot):
    count = 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            async for row in cursor:
                try:
                    await bot.send_message(row[0], message.text)
                    count += 1
                    await asyncio.sleep(0.05)
                except: pass
    await message.answer(f"Разослано: {count}")
    await state.clear()

@router.callback_query(F.data == "admin_close")
async def admin_close(callback: CallbackQuery):
    await callback.message.delete()

# --- MAIN ---
async def main():
    print(f"Запуск бота...")
    print(f"Загруженные Admin IDs: {ADMIN_IDS}") # Чтобы ты проверил в логах

    if not TOKEN:
        print("ОШИБКА: Нет токена!")
        return

    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
