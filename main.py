import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile

# --- КОНФИГУРАЦИЯ ---
# Бот берет токен из переменных окружения хостинга
TOKEN = os.getenv("BOT_TOKEN") 
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id] # ID админов через запятую

# Имя файла базы данных
DB_NAME = "bot_database.db"

# --- СОСТОЯНИЯ (FSM) ---
class UserState(StatesGroup):
    waiting_for_number = State()
    waiting_for_code = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()

# --- ИНИЦИАЛИЗАЦИЯ ---
router = Router()

# --- БАЗА ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Таблица пользователей
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT
        )""")
        # Таблица номеров
        # status: 'queue' (в очереди), 'work' (в работе), 'dead' (слетел/выплата), 'finished' (отработал)
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone TEXT,
            status TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            worker_thread_id INTEGER
        )""")
        # Таблица конфигурации (какой чат рабочий)
        await db.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await db.commit()

# --- КЛАВИАТУРЫ ---
def main_kb():
    kb = [
        [KeyboardButton(text="📥 Сдать номер")],
        [KeyboardButton(text="👤 Профиль")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def admin_kb():
    kb = [
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🧹 Очистить очередь", callback_data="admin_clear_queue")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- ЛОГИКА ПОЛЬЗОВАТЕЛЯ ---

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", 
                         (message.from_user.id, message.from_user.username))
        await db.commit()
    
    await message.answer(
        "👋 **Добро пожаловать!**\n\n"
        "Здесь ты можешь сдать свой номер в аренду и получить выплату.\n"
        "Нажми **«📥 Сдать номер»**, чтобы начать.",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )

@router.message(F.text == "📥 Сдать номер")
async def ask_number(message: types.Message, state: FSMContext):
    await message.answer("Отправь свой номер телефона в формате: `+77001234567`\nОдним сообщением.", parse_mode="Markdown")
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    # Простейшая валидация
    if not phone.startswith("+") or not phone[1:].isdigit():
        await message.answer("❌ Неверный формат. Попробуй еще раз (пример: +77001234567).")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        # Добавляем в очередь
        await db.execute("INSERT INTO numbers (user_id, phone, status) VALUES (?, ?, ?)", 
                         (message.from_user.id, phone, 'queue'))
        await db.commit()

    await message.answer(
        "✅ **Номер принят в очередь!**\n"
        "Ожидай, когда он поступит в работу. Мы пришлем уведомление.",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )
    await state.clear()

@router.message(F.text == "👤 Профиль")
async def profile(message: types.Message):
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Считаем активные и завершенные
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND status = 'queue'", (user_id,)) as cursor:
            in_queue = await cursor.fetchone()
        
        # Кнопка отчетов
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Мои отчеты (История)", callback_data="my_reports")]
        ])
        
        await message.answer(
            f"👤 **Твой профиль:**\n\n"
            f"🆔 ID: `{user_id}`\n"
            f"⏳ В очереди: {in_queue[0]} номеров\n\n"
            f"Для просмотра истории и выплат нажми кнопку ниже.",
            parse_mode="Markdown",
            reply_markup=kb
        )

@router.callback_query(F.data == "my_reports")
async def show_reports(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        # Выбираем завершенные номера
        async with db.execute("""
            SELECT phone, start_time, end_time, status 
            FROM numbers 
            WHERE user_id = ? AND (status = 'finished' OR status = 'dead')
            ORDER BY id DESC LIMIT 10
        """, (user_id,)) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        await callback.message.answer("📭 У тебя пока нет отработанных номеров.")
        await callback.answer()
        return

    report_text = "📄 **Твои последние отчеты:**\n\n"
    for row in rows:
        phone, start_str, end_str, status = row
        
        if start_str and end_str:
            start = datetime.fromisoformat(start_str)
            end = datetime.fromisoformat(end_str)
            duration = end - start
            # Форматируем время (убираем микросекунды)
            duration_str = str(duration).split('.')[0]
        else:
            duration_str = "Не определено"

        status_icon = "✅ Выплата" if status == 'finished' else "💀 Слет"
        report_text += f"📱 `{phone}`\n⏱ Работал: {duration_str}\nСтатус: {status_icon}\n\n"

    await callback.message.answer(report_text, parse_mode="Markdown")
    await callback.answer()

# --- ЛОГИКА WORKER (IT ГРУППА) ---

@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    # Команда должна работать только в группах
    if message.chat.type not in ['group', 'supergroup']:
        await message.answer("Эту команду нужно писать в рабочей группе.")
        return

    thread_id = message.message_thread_id if message.is_topic_message else None
    chat_id = message.chat.id

    async with aiosqlite.connect(DB_NAME) as db:
        # Сохраняем ID чата и топика для работы
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_chat_id', ?)", (str(chat_id),))
        if thread_id:
            await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_thread_id', ?)", (str(thread_id),))
        await db.commit()

    instructions = (
        "💻 **Рабочая панель активирована в этом топике!**\n\n"
        "📜 **Команды:**\n"
        "`/num` — Взять номер из очереди.\n"
        "`/sms +7... Текст` — Запросить код у дропа.\n"
        "Кнопки управления появятся после взятия номера."
    )
    await message.answer(instructions, parse_mode="Markdown")

@router.message(Command("num"))
async def worker_get_num(message: types.Message, bot: Bot):
    # Проверка, что пишут из правильного чата/топика (можно добавить, если нужно строго)
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Берем самый старый номер из очереди
        async with db.execute("SELECT id, user_id, phone FROM numbers WHERE status = 'queue' ORDER BY id ASC LIMIT 1") as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await message.answer("📭 Очередь пуста.")
            return

        row_id, user_id, phone = row
        start_time = datetime.now().isoformat()
        
        # Обновляем статус на 'work'
        await db.execute("UPDATE numbers SET status = 'work', start_time = ? WHERE id = ?", (start_time, row_id))
        await db.commit()

    # Уведомляем воркера
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💀 Слет/Ошибка", callback_data=f"work_dead_{row_id}")],
        [InlineKeyboardButton(text="💰 Выплата/Конец", callback_data=f"work_finish_{row_id}")]
    ])
    
    await message.answer(
        f"🔧 **Взят номер в работу!**\n\n"
        f"📱 Номер: `{phone}`\n"
        f"👤 User ID: `{user_id}`\n\n"
        f"Используй `/sms {phone} Подпись` для запроса кода.",
        parse_mode="Markdown",
        reply_markup=kb
    )

    # Уведомляем пользователя
    try:
        await bot.send_message(user_id, f"⚡️ Твой номер `{phone}` взят в работу!\nБудь на связи, скоро придет код.", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"⚠️ Не удалось уведомить юзера {user_id}: {e}")

@router.message(Command("sms"))
async def worker_request_sms(message: types.Message, command: CommandObject, bot: Bot):
    if not command.args:
        await message.answer("Использование: `/sms +7999... Текст подписи`")
        return

    parts = command.args.split(' ', 1)
    if len(parts) < 2:
        await message.answer("Нужно указать и номер, и текст подписи.")
        return
    
    target_phone = parts[0]
    instruction_text = parts[1]

    async with aiosqlite.connect(DB_NAME) as db:
        # Ищем владельца номера, который СЕЙЧАС в работе
        async with db.execute("SELECT user_id FROM numbers WHERE phone = ? AND status = 'work'", (target_phone,)) as cursor:
            row = await cursor.fetchone()
    
    if not row:
        await message.answer(f"❌ Номер {target_phone} не найден в активной работе.")
        return

    user_id = row[0]

    # Отправляем запрос юзеру
    # Тут можно добавить запрос фото, но пока сделаем текст
    await bot.send_message(
        user_id,
        f"🔔 **ПРИШЕЛ КОД!**\n\n"
        f"Для номера: `{target_phone}`\n"
        f"📝 Подпись: *{instruction_text}*\n\n"
        f"👇 **СРОЧНО НАПИШИ КОД (или отправь фото) В ОТВЕТ НА ЭТО СООБЩЕНИЕ!**",
        parse_mode="Markdown"
    )
    
    # Можно установить стейт, чтобы ловить ответ
    # Но для простоты будем ловить любой текст от юзера, если у него есть активный номер
    await message.answer(f"📨 Запрос кода отправлен юзеру для `{target_phone}`.")

# Обработка ответа юзера с кодом
@router.message(F.reply_to_message)
async def forward_code_to_worker(message: types.Message, bot: Bot):
    # Если юзер просто отвечает на сообщение бота (скорее всего это код)
    # Проверим, есть ли у него активный номер
    if message.chat.type != 'private':
        return

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status = 'work'", (user_id,)) as cursor:
            row = await cursor.fetchone()
    
    if not row:
        return # Игнорим, если нет активного номера

    phone = row[0]
    
    # Получаем ID чата воркеров из конфига
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key = 'work_chat_id'") as cursor:
            chat_row = await cursor.fetchone()
        async with db.execute("SELECT value FROM config WHERE key = 'work_thread_id'") as cursor:
            thread_row = await cursor.fetchone()
            
    if chat_row:
        worker_chat_id = int(chat_row[0])
        worker_thread_id = int(thread_row[0]) if thread_row else None

        # Пересылаем сообщение воркерам
        await bot.send_message(
            chat_id=worker_chat_id,
            message_thread_id=worker_thread_id,
            text=f"📩 **КОД ОТ ЮЗЕРА!**\n📱 Номер: `{phone}`\n\nСообщение:",
            parse_mode="Markdown"
        )
        await message.forward(chat_id=worker_chat_id, message_thread_id=worker_thread_id)
        await message.answer("✅ Код передан специалистам.")

# Кнопки завершения работы (для воркеров)
@router.callback_query(F.data.startswith("work_"))
async def work_status_callback(callback: types.CallbackQuery):
    action, num_id = callback.data.split('_')[1], callback.data.split('_')[2]
    end_time = datetime.now().isoformat()
    
    new_status = 'finished' if action == 'finish' else 'dead'
    status_text = "✅ ВЫПЛАТА (Успех)" if action == 'finish' else "💀 СЛЕТ (Ошибка)"

    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем, что номер еще в работе
        async with db.execute("SELECT start_time, phone, user_id FROM numbers WHERE id = ?", (num_id,)) as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await callback.answer("Номер уже закрыт.")
            return
            
        start_str, phone, user_id = row
        start_dt = datetime.fromisoformat(start_str)
        duration = datetime.now() - start_dt
        duration_str = str(duration).split('.')[0]

        # Обновляем БД
        await db.execute("UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", (new_status, end_time, num_id))
        await db.commit()

    # Редактируем сообщение в чате воркеров
    await callback.message.edit_text(
        f"🏁 **Сессия завершена**\n"
        f"📱 Номер: `{phone}`\n"
        f"⏱ Работал: {duration_str}\n"
        f"Статус: {status_text}\n"
        f"Закрыл: {callback.from_user.full_name}"
    )
    await callback.answer()

# --- АДМИН ПАНЕЛЬ ---

@router.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("👑 **Админ панель**", reply_markup=admin_kb())

@router.callback_query(F.data == "admin_clear_queue")
async def admin_clear_queue(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM numbers WHERE status = 'queue'")
        await db.commit()
    
    await callback.answer("Очередь очищена!", show_alert=True)

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await callback.message.answer("✍️ Напиши текст для рассылки всем пользователям:")
    await state.set_state(AdminState.waiting_for_broadcast)
    await callback.answer()

@router.message(AdminState.waiting_for_broadcast)
async def admin_broadcast_send(message: types.Message, state: FSMContext, bot: Bot):
    text = message.text
    count = 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            async for row in cursor:
                try:
                    await bot.send_message(row[0], f"📢 **Рассылка:**\n\n{text}", parse_mode="Markdown")
                    count += 1
                    await asyncio.sleep(0.05) # Антиспам
                except:
                    pass
    
    await message.answer(f"✅ Рассылка завершена. Доставлено: {count} пользователям.")
    await state.clear()


# --- ЗАПУСК ---
async def main():
    if not TOKEN:
        print("Ошибка: Токен не найден в переменных окружения!")
        return

    await init_db()
    
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    
    # Удаляем вебхуки и запускаем поллинг
    await bot.delete_webhook(drop_pending_updates=True)
    print("Бот запущен...")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
