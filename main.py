import asyncio
import logging
import sys
import os
import re
from datetime import datetime, date
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramForbiddenError

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "bot_database_exact.db"

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- СОСТОЯНИЯ (FSM) ---
class UserState(StatesGroup):
    waiting_for_number = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()

router = Router()

# --- БАЗА ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Пользователи
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Номера
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone TEXT,
            status TEXT, 
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Конфиг (для привязки чата воркеров)
        await db.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await db.commit()

# --- КЛАВИАТУРЫ (Точно как на скринах) ---

async def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"),
         InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]
    ]
    # Кнопка админки (видна только админу)
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔧 Админ панель", callback_data="admin_panel_start")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb)

def tariff_select_kb():
    # Как на скриншоте IMG_2246 (внизу)
    kb = [
        [InlineKeyboardButton(text="Холд (30+ мин -> $9)", callback_data="method_select")],
        [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def method_select_kb():
    # Как на скриншоте IMG_2246 (вверху/середина)
    kb = [
        [InlineKeyboardButton(text="✅ Обычный код", callback_data="input_sms"), 
         InlineKeyboardButton(text="QR-код", callback_data="input_qr")],
        [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])

def back_to_main_kb():
    # Как на скриншоте IMG_2248
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад в главное меню", callback_data="nav_main")]])

def profile_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Мои отчеты", callback_data="my_reports")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_main")]
    ])

def worker_control_kb(num_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💀 Слет", callback_data=f"w_dead_{num_id}"),
         InlineKeyboardButton(text="💰 Выплата", callback_data=f"w_finish_{num_id}")]
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🧹 Очистить очередь", callback_data="admin_clear_queue")],
        [InlineKeyboardButton(text="✖️ Закрыть", callback_data="admin_close")]
    ])

# --- ЛОГИКА ---

@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username", 
            (user.id, user.username, user.first_name)
        )
        await db.commit()
    
    # Текст как на IMG_2245
    text = (
        "🤖 **Бот для приема номеров**\n\n"
        "💎 Доступные тарифные планы:\n"
        "• Холд: 30+ мин -> $9\n\n"
        "🗓 График работы:\n"
        "• 09:00-20:00 (МСК)\n\n"
        "📞 Для сдачи номера нажмите кнопку ниже\n"
        "‼️ **ОТВЯЗ — НЕ ВЫПЛАТА** ‼️"
    )
    
    await message.answer(text, parse_mode="Markdown", reply_markup=await main_menu_kb(user.id))

@router.callback_query(F.data == "nav_main")
async def nav_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    # Тот же текст для кнопки "Главное меню"
    text = (
        "🤖 **Бот для приема номеров**\n\n"
        "💎 Доступные тарифные планы:\n"
        "• Холд: 30+ мин -> $9\n\n"
        "🗓 График работы:\n"
        "• 09:00-20:00 (МСК)\n\n"
        "📞 Для сдачи номера нажмите кнопку ниже\n"
        "‼️ **ОТВЯЗ — НЕ ВЫПЛАТА** ‼️"
    )
    await callback.message.edit_text(text, reply_markup=await main_menu_kb(callback.from_user.id), parse_mode="Markdown")

# --- РАЗДЕЛ ПОМОЩЬ (IMG_2248) ---
@router.callback_query(F.data == "menu_guide")
async def show_guide(callback: CallbackQuery):
    text = (
        "📖 **Как сдать свой номер:**\n\n"
        "1) Нажми \"📥 Сдать номер\".\n\n"
        "2) Отправь свой номер в ответ на сообщение.\n\n"
        "3) Ждёте своей очереди и ждёте код, в виде фото\n\n"
        "4) Вписываете код в WhatsApp (Три точки вверху > Связанные устройства > "
        "Связать по коду/номеру > И туда пишите код который вам дали) и ваш номер встаёт.\n\n"
        "5) Ждёте слёта и выплаты под конец дня, если ваши номера отстояли"
    )
    await callback.message.edit_text(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")

# --- ПРОФИЛЬ ---
@router.callback_query(F.data == "menu_profile")
async def show_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        today_start = datetime.combine(date.today(), datetime.min.time()).isoformat()
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND created_at >= ?", (user_id, today_start)) as cursor:
            today_count = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ?", (user_id,)) as cursor:
            total_count = (await cursor.fetchone())[0]

    text = (
        "👤 **Профиль**\n\n"
        f"🆔 Ваш ID: `{user_id}`\n"
        f"👤 Имя: {callback.from_user.first_name}\n\n"
        f"🗓 Сдал сегодня: {today_count}\n"
        f"📦 Сдал всего: {total_count}"
    )
    await callback.message.edit_text(text, reply_markup=profile_kb(), parse_mode="Markdown")

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

    await callback.message.answer(report_text, parse_mode="Markdown")
    await callback.answer()

# --- СДАЧА НОМЕРА ---
@router.callback_query(F.data == "select_tariff")
async def step_tariff(callback: CallbackQuery):
    # IMG_2246 (нижняя часть - выбор тарифа)
    await callback.message.edit_text(
        "💰 **Выберите тарифный план:**",
        reply_markup=tariff_select_kb(),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "method_select")
async def step_method(callback: CallbackQuery):
    # IMG_2246 (верхняя часть - текст про Холд и выбор кода)
    text = (
        "✅ Выбран тариф: Холд\n\n"
        "📝 Чтобы сдать номер(а) — отправь их одним сообщением.\n"
        "Пример: `+77001234567`\n"
        "Или несколько: `+77001234567, +77001234568`\n\n"
        "🔗 Способ привязки: Обычный код\n"
        "Выбери нужный вариант кнопками ниже.\n\n"
        "‼️ **Берем только Казахстанские номера** 🇰🇿"
    )
    await callback.message.edit_text(text, reply_markup=method_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "input_qr")
async def input_qr_stub(callback: CallbackQuery):
    await callback.answer("QR-код временно недоступен. Выберите Обычный код.", show_alert=True)

@router.callback_query(F.data == "input_sms")
async def step_input(callback: CallbackQuery, state: FSMContext):
    # Проверка на активный номер
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status = 'work'", (callback.from_user.id,)) as c:
             if await c.fetchone():
                 await callback.answer("У вас уже есть номер в работе! Дождитесь завершения.", show_alert=True)
                 return

    await callback.message.edit_text(
        "✏️ **Введите номер(а):**\n\n"
        "Ожидаю ввод в формате `+77...`",
        reply_markup=cancel_kb(),
        parse_mode="Markdown"
    )
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    text = message.text.strip()
    raw_phones = [p.strip().replace(" ", "").replace("-", "") for p in text.split(',')]
    
    kz_phone_pattern = re.compile(r"^\+77\d{9}$")
    valid_phones = []
    
    async with aiosqlite.connect(DB_NAME) as db:
        for p in raw_phones:
            if kz_phone_pattern.match(p):
                # Проверка на дубли
                async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work')", (p,)) as c:
                    if not await c.fetchone():
                        valid_phones.append(p)

    if not valid_phones:
        await message.answer(
            "❌ **Ошибка!** Номера должны быть Казахстанскими (+77...) и не дублироваться.\nПопробуйте еще раз.",
            reply_markup=cancel_kb(), parse_mode="Markdown"
        )
        return

    async with aiosqlite.connect(DB_NAME) as db:
        for phone in valid_phones:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, status) VALUES (?, ?, ?)", 
                (message.from_user.id, phone, 'queue')
            )
        await db.commit()

    await message.answer(
        f"✅ **Принято заявок: {len(valid_phones)}**\n"
        "Ожидайте очереди. Когда бот запросит код, вам придет уведомление.",
        reply_markup=await main_menu_kb(message.from_user.id), parse_mode="Markdown"
    )
    await state.clear()

# --- ВОРКЕР ПАНЕЛЬ (IT ОТДЕЛ) ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    # Привязать чат может только Админ
    if message.from_user.id != ADMIN_ID: return
    
    if message.chat.type in ['group', 'supergroup']:
        chat_id = message.chat.id
        thread_id = message.message_thread_id if message.is_topic_message else None
        
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_chat_id', ?)", (str(chat_id),))
            if thread_id:
                await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_thread_id', ?)", (str(thread_id),))
            else:
                await db.execute("DELETE FROM config WHERE key='work_thread_id'")
            await db.commit()
        await message.answer("✅ Чат привязан как рабочий.")

@router.message(Command("num"))
async def worker_get_num(message: types.Message, bot: Bot):
    # Проверка: работает только в привязанном чате
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            res = await c.fetchone()
            if not res or str(message.chat.id) != res[0]:
                return # Игнорим команду не в том чате

        async with db.execute("SELECT id, user_id, phone FROM numbers WHERE status = 'queue' ORDER BY id ASC LIMIT 1") as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await message.answer("📭 Очередь пуста.")
            return

        row_id, user_id, phone = row
        start_time = datetime.now().isoformat()
        await db.execute("UPDATE numbers SET status = 'work', start_time = ? WHERE id = ?", (start_time, row_id))
        await db.commit()

    work_message = await message.answer(
        f"🔧 **В Работе**\n📱 `{phone}`\n🆔 User: `{user_id}`\n\nКоманды: `/sms {phone} Текст`",
        parse_mode="Markdown",
        reply_markup=worker_control_kb(row_id)
    )
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (work_message.message_id, row_id))
        await db.commit()
    
    try:
        await bot.send_message(user_id, f"⚡️ Твой номер `{phone}` взят в работу! Будь готов дать код.", parse_mode="Markdown")
    except: pass

@router.message(Command("sms"))
async def worker_sms(message: types.Message, command: CommandObject, bot: Bot):
    # Упрощено: работает в любом чате (или можно добавить проверку ID чата), но главное - берет активный номер
    if not command.args: return
    try:
        phone, text = command.args.split(' ', 1)
    except: return
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM numbers WHERE phone = ? AND status = 'work'", (phone,)) as c:
            row = await c.fetchone()
            
    if row:
        try:
            await bot.send_message(row[0], f"🔔 **КОД!**\nДля номера: `{phone}`\n📝: **{text}**\n\n👇 Ответь кодом/фото!", parse_mode="Markdown")
            await message.react([types.ReactionTypeEmoji(emoji="👍")])
        except: await message.react([types.ReactionTypeEmoji(emoji="swearing_face")])

@router.message(F.reply_to_message)
async def forward_reply(message: types.Message, bot: Bot):
    if message.chat.type != 'private': return
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status = 'work' LIMIT 1", (message.from_user.id,)) as c:
            num = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            chat_res = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_thread_id'") as c:
            thread_res = await c.fetchone()

    if num and chat_res:
        chat_id, thread_id = int(chat_res[0]), int(thread_res[0]) if thread_res else None
        await bot.send_message(chat_id, f"📩 **ОТВЕТ ЮЗЕРА**\n📱 `{num[0]}`", message_thread_id=thread_id, parse_mode="Markdown")
        await message.forward(chat_id, message_thread_id=thread_id)
        await message.answer("✅ Передано.")

@router.callback_query(F.data.startswith("w_"))
async def worker_action(callback: CallbackQuery, bot: Bot):
    action, num_id = callback.data.split('_')[1], callback.data.split('_')[2]
    status = 'finished' if action == 'finish' else 'dead'
    res_text = "✅ ВЫПЛАТА" if action == 'finish' else "💀 СЛЕТ"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, start_time, user_id FROM numbers WHERE id = ?", (num_id,)) as c:
            row = await c.fetchone()
        if not row: return
        
        await db.execute("UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", (status, datetime.now().isoformat(), num_id))
        await db.commit()
        
    start_dt = datetime.fromisoformat(row[1])
    duration = str(datetime.now() - start_dt).split('.')[0]
    
    await callback.message.edit_text(f"🏁 **{res_text}**\n📱 `{row[0]}`\n⏱ {duration}\n👤 {callback.from_user.first_name}", parse_mode="Markdown")
    try: await bot.send_message(row[2], f"Статус номера `{row[0]}`: **{res_text}**", parse_mode="Markdown")
    except: pass
    await callback.answer()

# --- АДМИН ПАНЕЛЬ (ТОЛЬКО ДЛЯ ВЛАДЕЛЬЦА) ---
@router.callback_query(F.data == "admin_panel_start")
async def admin_start(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.message.edit_text("🔧 **Админ панель**", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "admin_clear_queue")
async def admin_clear(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM numbers WHERE status = 'queue'")
        await db.commit()
    await callback.answer("Очередь очищена!", show_alert=True)

@router.callback_query(F.data == "admin_broadcast")
async def admin_br_step1(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return
    await callback.message.answer("✍️ Пришли сообщение для рассылки:")
    await state.set_state(AdminState.waiting_for_broadcast)
    await callback.answer()

@router.message(AdminState.waiting_for_broadcast)
async def admin_br_step2(message: types.Message, state: FSMContext, bot: Bot):
    if message.from_user.id != ADMIN_ID: return
    count = 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            async for row in cursor:
                try:
                    await message.copy_to(row[0])
                    count += 1
                    await asyncio.sleep(0.05)
                except: pass
    await message.answer(f"✅ Разослано: {count}")
    await state.clear()

@router.callback_query(F.data == "admin_close")
async def admin_close(callback: CallbackQuery):
    await callback.message.delete()

# --- ЗАПУСК ---
async def main():
    print("Бот запускается...")
    if not TOKEN or not ADMIN_ID:
        print("Ошибка: Нет TOKEN или ADMIN_ID")
        return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    if sys.platform == "win32": asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
