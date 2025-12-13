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

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "bot_v8_final.db"

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
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone TEXT,
            method TEXT, 
            status TEXT, 
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        
        # Миграция
        try: await db.execute("ALTER TABLE numbers ADD COLUMN method TEXT")
        except: pass
            
        await db.commit()

# --- КЛАВИАТУРЫ ---

async def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="📊 Очередь", callback_data="check_queue")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"),
         InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔧 Админ панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def tariff_select_kb():
    # IMG_2246 (низ)
    kb = [[InlineKeyboardButton(text="Холд (30+ мин -> $9)", callback_data="method_select")],
          [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def method_select_kb():
    # IMG_2246 (верх)
    kb = [[InlineKeyboardButton(text="✅ Обычный код", callback_data="input_sms"), 
           InlineKeyboardButton(text="QR-код", callback_data="input_qr")],
          [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])

def back_to_main_kb():
    # IMG_2248
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад в главное меню", callback_data="nav_main")]])

def profile_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Мои отчеты", callback_data="my_reports")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_main")]
    ])

# Воркер: Этап 1
def worker_stage1_kb(num_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"w_active_{num_id}"),
         InlineKeyboardButton(text="❌ Ошибка", callback_data=f"w_error_{num_id}")]
    ])

# Воркер: Этап 2 (Только Слет)
def worker_stage2_kb(num_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📉 Слет", callback_data=f"w_dead_{num_id}")]
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🧹 Очистить очередь", callback_data="admin_clear_queue")],
        [InlineKeyboardButton(text="✖️ Закрыть", callback_data="admin_close")]
    ])

# --- ЛОГИКА ПОЛЬЗОВАТЕЛЯ ---

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

@router.callback_query(F.data == "menu_guide")
async def show_guide(callback: CallbackQuery):
    # Текст как на IMG_2248
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

@router.callback_query(F.data == "check_queue")
async def check_queue(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE status = 'queue'") as c:
            q_len = (await c.fetchone())[0]
        async with db.execute("SELECT id FROM numbers WHERE user_id = ? AND status = 'queue'", (callback.from_user.id,)) as c:
            user_nums = await c.fetchall()

    text = f"📊 **Состояние очереди**\n\nВсего номеров: **{q_len}**\n"
    if user_nums:
        text += f"Ваших: **{len(user_nums)}**"
    else:
        text += "Ваших нет."
    await callback.answer(text, show_alert=True)

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
        await callback.answer("Отчетов нет.", show_alert=True)
        return

    report_text = "📄 **Последние 5 номеров:**\n\n"
    for row in rows:
        phone, start_str, end_str, status = row
        # finished = Слет (Успех), dead = Ошибка
        status_text = "📉 Слет" if status == 'finished' else "❌ Ошибка"
        
        duration = "—"
        if start_str and end_str:
            try:
                s = datetime.fromisoformat(start_str)
                e = datetime.fromisoformat(end_str)
                diff = e - s
                hours, remainder = divmod(diff.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                duration = f"{hours}ч {minutes}мин"
            except: pass
            
        report_text += f"📱 `{phone}`\n⏱ {duration}\nСтатус: {status_text}\n\n"

    await callback.message.answer(report_text, parse_mode="Markdown")
    await callback.answer()

# --- СДАЧА НОМЕРА ---
@router.callback_query(F.data == "select_tariff")
async def step_tariff(callback: CallbackQuery):
    await callback.message.edit_text("💰 **Выберите тарифный план:**", reply_markup=tariff_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "method_select")
async def step_method(callback: CallbackQuery):
    # IMG_2246 (текст)
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

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(callback: CallbackQuery, state: FSMContext):
    method = 'sms' if callback.data == "input_sms" else 'qr'
    await state.update_data(method=method)

    # Проверка на активный номер
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('work', 'active')", (callback.from_user.id,)) as c:
             if await c.fetchone():
                 await callback.answer("У вас уже есть номер в работе!", show_alert=True)
                 return
    
    # IMG_2267 (ввод номера)
    await callback.message.edit_text(
        "✏️ **Введите номер(а):**\n\n"
        "Ожидаю ввод в формате `+77...`",
        reply_markup=cancel_kb(),
        parse_mode="Markdown"
    )
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    method = data.get('method', 'sms')
    
    text = message.text.strip()
    raw_phones = [p.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "") for p in text.split(',')]
    kz_phone_pattern = re.compile(r"^\+77\d{9}$")
    valid_phones = []
    
    async with aiosqlite.connect(DB_NAME) as db:
        for p in raw_phones:
            if kz_phone_pattern.match(p):
                async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (p,)) as c:
                    if not await c.fetchone():
                        valid_phones.append(p)

    if not valid_phones:
        await message.answer("❌ Ошибка! Только КЗ (+77) и без повторов.", reply_markup=cancel_kb())
        return

    async with aiosqlite.connect(DB_NAME) as db:
        for phone in valid_phones:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, method, status) VALUES (?, ?, ?, ?)", 
                (message.from_user.id, phone, method, 'queue')
            )
        await db.commit()

    type_icon = "QR-код" if method == 'qr' else "✉️ SMS"
    # IMG_2267 (Успешно)
    await message.answer(
        f"✅ **Успешно!**\n"
        f"📥 Принято номеров: **{len(valid_phones)}**\n"
        f"📌 Тип: **{type_icon}**\n\n"
        "🔔 **Ожидайте уведомления.**",
        reply_markup=await main_menu_kb(message.from_user.id), parse_mode="Markdown"
    )
    await state.clear()

# --- ВОРКЕР ПАНЕЛЬ ---

@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
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
            
        # ТУТОРИАЛ (ИСПРАВЛЕННЫЙ)
        tutorial = (
            "✅ **Чат привязан!**\n\n"
            "👨‍💻 **Гайд по использованию:**\n\n"
            "1️⃣ Пиши `/num` -> Получишь номер.\n\n"
            "2️⃣ Вбей номер в WhatsApp Web.\n\n"
            "3️⃣ **Если просят QR:** Сфоткай QR с экрана.\n"
            "   Скинь фото сюда и подпиши: `/sms +77... Сканируй`\n\n"
            "4️⃣ **Если просят Код (по номеру):** Сфоткай код с экрана.\n"
            "   Скинь фото сюда и подпиши: `/sms +77... Вводи этот код`\n\n"
            "5️⃣ Когда зашел -> жми **✅ Встал**.\n"
            "6️⃣ Когда номер слетел -> жми **📉 Слет**."
        )
        await message.answer(tutorial, parse_mode="Markdown")

@router.message(Command("num"))
async def worker_get_num(message: types.Message, bot: Bot):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            res = await c.fetchone()
            if not res or str(message.chat.id) != res[0]: return

        async with db.execute("SELECT id, user_id, phone, method FROM numbers WHERE status = 'queue' ORDER BY id ASC LIMIT 1") as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await message.answer("📭 **Очередь пуста.**")
            return

        row_id, user_id, phone, method = row
        method_str = "QR-КОД" if method == 'qr' else "Обычный код"
        
        await db.execute("UPDATE numbers SET status = 'work', start_time = ? WHERE id = ?", (datetime.now().isoformat(), row_id))
        await db.commit()

    work_message = await message.answer(
        f"🔧 **Новая заявка**\n"
        f"📱 `{phone}`\n"
        f"📌 Тип: **{method_str}**\n"
        f"🆔 User: `{user_id}`\n\n"
        f"📸 **Скинь ЮЗЕРУ код/QR:**\n"
        f"Фото + `/sms {phone} текст`",
        parse_mode="Markdown",
        reply_markup=worker_stage1_kb(row_id)
    )
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (work_message.message_id, row_id))
        await db.commit()
    
    try:
        await bot.send_message(user_id, f"⚡️ Ваш номер `{phone}` принят в работу! Ожидайте код.", parse_mode="Markdown")
    except: pass

# --- ОБРАБОТКА ФОТО ОТ ВОРКЕРА ---

@router.message(Command("sms"))
async def worker_sms_text(message: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return
    try: phone, text = command.args.split(' ', 1)
    except: return
    await process_worker_response(message, bot, phone, text, is_photo=False)

@router.message(F.photo & F.caption.startswith("/sms"))
async def worker_sms_photo(message: types.Message, bot: Bot):
    try:
        args_raw = message.caption[4:].strip() 
        phone, text = args_raw.split(' ', 1)
    except:
        await message.reply("⚠️ Формат: Фото + подпись `/sms +77... Текст`", parse_mode="Markdown")
        return
    await process_worker_response(message, bot, phone, text, is_photo=True)

async def process_worker_response(message, bot, phone, text, is_photo):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, method FROM numbers WHERE phone = ? AND status IN ('work', 'active')", (phone,)) as c:
            row = await c.fetchone()
            
    if row:
        user_id, method = row
        try:
            # Текст для пользователя
            caption_text = (
                f"🔔 **ВНИМАНИЕ!**\n"
                f"📱 Номер: `{phone}`\n"
                f"💬 Сообщение: **{text}**\n\n"
                f"👇 **Используйте этот код/QR!**"
            )
            
            if is_photo:
                photo_id = message.photo[-1].file_id
                await bot.send_photo(user_id, photo=photo_id, caption=caption_text, parse_mode="Markdown")
            else:
                await bot.send_message(user_id, caption_text, parse_mode="Markdown")
                
            await message.react([types.ReactionTypeEmoji(emoji="👍")])
        except Exception as e:
            await message.reply(f"❌ Не доставлено: {e}")
    else:
        await message.reply(f"❌ Номер `{phone}` не в работе.")

@router.message(F.reply_to_message)
async def forward_reply(message: types.Message, bot: Bot):
    if message.chat.type != 'private': return
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('work', 'active') LIMIT 1", (message.from_user.id,)) as c:
            num = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            chat_res = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_thread_id'") as c:
            thread_res = await c.fetchone()

    if num and chat_res:
        chat_id, thread_id = int(chat_res[0]), int(thread_res[0]) if thread_res else None
        await bot.send_message(chat_id, f"📩 **ОТВЕТ ЮЗЕРА**\n📱 `{num[0]}`", message_thread_id=thread_id, parse_mode="Markdown")
        await message.forward(chat_id, message_thread_id=thread_id)
        await message.answer("✅ Отправлено воркеру.")

# --- КНОПКИ ВОРКЕРА ---
@router.callback_query(F.data.startswith("w_"))
async def worker_action(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split('_')
    action, num_id = parts[1], parts[2]
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, start_time, user_id FROM numbers WHERE id = ?", (num_id,)) as c:
            row = await c.fetchone()
        if not row: return
        
        phone, start_str, user_id = row

        if action == "active":
            await db.execute("UPDATE numbers SET status = 'active' WHERE id = ?", (num_id,))
            await db.commit()
            
            await callback.message.edit_text(
                f"🟢 **АКТИВЕН (ВСТАЛ)**\n📱 `{phone}`\n⏳ Таймер идет...",
                reply_markup=worker_stage2_kb(num_id),
                parse_mode="Markdown"
            )
            try: await bot.send_message(user_id, f"✅ Номер `{phone}` успешно встал! Не закрывайте сессию.", parse_mode="Markdown")
            except: pass
            
        elif action == "error":
            await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (datetime.now().isoformat(), num_id))
            await db.commit()
            await callback.message.edit_text(f"❌ **ОШИБКА**\n📱 `{phone}`", reply_markup=None, parse_mode="Markdown")
            try: await bot.send_message(user_id, f"❌ Номер `{phone}` не подошел.", parse_mode="Markdown")
            except: pass
            
        elif action == "dead":
            # СЛЕТ
            await db.execute("UPDATE numbers SET status = 'finished', end_time = ? WHERE id = ?", (datetime.now().isoformat(), num_id))
            await db.commit()
            
            start_dt = datetime.fromisoformat(start_str)
            diff = datetime.now() - start_dt
            hours, remainder = divmod(diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            # Просто время без текста
            duration_str = f"{hours}ч {minutes}мин"
            
            await callback.message.edit_text(
                f"📉 **СЛЕТ**\n"
                f"📱 `{phone}`\n"
                f"⏱ **{duration_str}**\n"
                f"👤 Воркер: {callback.from_user.first_name}",
                parse_mode="Markdown"
            )
            try: await bot.send_message(user_id, f"📉 Номер `{phone}` завершил работу (Слет).\n⏱ {duration_str}", parse_mode="Markdown")
            except: pass

    await callback.answer()

# --- АДМИН ПАНЕЛЬ ---
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
    print("Бот v8.0 (Final Cut) запускается...")
    if not TOKEN or not ADMIN_ID:
        print("❌ ОШИБКА: Заполни BOT_TOKEN и ADMIN_ID")
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
