import asyncio
import logging
import sys
import os
import re
import json
from datetime import datetime, time, timedelta
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile, BufferedInputFile

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "fast_team_v10.db"

# Часовой пояс (МСК = UTC+3)
MSK_OFFSET = 3 

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

router = Router()

# --- СОСТОЯНИЯ (FSM) ---
class UserState(StatesGroup):
    waiting_for_number = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    setting_schedule_start = State()
    setting_schedule_end = State()
    adding_tariff_name = State()
    adding_tariff_price = State()

# --- БАЗА ДАННЫХ И НАСТРОЙКИ ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Добавили tariff_name и tariff_price для фиксации цены
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone TEXT,
            method TEXT, 
            tariff_name TEXT,
            tariff_price TEXT,
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
        
        # Дефолтные настройки, если их нет
        # Тарифы
        default_tariffs = {
            "ВЦ RU": "4$ Час",
            "MAX ФБХ": "3.5$ / 0 минут"
        }
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('tariffs', ?)", (json.dumps(default_tariffs),))
        
        # Время работы (07:00 - 17:30)
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_start', '07:00')")
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_end', '17:30')")
        
        await db.commit()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_msk_time():
    return datetime.utcnow() + timedelta(hours=MSK_OFFSET)

async def check_work_hours():
    """Проверяет, рабочее ли сейчас время по МСК"""
    now_msk = get_msk_time().time()
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_start'") as c:
            start_str = (await c.fetchone())[0]
        async with db.execute("SELECT value FROM config WHERE key='work_end'") as c:
            end_str = (await c.fetchone())[0]
            
    start_time = datetime.strptime(start_str, "%H:%M").time()
    end_time = datetime.strptime(end_str, "%H:%M").time()
    
    # Простая проверка (без перехода через полночь)
    if start_time <= end_time:
        return start_time <= now_msk <= end_time
    else:
        # Если смена через ночь (например 22:00 - 06:00)
        return start_time <= now_msk or now_msk <= end_time

def clean_phone(phone: str):
    """Очистка и валидация номера"""
    # Убираем все кроме цифр и плюса
    clean = re.sub(r'[^\d+]', '', phone)
    
    # Если начинается с 8 и длина 11 (РФ/КЗ формат 8705...), меняем 8 на +7
    if clean.startswith('8') and len(clean) == 11:
        clean = '+7' + clean[1:]
    # Если начинается с 7 и длина 11, добавляем +
    elif clean.startswith('7') and len(clean) == 11:
        clean = '+' + clean
    # Если просто куча цифр (10 шт), считаем что это +7...
    elif len(clean) == 10 and clean.isdigit():
        clean = '+7' + clean
        
    # Финальная проверка: должен начинаться с + и иметь от 10 до 15 цифр
    if not re.match(r'^\+\d{10,15}$', clean):
        return None
    return clean

# --- КЛАВИАТУРЫ ---

async def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"),
         InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔧 Админ панель (FAST TEAM)", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def tariffs_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c:
            tariffs = json.loads((await c.fetchone())[0])
            
    kb = []
    for name, price in tariffs.items():
        kb.append([InlineKeyboardButton(text=f"{name} ({price})", callback_data=f"trf_{name}")])
    
    kb.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def method_select_kb():
    kb = [[InlineKeyboardButton(text="✅ Обычный код", callback_data="input_sms"), 
           InlineKeyboardButton(text="📷 QR-код", callback_data="input_qr")],
          [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])

# Воркер: Главная кнопка "Взять номер"
def worker_take_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 ВЗЯТЬ ЗАЯВКУ", callback_data="worker_take_new")]
    ])

# Воркер: Активная работа
def worker_active_kb(num_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"w_active_{num_id}"),
         InlineKeyboardButton(text="❌ Ошибка/Спам", callback_data=f"w_error_{num_id}")]
    ])

# Воркер: Финал (Только Слет)
def worker_finish_kb(num_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📉 СЛЕТ", callback_data=f"w_dead_{num_id}")]
    ])

# Админка
def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать отчет (.txt)", callback_data="adm_report")],
        [InlineKeyboardButton(text="⏰ Изменить время работы", callback_data="adm_schedule")],
        [InlineKeyboardButton(text="💰 Редактор Тарифов", callback_data="adm_tariffs")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="⬅️ Выход", callback_data="admin_close")]
    ])

# --- ЮЗЕР САЙД ---

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
    
    await message.answer(
        "👋 **Добро пожаловать в FAST TEAM!**\n\n"
        "Мы скупаем номера по самым высоким ценам.\n"
        "Выберите действие в меню:",
        parse_mode="Markdown",
        reply_markup=await main_menu_kb(user.id)
    )

@router.callback_query(F.data == "nav_main")
async def nav_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🤖 **Главное меню FAST TEAM**", reply_markup=await main_menu_kb(callback.from_user.id), parse_mode="Markdown")

@router.callback_query(F.data == "select_tariff")
async def step_tariff(callback: CallbackQuery):
    # Проверка времени работы
    if not await check_work_hours():
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT value FROM config WHERE key='work_start'") as c: s = (await c.fetchone())[0]
            async with db.execute("SELECT value FROM config WHERE key='work_end'") as c: e = (await c.fetchone())[0]
        await callback.answer(f"💤 Мы сейчас не работаем.\nГрафик: {s} - {e} МСК", show_alert=True)
        return

    await callback.message.edit_text("💰 **Выберите тариф:**", reply_markup=await tariffs_kb(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_"))
async def step_method(callback: CallbackQuery, state: FSMContext):
    tariff_name = callback.data.split('_')[1]
    
    # Получаем цену из конфига для сохранения
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c:
            tariffs = json.loads((await c.fetchone())[0])
    
    price = tariffs.get(tariff_name, "Неизвестно")
    await state.update_data(tariff_name=tariff_name, tariff_price=price)

    await callback.message.edit_text(
        f"✅ Тариф: **{tariff_name}**\n\n"
        "Выберите способ передачи:",
        reply_markup=method_select_kb(), parse_mode="Markdown"
    )

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(callback: CallbackQuery, state: FSMContext):
    method = 'sms' if callback.data == "input_sms" else 'qr'
    await state.update_data(method=method)

    # Проверка на дурака (уже есть активный номер)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('queue', 'work', 'active')", (callback.from_user.id,)) as c:
             if await c.fetchone():
                 await callback.answer("🚫 У вас уже есть активная заявка!", show_alert=True)
                 return

    m_text = "✉️ SMS" if method == 'sms' else "📷 QR-код"
    await callback.message.edit_text(
        f"✏️ Тип: **{m_text}**\n\n"
        "Введите номер телефона:\n"
        "Можно без +7, бот сам исправит.",
        reply_markup=cancel_kb(),
        parse_mode="Markdown"
    )
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    method = data.get('method', 'sms')
    tariff_name = data.get('tariff_name')
    tariff_price = data.get('tariff_price')
    
    text = message.text.strip()
    phones_raw = text.split(',')
    valid_phones = []
    
    async with aiosqlite.connect(DB_NAME) as db:
        for p in phones_raw:
            cleaned = clean_phone(p)
            if cleaned:
                # Проверка дублей
                async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (cleaned,)) as c:
                    if not await c.fetchone():
                        valid_phones.append(cleaned)
    
    if not valid_phones:
        await message.answer("❌ **Ошибка!** Номер некорректен или уже в работе.\nПроверьте формат (минимум 10 цифр).", reply_markup=cancel_kb(), parse_mode="Markdown")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        for phone in valid_phones:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, status) VALUES (?, ?, ?, ?, ?, ?)", 
                (message.from_user.id, phone, method, tariff_name, tariff_price, 'queue')
            )
        await db.commit()

    await message.answer(
        f"✅ **Заявка принята!**\n"
        f"📱 Номер: `{valid_phones[0]}`\n"
        f"💰 Тариф: {tariff_name}\n\n"
        "🔔 Ожидайте уведомления от бота.",
        reply_markup=await main_menu_kb(message.from_user.id), parse_mode="Markdown"
    )
    await state.clear()

# --- ВОРКЕР ПАНЕЛЬ (БЕЗ КОМАНД, ТОЛЬКО КНОПКИ) ---

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
            
        await message.answer(
            "🚀 **Панель FAST TEAM активирована!**\n"
            "Нажмите кнопку ниже, чтобы взять заявку.",
            reply_markup=worker_take_kb()
        )

@router.callback_query(F.data == "worker_take_new")
async def worker_take_job(callback: CallbackQuery, bot: Bot):
    # Проверка чата
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            res = await c.fetchone()
            if not res or str(callback.message.chat.id) != res[0]: 
                await callback.answer("Чужой чат", show_alert=True)
                return

        # Берем из очереди
        async with db.execute("SELECT id, user_id, phone, method, tariff_name FROM numbers WHERE status = 'queue' ORDER BY id ASC LIMIT 1") as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await callback.answer("📭 Очередь пуста! Отдыхай.", show_alert=True)
            return

        row_id, user_id, phone, method, tariff = row
        method_str = "📸 QR-КОД" if method == 'qr' else "✉️ SMS-КОД"
        
        # Обновляем статус
        await db.execute("UPDATE numbers SET status = 'work', start_time = ? WHERE id = ?", (datetime.utcnow().isoformat(), row_id))
        await db.commit()

    # Меняем сообщение с кнопкой на панель работы
    text = (
        f"🔧 **В РАБОТЕ**\n"
        f"📱 `{phone}`\n"
        f"📌 Тип: **{method_str}** | Тариф: {tariff}\n"
        f"👇 **Копируй команду:**\n\n"
        f"`/sms {phone} текст`"
    )
    
    # Сохраняем ID сообщения чтобы потом редактировать
    work_msg = await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=worker_active_kb(row_id))
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (work_msg.message_id, row_id))
        await db.commit()

    # Уведомляем юзера
    try: await bot.send_message(user_id, f"⚡️ Ваш номер `{phone}` взят в работу! Ожидайте.", parse_mode="Markdown")
    except: pass

@router.callback_query(F.data.startswith("w_"))
async def worker_logic(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split('_')
    action, num_id = parts[1], parts[2]
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, start_time, user_id FROM numbers WHERE id = ?", (num_id,)) as c:
            row = await c.fetchone()
        if not row: 
            await callback.answer("Заявка не найдена.")
            return
        
        phone, start_str, user_id = row

        if action == "active":
            await db.execute("UPDATE numbers SET status = 'active' WHERE id = ?", (num_id,))
            await db.commit()
            await callback.message.edit_text(
                f"🟢 **АКТИВЕН**\n📱 `{phone}`\nНе закрывай сессию до слета!",
                reply_markup=worker_finish_kb(num_id),
                parse_mode="Markdown"
            )
            try: await bot.send_message(user_id, f"✅ Номер `{phone}` встал! Не закрывайте сессию.", parse_mode="Markdown")
            except: pass

        elif action == "error":
            await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (datetime.utcnow().isoformat(), num_id))
            await db.commit()
            # Возвращаем панель к "Взять номер"
            await callback.message.edit_text("❌ Номер помечен как ошибка.\nГотов к следующему?", reply_markup=worker_take_kb())
            try: await bot.send_message(user_id, f"❌ Номер `{phone}` не подошел.", parse_mode="Markdown")
            except: pass

        elif action == "dead":
            # СЛЕТ
            end_time = datetime.utcnow()
            await db.execute("UPDATE numbers SET status = 'finished', end_time = ? WHERE id = ?", (end_time.isoformat(), num_id))
            await db.commit()
            
            # Расчет времени
            start_dt = datetime.fromisoformat(start_str)
            diff = end_time - start_dt
            hours, remainder = divmod(diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            duration_str = f"{hours}ч {minutes}мин"
            
            # Возвращаем панель к "Взять номер"
            await callback.message.edit_text(
                f"🏁 **ЗАВЕРШЕНО**\n📱 `{phone}`\n⏱ {duration_str}\n\n👇 Жми кнопку ниже:",
                reply_markup=worker_take_kb(), # Кнопка возвращается!
                parse_mode="Markdown"
            )
            try: await bot.send_message(user_id, f"📉 Номер `{phone}` слетел (завершен).\nВремя: {duration_str}", parse_mode="Markdown")
            except: pass
    
    await callback.answer()

# --- ПЕРЕСЫЛКА ФОТО/СМС (Исправленная) ---

@router.message(F.photo & F.caption.startswith("/sms"))
async def worker_sms_photo(message: types.Message, bot: Bot):
    try:
        args = message.caption[4:].strip().split(' ', 1)
        phone, text = args[0], args[1]
    except:
        await message.reply("⚠️ Формат: Фото + `/sms +77... Текст`")
        return
    await send_to_user(message, bot, phone, text, True)

@router.message(Command("sms"))
async def worker_sms_text(message: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return
    try: phone, text = command.args.split(' ', 1)
    except: return
    await send_to_user(message, bot, phone, text, False)

async def send_to_user(message, bot, phone, text, is_photo):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM numbers WHERE phone = ? AND status IN ('work', 'active')", (phone,)) as c:
            row = await c.fetchone()
    
    if row:
        try:
            caption = f"🔔 **ВХОД!**\n📱 `{phone}`\n💬 **{text}**\n\n👇 Вводи код/сканируй QR!"
            if is_photo:
                await bot.send_photo(row[0], message.photo[-1].file_id, caption=caption, parse_mode="Markdown")
            else:
                await bot.send_message(row[0], caption, parse_mode="Markdown")
            await message.react([types.ReactionTypeEmoji(emoji="👍")])
        except Exception as e:
            await message.reply(f"❌ Ошибка: {e}")
    else:
        await message.reply("❌ Номер не в работе.")

# --- ПЕРЕСЫЛКА ОТВЕТА ОТ ЮЗЕРА ---
@router.message(F.reply_to_message)
async def user_reply(message: types.Message, bot: Bot):
    if message.chat.type != 'private': return
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('work', 'active') LIMIT 1", (message.from_user.id,)) as c:
            num = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            chat_id = (await c.fetchone())
            
    if num and chat_id:
        try:
            # Пересылаем в рабочий чат
            await bot.send_message(chat_id[0], f"📩 **ОТВЕТ ОТ ЮЗЕРА**\n📱 `{num[0]}`", parse_mode="Markdown")
            await message.forward(chat_id[0])
            await message.answer("✅ Передано.")
        except: pass

# --- АДМИН ПАНЕЛЬ (НОВЫЕ ФУНКЦИИ) ---

@router.callback_query(F.data == "admin_panel_start")
async def admin_start(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.message.edit_text("🔧 **Админка FAST TEAM**", reply_markup=admin_kb(), parse_mode="Markdown")

# 1. Скачать отчет
@router.callback_query(F.data == "adm_report")
async def admin_report(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID: return
    await callback.answer("⏳ Генерирую...")
    
    report_lines = []
    async with aiosqlite.connect(DB_NAME) as db:
        # Берем только finished
        async with db.execute("""
            SELECT u.username, n.phone, n.start_time, n.end_time, n.tariff_price 
            FROM numbers n 
            JOIN users u ON n.user_id = u.user_id 
            WHERE n.status = 'finished'
        """) as cursor:
            async for row in cursor:
                uname, phone, start, end, price = row
                try:
                    s = datetime.fromisoformat(start)
                    e = datetime.fromisoformat(end)
                    diff = e - s
                    hours, rem = divmod(diff.seconds, 3600)
                    mins, _ = divmod(rem, 60)
                    dur = f"{hours}ч {mins}мин"
                except: dur = "Ошибка времени"
                
                line = f"@{uname or 'NoUser'} | {phone} | {dur} | {price}"
                report_lines.append(line)
    
    file_data = "\n".join(report_lines).encode('utf-8')
    input_file = BufferedInputFile(file_data, filename=f"report_{date.today()}.txt")
    await bot.send_document(callback.message.chat.id, input_file, caption="📄 Отчет готов")

# 2. Изменить расписание
@router.callback_query(F.data == "adm_schedule")
async def adm_schedule_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("⏰ Введите время НАЧАЛА работы (МСК) в формате `ЧЧ:ММ` (например 07:00):", parse_mode="Markdown")
    await state.set_state(AdminState.setting_schedule_start)
    await callback.answer()

@router.message(AdminState.setting_schedule_start)
async def adm_sched_start_set(message: types.Message, state: FSMContext):
    await state.update_data(start_t=message.text.strip())
    await message.answer("⏰ Теперь введите время КОНЦА работы (например 17:30):")
    await state.set_state(AdminState.setting_schedule_end)

@router.message(AdminState.setting_schedule_end)
async def adm_sched_end_set(message: types.Message, state: FSMContext):
    data = await state.get_data()
    start_t = data['start_t']
    end_t = message.text.strip()
    
    # Сохраняем
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE config SET value = ? WHERE key = 'work_start'", (start_t,))
        await db.execute("UPDATE config SET value = ? WHERE key = 'work_end'", (end_t,))
        await db.commit()
    
    await message.answer(f"✅ График обновлен: {start_t} - {end_t} МСК")
    await state.clear()

# 3. Редактор тарифов (Простой JSON редактор)
@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs_view(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c:
            t_json = (await c.fetchone())[0]
            
    await callback.message.answer(
        f"💰 **Текущие тарифы (JSON):**\n`{t_json}`\n\n"
        "Чтобы добавить/изменить тариф, введи название:",
        parse_mode="Markdown"
    )
    # Здесь можно сделать сложнее, но пока простой проброс
    # Для простоты - предлагаю просто добавить новый через стейт

@router.callback_query(F.data == "admin_close")
async def admin_close(callback: CallbackQuery):
    await callback.message.delete()

# --- MAIN ---
async def main():
    print("🚀 FAST TEAM v10.0 Starting...")
    if not TOKEN or not ADMIN_ID: return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    if sys.platform == "win32": asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
