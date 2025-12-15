import asyncio
import logging
import sys
import os
import re
import json
from datetime import datetime, time, timedelta, date
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
DB_NAME = "fast_team_v13.db" 

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
            tariff_name TEXT,
            tariff_price TEXT,
            status TEXT, 
            worker_id INTEGER, 
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            last_ping TIMESTAMP, 
            is_check_pending INTEGER DEFAULT 0,
            worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        
        # Миграции
        try: 
            await db.execute("ALTER TABLE numbers ADD COLUMN last_ping TIMESTAMP")
            await db.execute("ALTER TABLE numbers ADD COLUMN is_check_pending INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE numbers ADD COLUMN worker_id INTEGER")
        except: pass
        
        default_tariffs = {"ВЦ RU": "4$ Час", "MAX ФБХ": "3.5$ / 0 минут"}
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('tariffs', ?)", (json.dumps(default_tariffs, ensure_ascii=False),))
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_start', '07:00')")
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_end', '17:30')")
        await db.commit()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_msk_time():
    return datetime.utcnow() + timedelta(hours=MSK_OFFSET)

async def check_work_hours(user_id):
    # GOD MODE: Админ работает всегда
    if user_id == ADMIN_ID:
        return True
        
    now_msk = get_msk_time().time()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_start'") as c: start_str = (await c.fetchone())[0]
        async with db.execute("SELECT value FROM config WHERE key='work_end'") as c: end_str = (await c.fetchone())[0]
    
    start_time = datetime.strptime(start_str, "%H:%M").time()
    end_time = datetime.strptime(end_str, "%H:%M").time()
    if start_time <= end_time: return start_time <= now_msk <= end_time
    else: return start_time <= now_msk or now_msk <= end_time

def clean_phone(phone: str):
    clean = re.sub(r'[^\d+]', '', phone)
    if clean.startswith('8') and len(clean) == 11: clean = '+7' + clean[1:]
    elif clean.startswith('7') and len(clean) == 11: clean = '+' + clean
    elif len(clean) == 10 and clean.isdigit(): clean = '+7' + clean
    if not re.match(r'^\+\d{10,15}$', clean): return None
    return clean

# --- AFK SYSTEM (МОНИТОРИНГ) ---
async def queue_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(30)
            now = datetime.utcnow()
            async with aiosqlite.connect(DB_NAME) as db:
                # 1. Проверка на неактивность (5 мин)
                async with db.execute("SELECT id, user_id, phone, last_ping, created_at FROM numbers WHERE status = 'queue' AND is_check_pending = 0") as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    num_id, user_id, phone, last_ping, created_at = row
                    base_time = datetime.fromisoformat(last_ping if last_ping else created_at)
                    if (now - base_time).total_seconds() > 300:
                        try:
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👋 Я ТУТ!", callback_data=f"afk_here_{num_id}")]])
                            await bot.send_message(user_id, f"💤 **ВЫ ТУТ?**\nНомер `{phone}`.\nНажмите кнопку за 3 мин!", reply_markup=kb, parse_mode="Markdown")
                            await db.execute("UPDATE numbers SET is_check_pending = 1, last_ping = ? WHERE id = ?", (now.isoformat(), num_id))
                            await db.commit()
                        except:
                            await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (now.isoformat(), num_id))
                            await db.commit()

                # 2. Кик (3 мин после вопроса)
                async with db.execute("SELECT id, user_id, phone, last_ping FROM numbers WHERE status = 'queue' AND is_check_pending = 1") as cursor:
                    pending_rows = await cursor.fetchall()
                for row in pending_rows:
                    num_id, user_id, phone, last_ping = row
                    if (now - datetime.fromisoformat(last_ping)).total_seconds() > 180:
                        await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (now.isoformat(), num_id))
                        await db.commit()
                        try: await bot.send_message(user_id, f"❌ Номер `{phone}` удален за неактив.", parse_mode="Markdown")
                        except: pass
        except Exception as e:
            print(f"Monitor Error: {e}")
            await asyncio.sleep(5)

@router.callback_query(F.data.startswith("afk_here_"))
async def afk_confirm(callback: CallbackQuery):
    num_id = callback.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT status FROM numbers WHERE id = ?", (num_id,)) as c: row = await c.fetchone()
        if not row or row[0] != 'queue': await callback.answer("Уже не актуально.", show_alert=True); await callback.message.delete(); return
        await db.execute("UPDATE numbers SET is_check_pending = 0, last_ping = ? WHERE id = ?", (datetime.utcnow().isoformat(), num_id))
        await db.commit()
    await callback.message.delete()
    await callback.answer("✅ Вы в очереди!", show_alert=True)

# --- КЛАВИАТУРЫ ---
async def main_menu_kb(user_id: int):
    kb = [[InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
          [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]]
    if user_id == ADMIN_ID: kb.append([InlineKeyboardButton(text="🔧 Админ панель (FAST TEAM)", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def tariffs_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: tariffs = json.loads((await c.fetchone())[0])
    kb = []
    for name, price in tariffs.items(): kb.append([InlineKeyboardButton(text=f"{name} ({price})", callback_data=f"trf_{name}")])
    kb.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_to_main_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_main")]])
def method_select_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Обычный код", callback_data="input_sms"), InlineKeyboardButton(text="📷 QR-код", callback_data="input_qr")], [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])
def cancel_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])
def worker_take_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 ВЗЯТЬ ЗАЯВКУ", callback_data="worker_take_new")]])
def worker_active_kb(num_id): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Встал", callback_data=f"w_active_{num_id}"), InlineKeyboardButton(text="❌ Ошибка/Спам", callback_data=f"w_error_{num_id}")]])
def worker_finish_kb(num_id): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📉 СЛЕТ", callback_data=f"w_dead_{num_id}")]])
def admin_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📥 Отчет за СЕГОДНЯ (.txt)", callback_data="adm_report")], [InlineKeyboardButton(text="⏰ Время работы", callback_data="adm_schedule"), InlineKeyboardButton(text="💰 Тарифы", callback_data="adm_tariffs")], [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"), InlineKeyboardButton(text="⬅️ Выход", callback_data="admin_close")]])

# --- ЛОГИКА ЮЗЕРА (С ПРОФИЛЕМ И ГАЙДОМ) ---
@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username", (user.id, user.username, user.first_name))
        await db.commit()
    await message.answer("👋 **Добро пожаловать в FAST TEAM!**\nСкупка номеров по лучшим ценам.", parse_mode="Markdown", reply_markup=await main_menu_kb(user.id))

@router.callback_query(F.data == "nav_main")
async def nav_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🤖 **Главное меню FAST TEAM**", reply_markup=await main_menu_kb(callback.from_user.id), parse_mode="Markdown")

# --- ВОССТАНОВЛЕННЫЕ ХЕНДЛЕРЫ ---
@router.callback_query(F.data == "menu_guide")
async def show_guide(callback: CallbackQuery):
    text = (
        "📖 **Инструкция FAST TEAM:**\n\n"
        "1. Жми **Сдать номер**.\n"
        "2. Выбери тариф и тип (СМС/QR).\n"
        "3. Введи номер (+77...).\n"
        "4. Жди сообщения от бота (Код или QR).\n"
        "5. Если QR — сканируй быстро! Если код — вводи.\n"
        "6. Не закрывай сессию до выплаты!"
    )
    await callback.message.edit_text(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "menu_profile")
async def show_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    # Считаем только за СЕГОДНЯ
    today_start = datetime.combine(date.today(), datetime.min.time()).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND created_at >= ?", (user_id, today_start)) as c:
            today_count = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ?", (user_id,)) as c:
            total_count = (await c.fetchone())[0]
            
    text = (
        f"👤 **Профиль**\n"
        f"🆔 ID: `{user_id}`\n"
        f"👤 Имя: {callback.from_user.first_name}\n\n"
        f"🔥 Сдано сегодня (с 00:00): **{today_count}**\n"
        f"📦 Всего сдано: **{total_count}**"
    )
    await callback.message.edit_text(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
# --------------------------------

@router.callback_query(F.data == "select_tariff")
async def step_tariff(callback: CallbackQuery):
    if not await check_work_hours(callback.from_user.id):
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT value FROM config WHERE key='work_start'") as c: s = (await c.fetchone())[0]
            async with db.execute("SELECT value FROM config WHERE key='work_end'") as c: e = (await c.fetchone())[0]
        await callback.answer(f"💤 Не работаем (График {s}-{e} МСК)", show_alert=True)
        return
    await callback.message.edit_text("💰 **Выберите тариф:**", reply_markup=await tariffs_kb(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_"))
async def step_method(callback: CallbackQuery, state: FSMContext):
    tariff_name = callback.data.split('_')[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: tariffs = json.loads((await c.fetchone())[0])
    price = tariffs.get(tariff_name, "Неизвестно")
    await state.update_data(tariff_name=tariff_name, tariff_price=price)
    await callback.message.edit_text(f"✅ Тариф: **{tariff_name}**\nВыберите способ:", reply_markup=method_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(callback: CallbackQuery, state: FSMContext):
    method = 'sms' if callback.data == "input_sms" else 'qr'
    await state.update_data(method=method)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('queue', 'work', 'active')", (callback.from_user.id,)) as c:
             if await c.fetchone(): await callback.answer("🚫 У вас уже есть активная заявка!", show_alert=True); return
    m_text = "✉️ SMS" if method == 'sms' else "📷 QR-код"
    await callback.message.edit_text(f"✏️ Тип: **{m_text}**\nВведите номер телефона:", reply_markup=cancel_kb(), parse_mode="Markdown")
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
                async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (cleaned,)) as c:
                    if not await c.fetchone(): valid_phones.append(cleaned)
    if not valid_phones:
        await message.answer("❌ **Ошибка!** Номер некорректен или уже в работе.", reply_markup=cancel_kb(), parse_mode="Markdown")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        for phone in valid_phones:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, status, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                (message.from_user.id, phone, method, tariff_name, tariff_price, 'queue', datetime.utcnow().isoformat()))
        await db.commit()
    await message.answer(f"✅ **Заявка принята!**\n📱 `{valid_phones[0]}`\n💰 {tariff_name}", reply_markup=await main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

# --- ВОРКЕР ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if message.chat.type in ['group', 'supergroup']:
        chat_id = message.chat.id
        thread_id = message.message_thread_id if message.is_topic_message else None
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_chat_id', ?)", (str(chat_id),))
            if thread_id: await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_thread_id', ?)", (str(thread_id),))
            else: await db.execute("DELETE FROM config WHERE key='work_thread_id'")
            await db.commit()
        await message.answer("🚀 **Панель FAST TEAM активирована!**", reply_markup=worker_take_kb())

@router.callback_query(F.data == "worker_take_new")
async def worker_take_job(callback: CallbackQuery, bot: Bot):
    worker_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c:
            res = await c.fetchone()
            if not res or str(callback.message.chat.id) != res[0]: return
        async with db.execute("SELECT id, user_id, phone, method, tariff_name FROM numbers WHERE status = 'queue' ORDER BY id ASC LIMIT 1") as cursor:
            row = await cursor.fetchone()
        if not row: await callback.answer("📭 Пусто!", show_alert=True); return
        row_id, user_id, phone, method, tariff = row
        method_str = "📷 QR-КОД" if method == 'qr' else "✉️ SMS-КОД"
        await db.execute("UPDATE numbers SET status = 'work', worker_id = ?, start_time = ? WHERE id = ?", (worker_id, datetime.utcnow().isoformat(), row_id))
        await db.commit()
    
    text = f"🔧 **В РАБОТЕ**\n📱 `{phone}`\n📌 Тип: **{method_str}** | {tariff}\n👤 Воркер: {callback.from_user.first_name}\n👇 **Копируй:**\n`/sms {phone} текст`"
    work_msg = await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=worker_active_kb(row_id))
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (work_msg.message_id, row_id))
        await db.commit()
    try: await bot.send_message(user_id, f"⚡️ Номер `{phone}` взят в работу!", parse_mode="Markdown")
    except: pass

@router.callback_query(F.data.startswith("w_"))
async def worker_logic(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split('_')
    action, num_id = parts[1], parts[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, start_time, user_id, worker_id FROM numbers WHERE id = ?", (num_id,)) as c: row = await c.fetchone()
        if not row: return
        phone, start_str, user_id, worker_id = row
        if worker_id != callback.from_user.id and callback.from_user.id != ADMIN_ID:
            await callback.answer("🚫 Это не ваша заявка!", show_alert=True); return
        
        if action == "active":
            await db.execute("UPDATE numbers SET status = 'active' WHERE id = ?", (num_id,))
            await db.commit()
            await callback.message.edit_text(f"🟢 **АКТИВЕН**\n📱 `{phone}`", reply_markup=worker_finish_kb(num_id), parse_mode="Markdown")
            try: await bot.send_message(user_id, f"✅ Номер `{phone}` встал!", parse_mode="Markdown")
            except: pass
        elif action == "error":
            await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (datetime.utcnow().isoformat(), num_id))
            await db.commit()
            await callback.message.edit_text("❌ Ошибка.", reply_markup=worker_take_kb())
            try: await bot.send_message(user_id, f"❌ Номер `{phone}` не подошел.", parse_mode="Markdown")
            except: pass
        elif action == "dead":
            end_time = datetime.utcnow()
            await db.execute("UPDATE numbers SET status = 'finished', end_time = ? WHERE id = ?", (end_time.isoformat(), num_id))
            await db.commit()
            diff = end_time - datetime.fromisoformat(start_str)
            dur_str = f"{diff.seconds//3600}ч {(diff.seconds%3600)//60}мин"
            await callback.message.edit_text(f"🏁 **ЗАВЕРШЕНО**\n📱 `{phone}`\n⏱ {dur_str}", reply_markup=worker_take_kb(), parse_mode="Markdown")
            try: await bot.send_message(user_id, f"📉 Номер `{phone}` завершен (Слет).", parse_mode="Markdown")
            except: pass
    await callback.answer()

@router.message(F.photo & F.caption.startswith("/sms"))
async def worker_sms_photo(message: types.Message, bot: Bot):
    try: args = message.caption[4:].strip().split(' ', 1); phone, text = args[0], args[1]
    except: await message.reply("⚠️ Формат: Фото + `/sms +77... Текст`"); return
    await send_to_user(message, bot, phone, text, True)

@router.message(Command("sms"))
async def worker_sms_text(message: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return
    try: phone, text = command.args.split(' ', 1)
    except: return
    await send_to_user(message, bot, phone, text, False)

async def send_to_user(message, bot, phone, text, is_photo):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, worker_id FROM numbers WHERE phone = ? AND status IN ('work', 'active')", (phone,)) as c: row = await c.fetchone()
    if row:
        if row[1] is not None and row[1] != message.from_user.id and message.from_user.id != ADMIN_ID: await message.reply("🚫 Чужой номер."); return
        try:
            caption = f"🔔 **ВХОД!**\n📱 `{phone}`\n💬 **{text}**\n\n👇 Вводи код/сканируй QR!"
            if is_photo: await bot.send_photo(row[0], message.photo[-1].file_id, caption=caption, parse_mode="Markdown")
            else: await bot.send_message(row[0], caption, parse_mode="Markdown")
            await message.react([types.ReactionTypeEmoji(emoji="👍")])
        except: await message.reply(f"❌ Ошибка отправки.")
    else: await message.reply("❌ Номер не в работе.")

@router.message(F.reply_to_message)
async def user_reply(message: types.Message, bot: Bot):
    if message.chat.type != 'private': return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('work', 'active') LIMIT 1", (message.from_user.id,)) as c: num = await c.fetchone()
        async with db.execute("SELECT value FROM config WHERE key='work_chat_id'") as c: chat_id = (await c.fetchone())
    if num and chat_id:
        try:
            await bot.send_message(chat_id[0], f"📩 **ОТВЕТ ОТ ЮЗЕРА**\n📱 `{num[0]}`", parse_mode="Markdown")
            await message.forward(chat_id[0])
            await message.answer("✅ Передано.")
        except: pass

# --- АДМИНКА ---
@router.callback_query(F.data == "admin_panel_start")
async def admin_start(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.message.edit_text("🔧 **Админка FAST TEAM**", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_report")
async def admin_report(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID: return
    await callback.answer("⏳ Генерирую...")
    report_lines = []
    # ФИЛЬТР ПО ДАТЕ (СЕГОДНЯ)
    today_start = datetime.combine(date.today(), datetime.min.time()).isoformat()
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Добавил фильтр: AND n.created_at >= today_start (или end_time)
        # Лучше фильтровать по end_time, чтобы попадали те, кто завершил СЕГОДНЯ
        async with db.execute("""
            SELECT u.username, n.phone, n.start_time, n.end_time, n.tariff_price 
            FROM numbers n 
            JOIN users u ON n.user_id = u.user_id 
            WHERE n.status = 'finished' AND n.end_time >= ?
        """, (today_start,)) as cursor:
            async for row in cursor:
                uname, phone, start, end, price = row
                try: 
                    diff = datetime.fromisoformat(end) - datetime.fromisoformat(start)
                    dur = f"{diff.seconds//3600}ч {(diff.seconds%3600)//60}мин"
                except: dur = "Ошибка"
                report_lines.append(f"@{uname or 'NoUser'} | {phone} | {dur} | {price}")
    
    if not report_lines:
        await callback.message.answer("📂 Отчетов за сегодня нет.")
        return

    file_data = "\n".join(report_lines).encode('utf-8')
    input_file = BufferedInputFile(file_data, filename=f"report_{date.today()}.txt")
    await bot.send_document(callback.message.chat.id, input_file, caption="📄 Отчет за СЕГОДНЯ (с 00:00)")

@router.callback_query(F.data == "adm_schedule")
async def adm_schedule(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("⏰ Введите время НАЧАЛА (МСК) (напр. 07:00):"); await state.set_state(AdminState.setting_schedule_start); await callback.answer()
@router.message(AdminState.setting_schedule_start)
async def adm_start_set(msg: types.Message, state: FSMContext): await state.update_data(s=msg.text); await msg.answer("⏰ Введите время КОНЦА (напр. 17:30):"); await state.set_state(AdminState.setting_schedule_end)
@router.message(AdminState.setting_schedule_end)
async def adm_end_set(msg: types.Message, state: FSMContext):
    data = await state.get_data(); s = data['s']; e = msg.text
    async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE config SET value=? WHERE key='work_start'",(s,)); await db.execute("UPDATE config SET value=? WHERE key='work_end'",(e,)); await db.commit()
    await msg.answer(f"✅ График: {s}-{e}"); await state.clear()

@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: t = json.loads((await c.fetchone())[0])
    await callback.message.answer(f"💰 Тарифы:\n`{json.dumps(t, ensure_ascii=False, indent=2)}`", parse_mode="Markdown")

@router.callback_query(F.data == "admin_broadcast")
async def admin_br(callback: CallbackQuery, state: FSMContext): await callback.message.answer("✍️ Текст рассылки:"); await state.set_state(AdminState.waiting_for_broadcast); await callback.answer()
@router.message(AdminState.waiting_for_broadcast)
async def admin_br_send(msg: types.Message, state: FSMContext, bot: Bot):
    cnt=0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as c:
            async for r in c:
                try: await msg.copy_to(r[0]); cnt+=1; await asyncio.sleep(0.05)
                except: pass
    await msg.answer(f"✅ Разослано: {cnt}"); await state.clear()
@router.callback_query(F.data == "admin_close")
async def admin_close(callback: CallbackQuery): await callback.message.delete()

# --- START ---
async def main():
    print("🚀 FAST TEAM v13.0 (Day Zero) Starting...")
    if not TOKEN or not ADMIN_ID: return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(queue_monitor(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    if sys.platform == "win32": asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
