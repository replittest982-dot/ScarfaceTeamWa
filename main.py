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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile

# --- КОНФИГУРАЦИЯ ---
# Переменные берутся из BotHost (Environment Variables)
TOKEN = os.getenv("BOT_TOKEN") 
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "fast_team_v20.db" # Новая версия базы для поддержки авторизации
MSK_OFFSET = 3 

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
        # Таблица пользователей (добавили is_approved)
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Таблица номеров
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, method TEXT, 
            tariff_name TEXT, tariff_price TEXT, status TEXT, worker_id INTEGER, 
            start_time TIMESTAMP, end_time TIMESTAMP, last_ping TIMESTAMP, 
            is_check_pending INTEGER DEFAULT 0, worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Конфигурация
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Дефолтные настройки
        default_tariffs = {"ВЦ RU": "4$ Час", "MAX ФБХ": "3.5$ / 0 минут"}
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('tariffs', ?)", (json.dumps(default_tariffs, ensure_ascii=False),))
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_start', '07:00')")
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_end', '17:30')")
        await db.commit()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_msk_time(): 
    return datetime.utcnow() + timedelta(hours=MSK_OFFSET)

async def check_work_hours(user_id):
    # АДМИНУ МОЖНО ВСЕГДА (ДЛЯ ТЕСТОВ)
    if user_id == ADMIN_ID: return True
    
    now_msk = get_msk_time().time()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_start'") as c: 
            res_s = await c.fetchone()
            s = res_s[0] if res_s else "00:00"
        async with db.execute("SELECT value FROM config WHERE key='work_end'") as c: 
            res_e = await c.fetchone()
            e = res_e[0] if res_e else "23:59"
    st = datetime.strptime(s, "%H:%M").time()
    et = datetime.strptime(e, "%H:%M").time()
    if st <= et: return st <= now_msk <= et
    else: return st <= now_msk or now_msk <= et

def clean_phone(phone: str):
    clean = re.sub(r'[^\d+]', '', phone)
    if clean.startswith('8') and len(clean) == 11: clean = '+7' + clean[1:]
    elif clean.startswith('7') and len(clean) == 11: clean = '+' + clean
    elif len(clean) == 10 and clean.isdigit(): clean = '+7' + clean
    if not re.match(r'^\+\d{10,15}$', clean): return None
    return clean

# --- AFK МОНИТОР (ФОНОВАЯ ЗАДАЧА) ---
async def queue_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60) # Проверка каждую минуту
            now = datetime.utcnow()
            async with aiosqlite.connect(DB_NAME) as db:
                # 1. Спрашиваем "Ты тут?" если прошло 5 минут с создания или пинга
                async with db.execute("SELECT id, user_id, phone, last_ping, created_at FROM numbers WHERE status = 'queue' AND is_check_pending = 0") as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    num_id, user_id, phone, last_ping, created_at = row
                    base_str = last_ping if last_ping else created_at
                    base = datetime.fromisoformat(base_str)
                    
                    if (now - base).total_seconds() > 300: # 5 минут
                        try:
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👋 Я ТУТ!", callback_data=f"afk_here_{num_id}")]])
                            await bot.send_message(user_id, f"💤 **ВЫ ТУТ?**\nНомер `{phone}`.\nНажмите кнопку, иначе удалим через 3 мин из очереди!", reply_markup=kb, parse_mode="Markdown")
                            await db.execute("UPDATE numbers SET is_check_pending = 1, last_ping = ? WHERE id = ?", (now.isoformat(), num_id))
                            await db.commit()
                        except:
                            await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (now.isoformat(), num_id))
                            await db.commit()
                
                # 2. Удаляем, если не ответил за 3 минуты после вопроса
                async with db.execute("SELECT id, user_id, phone, last_ping FROM numbers WHERE status = 'queue' AND is_check_pending = 1") as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    if (now - datetime.fromisoformat(row[3])).total_seconds() > 180:
                        await db.execute("UPDATE numbers SET status = 'dead', end_time = ? WHERE id = ?", (now.isoformat(), row[0]))
                        await db.commit()
                        try: await bot.send_message(row[1], f"❌ Номер `{row[2]}` удален из очереди (неактив).")
                        except: pass
        except Exception as e:
            logging.error(f"Monitor Error: {e}")
            await asyncio.sleep(10)

@router.callback_query(F.data.startswith("afk_here_"))
async def afk_confirm(callback: CallbackQuery):
    num_id = callback.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET is_check_pending = 0, last_ping = ? WHERE id = ?", (datetime.utcnow().isoformat(), num_id))
        await db.commit()
    await callback.message.delete()
    await callback.answer("✅ Вы остались в очереди!")

# --- КЛАВИАТУРЫ ---
async def main_menu_kb(user_id: int):
    kb = [[InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
          [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]]
    if user_id == ADMIN_ID: kb.append([InlineKeyboardButton(text="🔧 Админ панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def tariffs_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: tariffs = json.loads((await c.fetchone())[0])
    kb = []
    for name, price in tariffs.items(): kb.append([InlineKeyboardButton(text=f"{name} ({price})", callback_data=f"trf_{name}")])
    kb.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- КЛАВИАТУРЫ ВОРКЕРА ---
async def topic_setup_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: 
            tariffs = json.loads((await c.fetchone())[0])
    kb = []
    for t in tariffs.keys():
        kb.append([InlineKeyboardButton(text=f"📌 Привязать: {t}", callback_data=f"set_topic_{t}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def worker_initial_kb(num_id): 
    # Кнопки сразу после выдачи номера
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Встал ✅", callback_data=f"w_act_{num_id}"), InlineKeyboardButton(text="Ошибка ❌", callback_data=f"w_err_{num_id}")]
    ])

def worker_finish_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📉 СЛЕТ / ВЫПЛАТА", callback_data=f"w_fin_{num_id}")]])

# Клавиатура админа для подтверждения юзера
def approve_kb(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"access_yes_{user_id}"), 
         InlineKeyboardButton(text="🚫 Отклонить", callback_data=f"access_no_{user_id}")]
    ])

def back_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_main")]])
def cancel_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])
def method_select_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Обычный код", callback_data="input_sms"), InlineKeyboardButton(text="📷 QR-код", callback_data="input_qr")], [InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Очередь", callback_data="adm_queue_stats"), InlineKeyboardButton(text="📥 Отчет", callback_data="adm_report")],
        [InlineKeyboardButton(text="⏰ График", callback_data="adm_schedule"), InlineKeyboardButton(text="💰 Тарифы", callback_data="adm_tariffs")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"), InlineKeyboardButton(text="⬅️ Выход", callback_data="admin_close")]
    ])

# --- ЛОГИКА ЮЗЕРА (СИСТЕМА СВОЙ-ЧУЖОЙ) ---
@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем статус юзера
        async with db.execute("SELECT is_approved FROM users WHERE user_id = ?", (user.id,)) as c:
            res = await c.fetchone()
        
        if not res:
            # Новый юзер
            await db.execute("INSERT INTO users (user_id, username, first_name, is_approved) VALUES (?, ?, ?, 0)", (user.id, user.username, user.first_name))
            await db.commit()
            
            # Уведомляем админа
            text_admin = (f"👤 **НОВЫЙ ЗАПРОС ДОСТУПА!**\n\n"
                          f"ID: `{user.id}`\n"
                          f"User: @{user.username or 'Нет юзернейма'}\n"
                          f"Имя: {user.first_name}\n"
                          f"Принять?")
            try:
                await message.bot.send_message(ADMIN_ID, text_admin, reply_markup=approve_kb(user.id), parse_mode="Markdown")
            except: pass
            
            await message.answer("🔒 **Доступ ограничен.**\nВаша заявка отправлена администратору. Ожидайте подтверждения.")
            return
        
        is_approved = res[0]
    
    # Если забанен или не принят
    if is_approved == 0 and user.id != ADMIN_ID:
        await message.answer("⏳ **Ваша заявка еще на рассмотрении.**\nОжидайте решения администратора.")
        return

    # Если принят или админ
    await message.answer("👋 **Добро пожаловать в FAST TEAM!**\nСкупка номеров по лучшим ценам.", parse_mode="Markdown", reply_markup=await main_menu_kb(user.id))

# --- ОБРАБОТКА ЗАЯВОК НА ДОСТУП ---
@router.callback_query(F.data.startswith("access_"))
async def access_handler(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID: return
    
    decision, user_id = callback.data.split('_')[1], int(callback.data.split('_')[2])
    
    if decision == "yes":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET is_approved = 1 WHERE user_id = ?", (user_id,))
            await db.commit()
        await callback.message.edit_text(f"✅ Доступ разрешен для ID `{user_id}`")
        try: await bot.send_message(user_id, "✅ **Администратор подтвердил ваш доступ!**\nНажмите /start для начала.")
        except: pass
    else:
        # Можно удалить из базы или оставить с is_approved=0 (как бан)
        await callback.message.edit_text(f"🚫 Доступ запрещен для ID `{user_id}`")
        try: await bot.send_message(user_id, "🚫 **Вам отказано в доступе.**")
        except: pass

@router.callback_query(F.data == "nav_main")
async def nav_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🤖 **Главное меню FAST TEAM**", reply_markup=await main_menu_kb(callback.from_user.id), parse_mode="Markdown")

@router.callback_query(F.data == "menu_profile")
async def show_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    today_start = datetime.combine(date.today(), datetime.min.time()).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND created_at >= ?", (user_id, today_start)) as c: today = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ?", (user_id,)) as c: total = (await c.fetchone())[0]
    await callback.message.edit_text(f"👤 **Профиль**\n🆔 ID: `{user_id}`\n🔥 Сегодня: **{today}**\n📦 Всего: **{total}**", reply_markup=back_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "menu_guide")
async def show_guide(callback: CallbackQuery):
    await callback.message.edit_text("📖 **Инструкция:**\n1. Жми Сдать номер.\n2. Выбери тариф.\n3. Введи номер (+77...).\n4. Жди код/QR.\n5. Не закрывай сессию!", reply_markup=back_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "select_tariff")
async def step_tariff(callback: CallbackQuery):
    # Проверка времени работы (Админ игнорит)
    if not await check_work_hours(callback.from_user.id):
        await callback.answer(f"💤 Не работаем сейчас (График)", show_alert=True); return
        
    await callback.message.edit_text("💰 **Выберите тариф:**", reply_markup=await tariffs_kb(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_"))
async def step_method(callback: CallbackQuery, state: FSMContext):
    t_name = callback.data.split('_')[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: tariffs = json.loads((await c.fetchone())[0])
    await state.update_data(tariff_name=t_name, tariff_price=tariffs.get(t_name, "?"))
    await callback.message.edit_text(f"✅ Тариф: **{t_name}**\nВыберите способ:", reply_markup=method_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(callback: CallbackQuery, state: FSMContext):
    method = 'sms' if callback.data == "input_sms" else 'qr'
    await state.update_data(method=method)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone FROM numbers WHERE user_id = ? AND status IN ('queue', 'work', 'active')", (callback.from_user.id,)) as c:
             if await c.fetchone(): await callback.answer("🚫 У вас уже есть активная заявка!", show_alert=True); return
    await callback.message.edit_text(f"✏️ Введите номер телефона (+77...):", reply_markup=cancel_kb(), parse_mode="Markdown")
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    t_name, t_price, method = data.get('tariff_name'), data.get('tariff_price'), data.get('method')
    text = message.text.strip()
    valid_phones = []
    async with aiosqlite.connect(DB_NAME) as db:
        for p in text.split(','):
            cl = clean_phone(p)
            if cl:
                async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (cl,)) as c:
                    if not await c.fetchone(): valid_phones.append(cl)
    if not valid_phones: await message.answer("❌ Ошибка номера.", reply_markup=cancel_kb()); return
    async with aiosqlite.connect(DB_NAME) as db:
        for p in valid_phones:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, status, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                (message.from_user.id, p, method, t_name, t_price, 'queue', datetime.utcnow().isoformat()))
        await db.commit()
    await message.answer(f"✅ **Принято!**\n📱 `{valid_phones[0]}`\n💰 {t_name}", reply_markup=await main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

# --- ВОРКЕР (ЛОГИКА /NUM) ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if message.chat.type not in ['group', 'supergroup']:
        await message.answer("⚠️ Пиши это в рабочем чате/топике!")
        return
    await message.answer("🛠 **НАСТРОЙКА ТОПИКА**\nКакой тариф привязать?", reply_markup=await topic_setup_kb())

@router.callback_query(F.data.startswith("set_topic_"))
async def set_topic_config(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    t_name = callback.data.split("set_topic_")[1]
    chat_id = callback.message.chat.id
    thread_id = callback.message.message_thread_id if callback.message.is_topic_message else 0
    key = f"topic_cfg_{chat_id}_{thread_id}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name))
        await db.commit()
    await callback.message.edit_text(f"✅ Топик привязан к: **{t_name}**.\nТеперь пишите /num чтобы брать номера.")

@router.message(Command("num"))
async def worker_get_num(message: types.Message, bot: Bot):
    chat_id = message.chat.id
    thread_id = message.message_thread_id if message.is_topic_message else 0
    worker_id = message.from_user.id
    key = f"topic_cfg_{chat_id}_{thread_id}"

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as c: res = await c.fetchone()
        if not res: await message.answer("⚠️ Топик не настроен! (/startwork)"); return
        t_name = res[0]
        
        # Берем самый старый номер из очереди по этому тарифу
        async with db.execute("SELECT id, user_id, phone, method, tariff_price FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY id ASC LIMIT 1", (t_name,)) as c:
            row = await c.fetchone()
            
        if not row:
            await message.answer(f"📭 Очередь **{t_name}** пуста!"); return
            
        num_id, user_id, phone, method, price = row
        
        # Обновляем статус
        await db.execute("UPDATE numbers SET status = 'work', worker_id = ?, start_time = ? WHERE id = ?", (worker_id, datetime.utcnow().isoformat(), num_id))
        await db.commit()
    
    m_str = "📷 QR" if method == 'qr' else "✉️ SMS"
    text = (f"🔧 **ВЗЯТ В РАБОТУ**\n"
            f"📱 `{phone}`\n"
            f"💰 **{t_name}** ({price})\n"
            f"📌 {m_str}\n\n"
            f"Копируй: `/sms {phone} КОД`")
    
    # Сразу выдаем кнопки Встал / Ошибка
    work_msg = await message.answer(text, parse_mode="Markdown", reply_markup=worker_initial_kb(num_id))
    
    async with aiosqlite.connect(DB_NAME) as db: 
        await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (work_msg.message_id, num_id))
        await db.commit()
    
    try: await bot.send_message(user_id, f"⚡️ Номер `{phone}` в работе!")
    except: pass

@router.callback_query(F.data.startswith("w_act_"))
async def worker_act(callback: CallbackQuery, bot: Bot):
    num_id = callback.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = 'active' WHERE id = ?", (num_id,))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as c: p, u = await c.fetchone()
        await db.commit()
    await callback.message.edit_text(f"🟢 **АКТИВЕН**\n📱 `{p}`", reply_markup=worker_finish_kb(num_id), parse_mode="Markdown")
    try: await bot.send_message(u, f"✅ Номер `{p}` встал!")
    except: pass

@router.callback_query(F.data.startswith("w_fin_") | F.data.startswith("w_err_"))
async def worker_fin(callback: CallbackQuery, bot: Bot):
    act = "finished" if "w_fin_" in callback.data else "dead"
    num_id = callback.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", (act, datetime.utcnow().isoformat(), num_id))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as c: p, u = await c.fetchone()
        await db.commit()
    
    await callback.message.edit_text(f"🏁 Заявка закрыта.\n📱 `{p}`")
    msg = "📉 Слет/Выплата." if act == "finished" else "❌ Ошибка/Отмена."
    try: await bot.send_message(u, f"{msg}\n📱 `{p}`")
    except: pass

# --- /SMS ЛОГИКА (ТЕКСТ И ФОТО) ---
async def send_sms_logic(message, bot, phone_raw, text, is_photo=False):
    # Очищаем номер от лишнего, если воркер ввел криво
    phone = clean_phone(phone_raw)
    if not phone:
        await message.reply("❌ Некорректный формат номера!")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        # Ищем этот номер в статусе work или active
        async with db.execute("SELECT user_id, worker_id FROM numbers WHERE phone = ? AND status IN ('work', 'active')", (phone,)) as c: 
            r = await c.fetchone()
    
    if not r:
        await message.reply("❌ Этот номер сейчас не в работе или не найден.")
        return
        
    user_id_db, worker_id_db = r
    
    # Проверка (не обязательная, но полезная): отправляет ли тот, кто взял номер? (или админ)
    # Если хочешь, чтобы ЛЮБОЙ воркер мог кинуть смс, убери это условие.
    # Но лучше оставить для порядка.
    if worker_id_db != message.from_user.id and message.from_user.id != ADMIN_ID:
         # Но так как /sms может писать любой, иногда бывает полезно помочь коллеге.
         # Если хочешь жестко: раскомментируй строку ниже
         # await message.reply("🚫 Вы не воркер этого номера!"); return
         pass 

    try:
        msg_to_user = f"🔔 **КОД / СООБЩЕНИЕ!**\n📱 `{phone}`\n💬 **{text}**"
        if is_photo:
            await bot.send_photo(user_id_db, message.photo[-1].file_id, caption=msg_to_user, parse_mode="Markdown")
        else:
            await bot.send_message(user_id_db, msg_to_user, parse_mode="Markdown")
        
        await message.react([types.ReactionTypeEmoji(emoji="👍")])
    except Exception as e:
        await message.reply(f"❌ Не удалось отправить юзеру (мб заблокировал бота).")

@router.message(F.photo & F.caption.startswith("/sms"))
async def sms_with_photo(msg: types.Message, bot: Bot):
    # Пример: /sms +7999... текст (с картинкой)
    try:
        # msg.caption: "/sms +7999... текст"
        args = msg.caption.split(maxsplit=2) # ['/sms', '+7...', 'текст']
        if len(args) < 2:
            await msg.reply("⚠️ Формат: `/sms номер текст` (с фото)")
            return
        
        phone = args[1]
        text = args[2] if len(args) > 2 else "Вам пришло фото!"
        await send_sms_logic(msg, bot, phone, text, is_photo=True)
    except Exception as e:
        pass

@router.message(Command("sms"))
async def sms_text_only(msg: types.Message, cmd: CommandObject, bot: Bot):
    # Пример: /sms +7999... код
    if not cmd.args:
        await msg.reply("⚠️ Формат: `/sms номер текст`")
        return
    
    try:
        # Разбиваем только по первому пробелу: "номер", "всё остальное"
        parts = cmd.args.split(' ', 1)
        phone = parts[0]
        text = parts[1] if len(parts) > 1 else "Вам пришло уведомление!"
        await send_sms_logic(msg, bot, phone, text, is_photo=False)
    except:
        pass

# --- АДМИНКА ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await state.clear()
    await c.message.edit_text("🔧 **Админка FAST TEAM**", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_queue_stats")
async def adm_stats(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT tariff_name, COUNT(*) FROM numbers WHERE status = 'queue' GROUP BY tariff_name") as cursor:
            stats = await cursor.fetchall()
    text = "📊 **Очередь:**\n\n" + ("\n".join([f"🔹 {t}: {cnt} шт." for t, cnt in stats]) if stats else "Пусто")
    await c.message.edit_text(text, reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_report")
async def adm_rep(c: CallbackQuery, bot: Bot):
    await c.answer("Генерирую...")
    ts = datetime.combine(date.today(), datetime.min.time()).isoformat()
    lines = []
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT u.username, n.phone, n.tariff_price, n.tariff_name FROM numbers n JOIN users u ON n.user_id=u.user_id WHERE n.status='finished' AND n.end_time >= ?", (ts,)) as cur:
            async for r in cur: lines.append(f"@{r[0]}|{r[1]}|{r[2]}|{r[3]}")
    if not lines: await c.message.answer("📂 Отчетов за сегодня нет."); return
    f = BufferedInputFile("\n".join(lines).encode(), filename="rep.txt")
    await bot.send_document(c.message.chat.id, f, caption="Отчет за сегодня")

@router.callback_query(F.data == "admin_broadcast")
async def adm_br(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.answer("📢 Отправьте сообщение для рассылки:")
    await state.set_state(AdminState.waiting_for_broadcast)
    await c.answer()

@router.message(AdminState.waiting_for_broadcast)
async def adm_br_send(msg: types.Message, state: FSMContext):
    cnt = 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as c:
            async for r in c:
                try: await msg.copy_to(r[0]); cnt+=1; await asyncio.sleep(0.05)
                except: pass
    await msg.answer(f"✅ Разослано: {cnt}")
    await state.clear()
    await msg.answer("Админка", reply_markup=admin_kb())

@router.callback_query(F.data == "adm_schedule")
async def adm_sched(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.answer("⏰ Введите НАЧАЛО (напр. 07:00):")
    await state.set_state(AdminState.setting_schedule_start)
    await c.answer()

@router.message(AdminState.setting_schedule_start)
async def adm_s_set(msg: types.Message, state: FSMContext):
    await state.update_data(s=msg.text)
    await msg.answer("⏰ Введите КОНЕЦ (напр. 17:30):")
    await state.set_state(AdminState.setting_schedule_end)

@router.message(AdminState.setting_schedule_end)
async def adm_e_set(msg: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE config SET value=? WHERE key='work_start'",(d['s'],))
        await db.execute("UPDATE config SET value=? WHERE key='work_end'",(msg.text,))
        await db.commit()
    await msg.answer(f"✅ Обновлено: {d['s']} - {msg.text}")
    await state.clear()

@router.callback_query(F.data == "adm_tariffs")
async def adm_trf(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c_db: t = json.loads((await c_db.fetchone())[0])
    await c.message.edit_text(f"💰 **Тарифы:**\n`{json.dumps(t, ensure_ascii=False, indent=2)}`", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "admin_close")
async def adm_cls(c: CallbackQuery, state: FSMContext): 
    await state.clear()
    await c.message.delete()

# --- START ---
async def main():
    if not TOKEN or not ADMIN_ID: print("❌ ЗАПОЛНИ TOKEN/ADMIN_ID"); return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    print("🚀 FAST TEAM v20.0 STARTED")
    asyncio.create_task(queue_monitor(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
