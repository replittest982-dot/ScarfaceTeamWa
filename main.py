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

# --- CONFIG ---
TOKEN = os.getenv("BOT_TOKEN") 
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "fast_team_v20.db"
MSK_OFFSET = 3 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
router = Router()

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    setting_price = State()
    setting_schedule_start = State()
    setting_schedule_end = State()

# --- DATABASE ENGINE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Users: добавили is_approved
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Numbers
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, method TEXT, 
            tariff_name TEXT, tariff_price TEXT, status TEXT, worker_id INTEGER, 
            start_time TIMESTAMP, end_time TIMESTAMP, last_ping TIMESTAMP, 
            is_check_pending INTEGER DEFAULT 0, worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Config + Topic Statuses
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Defaults
        default_tariffs = {"ВЦ RU": "4$ Час", "MAX ФБХ": "3.5$"}
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('tariffs', ?)", (json.dumps(default_tariffs, ensure_ascii=False),))
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_start', '07:00')")
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_end', '17:30')")
        await db.commit()

# --- UTILS ---
def get_msk_time(): 
    return datetime.utcnow() + timedelta(hours=MSK_OFFSET)

def extract_price(price_str):
    # Вытаскивает число из строки "4$ Час" -> 4.0
    match = re.search(r"(\d+(\.\d+)?)", str(price_str))
    return float(match.group(1)) if match else 0.0

async def is_topic_paused(chat_id, thread_id):
    key = f"topic_paused_{chat_id}_{thread_id if thread_id else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as c:
            res = await c.fetchone()
            return res and res[0] == "1"

async def toggle_topic_pause(chat_id, thread_id):
    key = f"topic_paused_{chat_id}_{thread_id if thread_id else 0}"
    is_paused = await is_topic_paused(chat_id, thread_id)
    new_val = "0" if is_paused else "1"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, new_val))
        await db.commit()
    return new_val == "1"

async def check_work_hours(user_id):
    if user_id == ADMIN_ID: return True
    now_msk = get_msk_time().time()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_start'") as c: s = (await c.fetchone())[0]
        async with db.execute("SELECT value FROM config WHERE key='work_end'") as c: e = (await c.fetchone())[0]
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

# --- KEYBOARDS ---
def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), 
         InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]
    ]
    if user_id == ADMIN_ID: 
        kb.append([InlineKeyboardButton(text="⚡️ ADMIN PANEL", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def tariffs_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: 
            tariffs = json.loads((await c.fetchone())[0])
    kb = []
    # Делаем по 2 кнопки в ряд для красоты
    items = list(tariffs.items())
    for i in range(0, len(items), 2):
        row = []
        row.append(InlineKeyboardButton(text=f"{items[i][0]} | {items[i][1]}", callback_data=f"trf_{items[i][0]}"))
        if i+1 < len(items):
            row.append(InlineKeyboardButton(text=f"{items[i+1][0]} | {items[i+1][1]}", callback_data=f"trf_{items[i+1][0]}"))
        kb.append(row)
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]])
def cancel_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])

def method_select_kb(): 
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 SMS Код", callback_data="input_sms"), InlineKeyboardButton(text="📷 QR Код", callback_data="input_qr")], 
        [InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]
    ])

# Worker Keyboards
async def worker_auto_kb(chat_id, thread_id):
    # Проверка статуса паузы
    is_paused = await is_topic_paused(chat_id, thread_id)
    status_emoji = "🔴 STOPPED" if is_paused else "🟢 ACTIVE"
    
    key = f"topic_cfg_{chat_id}_{thread_id if thread_id else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as c:
            res = await c.fetchone()
            tariff_name = res[0] if res else "NOT SET"
        count = 0
        if tariff_name != "NOT SET":
            async with db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND tariff_name=?", (tariff_name,)) as c:
                count = (await c.fetchone())[0]

    if is_paused:
        # Если группа на паузе, кнопка взять не работает (визуально или логически)
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⛔️ GROUP PAUSED ⛔️", callback_data="worker_paused_alert")],
            [InlineKeyboardButton(text=f"⚙️ {status_emoji}", callback_data="worker_topic_status")]
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🚀 ВЗЯТЬ ({count}) | {tariff_name}", callback_data="worker_take_auto")],
            [InlineKeyboardButton(text=f"🔄 Обновить", callback_data="worker_refresh_auto")]
        ])

def worker_active_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ВСТАЛ", callback_data=f"w_act_{num_id}"), InlineKeyboardButton(text="❌ ОШИБКА", callback_data=f"w_err_{num_id}")]
    ])
def worker_finish_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 ВЫПЛАТА / СЛЕТ", callback_data=f"w_fin_{num_id}")]
    ])

# Admin Keyboards
def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Очередь", callback_data="adm_queue_stats"), InlineKeyboardButton(text="💵 Отчет ($)", callback_data="adm_report")],
        [InlineKeyboardButton(text="⚙️ Тарифы", callback_data="adm_tariffs_edit"), InlineKeyboardButton(text="⏰ График", callback_data="adm_schedule")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"), InlineKeyboardButton(text="👥 Юзеры", callback_data="adm_users_manage")],
        [InlineKeyboardButton(text="🔙 Выход", callback_data="admin_close")]
    ])

def access_request_kb(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{user_id}"), 
         InlineKeyboardButton(text="🚫 Отказать", callback_data=f"acc_no_{user_id}")]
    ])

# --- MAIN LOGIC ---

@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем юзера
        async with db.execute("SELECT is_approved FROM users WHERE user_id = ?", (user.id,)) as c:
            res = await c.fetchone()
            
        if not res:
            # Новый юзер
            await db.execute("INSERT INTO users (user_id, username, first_name, is_approved) VALUES (?, ?, ?, 0)", (user.id, user.username, user.first_name))
            await db.commit()
            
            # Уведомляем админа
            text = (f"👤 **Запрос доступа!**\n\n"
                    f"ID: `{user.id}`\n"
                    f"User: @{user.username or 'NoNick'}\n\n"
                    f"Что делаем?")
            try:
                await message.bot.send_message(ADMIN_ID, text, reply_markup=access_request_kb(user.id), parse_mode="Markdown")
            except: pass
            
            await message.answer("🔒 **Доступ ограничен.**\nЗаявка отправлена администратору. Ожидайте подтверждения.")
            return

        is_approved = res[0]
        
    if is_approved:
        await message.answer("👋 **FAST TEAM PLATFORM**\nГлавное меню:", parse_mode="Markdown", reply_markup=main_menu_kb(user.id))
    else:
        await message.answer("⏳ **Ваша заявка на рассмотрении.**")

# --- ACCESS CONTROL CALLBACKS ---
@router.callback_query(F.data.startswith("acc_"))
async def access_control(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    action, user_id = c.data.split('_')[1], int(c.data.split('_')[2])
    
    if action == "ok":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET is_approved = 1 WHERE user_id = ?", (user_id,))
            await db.commit()
        await c.message.edit_text(f"✅ Доступ выдан ID `{user_id}`")
        try: await bot.send_message(user_id, "✅ **Доступ открыт!**\nНапишите /start")
        except: pass
    else:
        await c.message.edit_text(f"🚫 Отказано ID `{user_id}`")

# --- USER INTERFACE ---
@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("👋 **Главное меню**", reply_markup=main_menu_kb(c.from_user.id), parse_mode="Markdown")

@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    if not await check_work_hours(c.from_user.id):
        await c.answer("💤 Сейчас нерабочее время.", show_alert=True); return
    await c.message.edit_text("💰 **Выберите тариф:**", reply_markup=await tariffs_kb(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_"))
async def step_method(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split('_', 1)[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as cursor: 
            tariffs = json.loads((await cursor.fetchone())[0])
    
    t_price = tariffs.get(t_name, "?")
    await state.update_data(tariff_name=t_name, tariff_price=t_price)
    await c.message.edit_text(f"💎 Тариф: **{t_name}**\n💵 Прайс: {t_price}\n\nКак будем входить?", reply_markup=method_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(c: CallbackQuery, state: FSMContext):
    method = 'sms' if c.data == "input_sms" else 'qr'
    await state.update_data(method=method)
    
    # Check Active
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT 1 FROM numbers WHERE user_id = ? AND status IN ('queue', 'work', 'active')", (c.from_user.id,)) as cur:
             if await cur.fetchone(): 
                 await c.answer("⚠️ У тебя уже есть заявка в работе!", show_alert=True)
                 return
    
    await c.message.edit_text("📱 **Введите номер телефона:**\n(Например: +7999...)", reply_markup=cancel_kb(), parse_mode="Markdown")
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    t_name, t_price, method = data.get('tariff_name'), data.get('tariff_price'), data.get('method')
    
    phones = [clean_phone(p.strip()) for p in message.text.split(',')]
    valid_phones = [p for p in phones if p]

    if not valid_phones:
        await message.answer("❌ **Некорректный номер.** Попробуй еще раз.", reply_markup=cancel_kb(), parse_mode="Markdown")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        # Проверка дублей
        for p in valid_phones:
            async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (p,)) as cur:
                if await cur.fetchone():
                    await message.answer(f"⚠️ Номер `{p}` уже в работе!", reply_markup=cancel_kb(), parse_mode="Markdown"); return
        
        # Insert
        for p in valid_phones:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, status, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                (message.from_user.id, p, method, t_name, t_price, 'queue', datetime.utcnow().isoformat()))
        await db.commit()

    await message.answer(f"✅ **Заявка создана!**\n📱 `{valid_phones[0]}`\n💎 {t_name}\n\nОжидайте, воркер скоро примет.", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

@router.callback_query(F.data == "menu_profile")
async def show_profile(c: CallbackQuery):
    user_id = c.from_user.id
    today_start = datetime.combine(date.today(), datetime.min.time()).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND created_at >= ?", (user_id, today_start)) as cur: 
            today = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ?", (user_id,)) as cur: 
            total = (await cur.fetchone())[0]
            
    await c.message.edit_text(f"👤 **Личный кабинет**\n\n🆔 `{user_id}`\n📅 Сегодня сдано: **{today}**\n📦 Всего сдано: **{total}**", reply_markup=back_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "menu_guide")
async def show_guide(c: CallbackQuery):
    await c.message.edit_text("ℹ️ **Помощь**\n\n1. Нажми 'Сдать номер'.\n2. Выбери тариф и метод.\n3. Введи номер.\n4. Не закрывай диалог до конца сделки.", reply_markup=back_kb(), parse_mode="Markdown")

# --- WORKER SECTION ---
@router.message(Command("startwork"))
async def worker_start(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if message.chat.type not in ['group', 'supergroup']: return
    
    await message.answer("⚙️ **SETUP TOPIC**\nВыберите тариф для этого чата:", reply_markup=await topic_setup_kb())

async def topic_setup_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as c: 
            tariffs = json.loads((await c.fetchone())[0])
    kb = []
    for t in tariffs.keys():
        kb.append([InlineKeyboardButton(text=f"📌 {t}", callback_data=f"set_topic_{t}")])
    # Кнопка для стопа группы
    kb.append([InlineKeyboardButton(text="⏯ СТОП/СТАРТ ГРУППЫ", callback_data="toggle_group_pause")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

@router.callback_query(F.data.startswith("set_topic_"))
async def set_topic_config(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_topic_")[1]
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_cfg_{chat_id}_{thread_id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name))
        await db.commit()
    
    await c.message.edit_text(f"✅ Топик настроен на: **{t_name}**", reply_markup=await worker_auto_kb(chat_id, thread_id))

@router.callback_query(F.data == "toggle_group_pause")
async def toggle_pause_handler(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    
    paused = await toggle_topic_pause(chat_id, thread_id)
    status = "🔴 PAUSED" if paused else "🟢 RESUMED"
    await c.answer(f"Status: {status}", show_alert=True)
    await c.message.delete() # Удаляем меню настройки

@router.callback_query(F.data == "worker_refresh_auto")
async def worker_refresh_auto(c: CallbackQuery):
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    try: await c.message.edit_reply_markup(reply_markup=await worker_auto_kb(chat_id, thread_id))
    except: pass
    await c.answer()

@router.callback_query(F.data == "worker_paused_alert")
async def worker_paused_alert(c: CallbackQuery):
    await c.answer("⛔️ Эта группа остановлена админом!", show_alert=True)

@router.callback_query(F.data == "worker_take_auto")
async def worker_take_auto(c: CallbackQuery, bot: Bot):
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    worker_id = c.from_user.id

    if await is_topic_paused(chat_id, thread_id):
        await c.answer("⛔️ Группа на паузе!", show_alert=True); return

    key = f"topic_cfg_{chat_id}_{thread_id}"
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as cur: 
            res = await cur.fetchone()
            if not res: await c.answer("⚠️ Не настроено!", show_alert=True); return
            t_name = res[0]
        
        async with db.execute("SELECT id, user_id, phone, method, tariff_price FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY id ASC LIMIT 1", (t_name,)) as cur:
            row = await cur.fetchone()
        
        if not row:
            await c.answer("📭 Пусто!", show_alert=True); return
            
        num_id, user_id, phone, method, price = row
        await db.execute("UPDATE numbers SET status = 'work', worker_id = ?, start_time = ? WHERE id = ? AND status = 'queue'", (worker_id, datetime.utcnow().isoformat(), num_id))
        if db.rowcount == 0: await c.answer("⚠️ Уже забрали!"); return
        await db.commit()

    # Красивая выдача для копирования
    m_icon = "📷 QR" if method == 'qr' else "💬 SMS"
    text = (f"🚀 **В РАБОТЕ**\n\n"
            f"📱 `{phone}`\n"
            f"💰 {t_name} | {price}\n"
            f"{m_icon}\n\n"
            f"👇 Команда для кода:\n`/sms {phone} текст`")
            
    msg = await c.message.edit_text(text, parse_mode="Markdown", reply_markup=worker_active_kb(num_id))
    async with aiosqlite.connect(DB_NAME) as db: 
        await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (msg.message_id, num_id))
        await db.commit()
    
    try: await bot.send_message(user_id, f"⚡️ Воркер принял номер `{phone}`")
    except: pass

@router.callback_query(F.data.startswith("w_act_"))
async def worker_act(c: CallbackQuery, bot: Bot):
    num_id = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = 'active' WHERE id = ?", (num_id,))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as cur: 
            res = await cur.fetchone()
            p, u = res if res else (None, None)
        await db.commit()
    
    if p:
        await c.message.edit_text(f"🟢 **АКТИВЕН**\n📱 `{p}`", reply_markup=worker_finish_kb(num_id), parse_mode="Markdown")
        try: await bot.send_message(u, f"✅ Номер `{p}` активен!")
        except: pass

@router.callback_query(F.data.startswith("w_fin_") | F.data.startswith("w_err_"))
async def worker_fin(c: CallbackQuery, bot: Bot):
    status = "finished" if "w_fin_" in c.data else "dead"
    num_id = c.data.split('_')[2]
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", (status, datetime.utcnow().isoformat(), num_id))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as cur:
            res = await cur.fetchone()
            p, u = res if res else ("?", None)
        await db.commit()

    # Возврат к авто-меню
    await c.message.edit_text(f"🏁 Завершен: `{p}`", reply_markup=await worker_auto_kb(chat_id, thread_id))
    
    user_msg = "💰 Выплата оформлена!" if status == "finished" else "❌ Отмена / Ошибка"
    try: await bot.send_message(u, f"{user_msg}\n📱 `{p}`")
    except: pass

# --- SMS HANDLER ---
@router.message(Command("sms"))
async def sms_handler(msg: types.Message, cmd: CommandObject, bot: Bot):
    if not cmd.args: return
    try:
        phone, text = cmd.args.split(' ', 1)
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id, worker_id FROM numbers WHERE phone = ? AND status IN ('work', 'active')", (phone,)) as c:
                res = await c.fetchone()
        
        if res:
            u_id, w_id = res
            if w_id != msg.from_user.id and msg.from_user.id != ADMIN_ID: return
            
            await bot.send_message(u_id, f"🔔 **SMS / КОД**\n📱 `{phone}`\n💬 `{text}`", parse_mode="Markdown")
            await msg.react([types.ReactionTypeEmoji(emoji="👍")])
        else:
            await msg.reply("❌ Номер не в работе.")
    except: pass

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("⚡️ **CONTROL PANEL**", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_report")
async def adm_report(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    ts = datetime.combine(date.today(), datetime.min.time()).isoformat()
    total_money = 0.0
    text_report = f"📅 **ОТЧЕТ ЗА {date.today()}**\n\n"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, tariff_price FROM numbers WHERE status='finished' AND end_time >= ?", (ts,)) as cur:
            rows = await cur.fetchall()
            
    if not rows:
        await c.message.edit_text("📂 Сегодня пусто.", reply_markup=admin_kb()); return

    for r in rows:
        price_val = extract_price(r[1]) # Парсим "4$" -> 4.0
        total_money += price_val
        text_report += f"✅ `{r[0]}` | {r[1]}\n"
        
    text_report += f"\n💵 **ИТОГО: {total_money}$**"
    
    # Чтобы не упереться в лимит сообщения телеграм (4096 символов)
    if len(text_report) > 4000:
        f = BufferedInputFile(text_report.encode(), filename="report.txt")
        await c.message.answer_document(f, caption=f"💵 ИТОГО: {total_money}$")
    else:
        await c.message.edit_text(text_report, reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_tariffs_edit")
async def adm_tariffs_edit(c: CallbackQuery):
    # Упрощенный просмотр, редактирование лучше через JSON файл или сложные диалоги
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='tariffs'") as cur: 
            t = json.loads((await cur.fetchone())[0])
    
    msg = "⚙️ **Текущие тарифы (JSON):**\n\n" + json.dumps(t, indent=2, ensure_ascii=False)
    await c.message.edit_text(msg, reply_markup=admin_kb())

@router.callback_query(F.data == "admin_close")
async def adm_close(c: CallbackQuery):
    await c.message.delete()
    await c.message.answer("👋", reply_markup=main_menu_kb(c.from_user.id))

# --- STARTUP ---
async def main():
    if not TOKEN or not ADMIN_ID:
        print("❌ ENV VARIABLES MISSING")
        return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    
    # AFK Monitor (Simplified for clarity, insert full logic if needed)
    asyncio.create_task(queue_monitor(bot))
    
    print("🚀 CYBERPUNK v20.0 STARTED")
    await dp.start_polling(bot)

# Insert the Queue Monitor from previous version here
async def queue_monitor(bot: Bot):
    while True:
        await asyncio.sleep(60)
        # ... (Код монитора из v19.1) ...

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
