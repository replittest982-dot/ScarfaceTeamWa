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
DB_NAME = "fast_team_v21.db"
MSK_OFFSET = 3 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
router = Router()

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    setting_schedule_start = State()
    setting_schedule_end = State()
    # Tariff Management
    trf_adding_name = State()
    trf_adding_price = State()
    trf_adding_hold = State()
    trf_editing_value = State()

# --- DATABASE ENGINE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Users
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Numbers
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, method TEXT, 
            tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, status TEXT, worker_id INTEGER, 
            start_time TIMESTAMP, end_time TIMESTAMP, last_ping TIMESTAMP, 
            is_check_pending INTEGER DEFAULT 0, worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # New Tariffs Table (Вместо JSON)
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price TEXT,
            hold_info TEXT
        )""")

        # Config (Global settings)
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Defaults
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_start', '07:00')")
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_end', '17:30')")
        
        # Default Tariffs (если таблица пуста)
        async with db.execute("SELECT COUNT(*) FROM tariffs") as c:
            if (await c.fetchone())[0] == 0:
                await db.execute("INSERT INTO tariffs (name, price, hold_info) VALUES (?, ?, ?)", ("ВЦ RU", "4$", "0 мин"))
                await db.execute("INSERT INTO tariffs (name, price, hold_info) VALUES (?, ?, ?)", ("MAX ФБХ", "3.5$", "15 мин"))
        
        await db.commit()

# --- UTILS ---
def get_msk_time(): 
    return datetime.utcnow() + timedelta(hours=MSK_OFFSET)

def extract_price(price_str):
    match = re.search(r"(\d+(\.\d+)?)", str(price_str))
    return float(match.group(1)) if match else 0.0

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

async def tariffs_kb_user():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, price, hold_info FROM tariffs") as c: 
            rows = await c.fetchall()
    kb = []
    # По 2 в ряд
    for i in range(0, len(rows), 2):
        row_btns = []
        n1, p1, h1 = rows[i]
        row_btns.append(InlineKeyboardButton(text=f"{n1} | {p1}", callback_data=f"trf_pick_{n1}"))
        if i+1 < len(rows):
            n2, p2, h2 = rows[i+1]
            row_btns.append(InlineKeyboardButton(text=f"{n2} | {p2}", callback_data=f"trf_pick_{n2}"))
        kb.append(row_btns)
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
    key = f"topic_cfg_{chat_id}_{thread_id if thread_id else 0}"
    # Проверка паузы группы
    is_paused_key = f"topic_paused_{chat_id}_{thread_id if thread_id else 0}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (is_paused_key,)) as c:
            paused = (await c.fetchone())
            is_paused = paused and paused[0] == "1"
            
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as c:
            res = await c.fetchone()
            tariff_name = res[0] if res else None
        
        count = 0
        if tariff_name:
            async with db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND tariff_name=?", (tariff_name,)) as c:
                count = (await c.fetchone())[0]

    if is_paused:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⛔️ ГРУППА ОСТАНОВЛЕНА", callback_data="worker_paused_alert")]
        ])
    
    if not tariff_name:
         return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⚠️ Не настроено (Жми /startwork)", callback_data="none")]])

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚀 ВЗЯТЬ ({count}) | {tariff_name}", callback_data="worker_take_auto")],
        [InlineKeyboardButton(text=f"🔄 Обновить", callback_data="worker_refresh_auto")]
    ])

def worker_active_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ВСТАЛ", callback_data=f"w_act_{num_id}"), InlineKeyboardButton(text="❌ ОШИБКА / НЕ ВСТАЛ", callback_data=f"w_err_{num_id}")]
    ])
def worker_finish_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 ВЫПЛАТА / СЛЕТ", callback_data=f"w_fin_{num_id}")]
    ])

# Admin Keyboards
def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Управление Тарифами", callback_data="adm_tariffs_menu")],
        [InlineKeyboardButton(text="💵 Отчет ($)", callback_data="adm_report"), InlineKeyboardButton(text="📊 Очередь", callback_data="adm_queue_stats")],
        [InlineKeyboardButton(text="⏰ График", callback_data="adm_schedule"), InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Юзеры", callback_data="adm_users_manage"), InlineKeyboardButton(text="🔙 Выход", callback_data="admin_close")]
    ])

async def admin_tariffs_list_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, price, hold_info FROM tariffs") as c: rows = await c.fetchall()
    kb = []
    for r in rows:
        # ID: Name | Price
        kb.append([InlineKeyboardButton(text=f"{r[1]} | {r[2]} | {r[3]}", callback_data=f"adm_trf_edit_{r[0]}")])
    kb.append([InlineKeyboardButton(text="➕ ДОБАВИТЬ ТАРИФ", callback_data="adm_trf_add")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def admin_tariff_edit_kb(t_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить Название", callback_data=f"trf_act_name_{t_id}")],
        [InlineKeyboardButton(text="💵 Изменить Цену", callback_data=f"trf_act_price_{t_id}")],
        [InlineKeyboardButton(text="⏳ Изменить Холд", callback_data=f"trf_act_hold_{t_id}")],
        [InlineKeyboardButton(text="🗑 УДАЛИТЬ", callback_data=f"trf_act_del_{t_id}")],
        [InlineKeyboardButton(text="🔙 К списку", callback_data="adm_tariffs_menu")]
    ])

def access_request_kb(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{user_id}"), 
         InlineKeyboardButton(text="🚫 Отказать", callback_data=f"acc_no_{user_id}")]
    ])

# --- LOGIC ---

@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id = ?", (user.id,)) as c: res = await c.fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name, is_approved) VALUES (?, ?, ?, 0)", (user.id, user.username, user.first_name))
            await db.commit()
            text = (f"👤 **Запрос доступа!**\nID: `{user.id}`\nUser: @{user.username}\nЧто делаем?")
            try: await message.bot.send_message(ADMIN_ID, text, reply_markup=access_request_kb(user.id), parse_mode="Markdown")
            except: pass
            await message.answer("🔒 **Доступ ограничен.**\nОжидайте подтверждения администратора.")
            return
        is_approved = res[0]
        
    if is_approved:
        await message.answer("👋 **FAST TEAM PLATFORM**\n\n💡 _Совет: Используйте кнопки меню для навигации._", parse_mode="Markdown", reply_markup=main_menu_kb(user.id))
    else:
        await message.answer("⏳ **Ваша заявка на рассмотрении.**")

@router.callback_query(F.data.startswith("acc_"))
async def access_control(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    action, user_id = c.data.split('_')[1], int(c.data.split('_')[2])
    if action == "ok":
        async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE users SET is_approved = 1 WHERE user_id = ?", (user_id,)); await db.commit()
        await c.message.edit_text(f"✅ Доступ выдан ID `{user_id}`")
        try: await bot.send_message(user_id, "✅ **Доступ открыт!**\nЖми /start")
        except: pass
    else:
        await c.message.edit_text(f"🚫 Отказано ID `{user_id}`")

# --- USER FLOW ---
@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("👋 **Главное меню**", reply_markup=main_menu_kb(c.from_user.id), parse_mode="Markdown")

@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    if not await check_work_hours(c.from_user.id):
        await c.answer("💤 Сейчас нерабочее время.", show_alert=True); return
    await c.message.edit_text("💰 **Выберите тариф:**\n\n💡 _Цена и время выплаты указаны на кнопках._", reply_markup=await tariffs_kb_user(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_pick_"))
async def step_method(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split('trf_pick_')[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as c_db: 
            res = await c_db.fetchone()
    
    if not res:
        await c.answer("Тариф удален!", show_alert=True); return
    
    t_price, t_hold = res
    await state.update_data(tariff_name=t_name, tariff_price=t_price, tariff_hold=t_hold)
    
    text = (f"💎 Тариф: **{t_name}**\n"
            f"💵 Цена: **{t_price}**\n"
            f"⏳ Холд (Выплата): **{t_hold}**\n\n"
            f"💡 _Выберите способ передачи номера:_")
    
    await c.message.edit_text(text, reply_markup=method_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(c: CallbackQuery, state: FSMContext):
    method = 'sms' if c.data == "input_sms" else 'qr'
    await state.update_data(method=method)
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT 1 FROM numbers WHERE user_id = ? AND status IN ('queue', 'work', 'active')", (c.from_user.id,)) as cur:
             if await cur.fetchone(): await c.answer("⚠️ У вас уже есть активная заявка!", show_alert=True); return
    
    guide = ("💡 **Как вводить:**\n"
             "- Можно один номер: `+79001234567`\n"
             "- Можно списком через запятую: `+79.., +79..`\n"
             "- Без пробелов и лишних знаков.")
             
    await c.message.edit_text(f"📱 **Введите номер телефона:**\n\n{guide}", reply_markup=cancel_kb(), parse_mode="Markdown")
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    t_name, t_price, t_hold, method = data.get('tariff_name'), data.get('tariff_price'), data.get('tariff_hold'), data.get('method')
    phones = [clean_phone(p.strip()) for p in message.text.split(',')]
    valid_phones = [p for p in phones if p]

    if not valid_phones:
        await message.answer("❌ **Некорректный формат.**\n💡 _Попробуйте еще раз, начиная с +7..._", reply_markup=cancel_kb(), parse_mode="Markdown"); return

    async with aiosqlite.connect(DB_NAME) as db:
        for p in valid_phones:
            async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (p,)) as cur:
                if await cur.fetchone(): await message.answer(f"⚠️ Номер `{p}` уже в работе!", reply_markup=cancel_kb(), parse_mode="Markdown"); return
        
        for p in valid_phones:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                (message.from_user.id, p, method, t_name, t_price, t_hold, 'queue', datetime.utcnow().isoformat()))
        await db.commit()

    await message.answer(f"✅ **Заявка в очереди!**\n📱 `{valid_phones[0]}`\n\n💡 _Не закрывайте бота, вам придет код или QR._", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

@router.callback_query(F.data == "menu_guide")
async def show_guide(c: CallbackQuery):
    text = ("📖 **Мини-Гайд**\n\n"
            "1️⃣ Жми **Сдать номер**.\n"
            "2️⃣ Выбери тариф (смотри цену и холд).\n"
            "3️⃣ Выбери SMS или QR.\n"
            "4️⃣ Введи номер.\n"
            "5️⃣ **Жди!** Воркер возьмет номер и бот попросит код.\n"
            "💡 _Если долго не берут — проверьте расписание._")
    await c.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")

# --- WORKER SECTION ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if message.chat.type not in ['group', 'supergroup']: return
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    
    kb = []
    for r in rows: kb.append([InlineKeyboardButton(text=f"📌 {r[0]}", callback_data=f"set_topic_{r[0]}")])
    kb.append([InlineKeyboardButton(text="⏯ СТОП/СТАРТ ЭТОЙ ГРУППЫ", callback_data="toggle_group_pause")])
    
    await message.answer("⚙️ **Настройка Топика**\n💡 _Какой тариф обрабатываем здесь?_", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("set_topic_"))
async def set_topic(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_topic_")[1]
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_cfg_{chat_id}_{thread_id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name))
        await db.commit()
    await c.message.edit_text(f"✅ Топик привязан к: **{t_name}**", reply_markup=await worker_auto_kb(chat_id, thread_id))

@router.callback_query(F.data == "toggle_group_pause")
async def toggle_group_pause(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_paused_{chat_id}_{thread_id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as cur:
            val = (await cur.fetchone())
            new_val = "1" if not val or val[0] == "0" else "0"
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, new_val))
        await db.commit()
    
    status = "🔴 ОСТАНОВЛЕНО" if new_val == "1" else "🟢 ЗАПУЩЕНО"
    await c.answer(f"Группа: {status}", show_alert=True)
    await c.message.delete()

@router.callback_query(F.data == "worker_take_auto")
async def worker_take(c: CallbackQuery, bot: Bot):
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    
    # Check pause
    is_paused_key = f"topic_paused_{chat_id}_{thread_id}"
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (is_paused_key,)) as cur:
            res = await cur.fetchone()
            if res and res[0] == "1": await c.answer("⛔️ Группа на паузе!", show_alert=True); return

    key = f"topic_cfg_{chat_id}_{thread_id}"
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as cur: 
            t_res = await cur.fetchone()
            t_name = t_res[0] if t_res else None
        
        if not t_name: await c.answer("Ошибка настройки!", show_alert=True); return

        async with db.execute("SELECT id, user_id, phone, method, tariff_price, tariff_hold FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY id ASC LIMIT 1", (t_name,)) as cur:
            row = await cur.fetchone()
        
        if not row: await c.answer("📭 Пусто!", show_alert=True); return
        
        num_id, user_id, phone, method, price, hold = row
        await db.execute("UPDATE numbers SET status = 'work', worker_id = ?, start_time = ? WHERE id = ? AND status = 'queue'", (c.from_user.id, datetime.utcnow().isoformat(), num_id))
        if db.rowcount == 0: await c.answer("⚠️ Уже забрали!"); return
        await db.commit()

    m_icon = "📷 QR" if method == 'qr' else "💬 SMS"
    text = (f"🚀 **В РАБОТЕ**\n"
            f"📱 `{phone}`\n"
            f"💰 {t_name} | {price}\n"
            f"⏳ {hold}\n"
            f"{m_icon}\n\n"
            f"💡 _Нажми на номер чтобы скопировать._\n"
            f"👇 _Команда для отправки кода:_ \n`/sms {phone} код`")
            
    msg = await c.message.edit_text(text, parse_mode="Markdown", reply_markup=worker_active_kb(num_id))
    async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (msg.message_id, num_id)); await db.commit()
    try: await bot.send_message(user_id, f"⚡️ Воркер принял номер `{phone}`. Ожидайте SMS/QR!")
    except: pass

@router.callback_query(F.data == "worker_refresh_auto")
async def worker_ref(c: CallbackQuery):
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    try: await c.message.edit_reply_markup(reply_markup=await worker_auto_kb(chat_id, thread_id))
    except: pass
    await c.answer()

@router.callback_query(F.data.startswith("w_act_"))
async def worker_act(c: CallbackQuery, bot: Bot):
    num_id = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = 'active' WHERE id = ?", (num_id,))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as cur: 
            res = await cur.fetchone()
            p, u = res if res else (None, None)
        await db.commit()
    await c.message.edit_text(f"🟢 **АКТИВЕН**\n📱 `{p}`\n💡 _Номер подтвержден. Не забудь выплатить!_", reply_markup=worker_finish_kb(num_id), parse_mode="Markdown")
    try: await bot.send_message(u, f"✅ Номер `{p}` успешно активирован! Холд пошел.")
    except: pass

@router.callback_query(F.data.startswith("w_fin_") | F.data.startswith("w_err_"))
async def worker_fin(c: CallbackQuery, bot: Bot):
    status = "finished" if "w_fin_" in c.data else "dead"
    num_id = c.data.split('_')[2]
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", (status, datetime.utcnow().isoformat(), num_id))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as cur: p, u = await cur.fetchone()
        await db.commit()

    await c.message.edit_text(f"🏁 Завершен: `{p}`", reply_markup=await worker_auto_kb(chat_id, thread_id))
    msg = "💰 Выплата оформлена!" if status == "finished" else "❌ Отмена / Ошибка заявки."
    try: await bot.send_message(u, f"{msg}\n📱 `{p}`")
    except: pass

# --- ADMIN PANEL (TARIFFS) ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("⚡️ **ADMIN PANEL**", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_tariffs_menu")
async def adm_trf_menu(c: CallbackQuery):
    await c.message.edit_text("💰 **Управление Тарифами**\n\n💡 _Нажми на тариф, чтобы изменить или удалить._", reply_markup=await admin_tariffs_list_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_trf_add")
async def adm_trf_add(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("➕ **Добавление Тарифа**\n\nВведите НАЗВАНИЕ тарифа (например: `KZ WhatsApp`):", parse_mode="Markdown")
    await state.set_state(AdminState.trf_adding_name)

@router.message(AdminState.trf_adding_name)
async def adm_trf_save_name(m: types.Message, state: FSMContext):
    await state.update_data(name=m.text)
    await m.answer("💵 Введите ЦЕНУ (например: `4$`):")
    await state.set_state(AdminState.trf_adding_price)

@router.message(AdminState.trf_adding_price)
async def adm_trf_save_price(m: types.Message, state: FSMContext):
    await state.update_data(price=m.text)
    await m.answer("⏳ Введите время ХОЛДА (например: `20 мин`):")
    await state.set_state(AdminState.trf_adding_hold)

@router.message(AdminState.trf_adding_hold)
async def adm_trf_save_hold(m: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute("INSERT INTO tariffs (name, price, hold_info) VALUES (?, ?, ?)", (d['name'], d['price'], m.text))
            await db.commit()
            await m.answer(f"✅ Тариф **{d['name']}** создан!", reply_markup=admin_kb(), parse_mode="Markdown")
        except:
            await m.answer("❌ Ошибка. Такое имя уже есть?", reply_markup=admin_kb())
    await state.clear()

@router.callback_query(F.data.startswith("adm_trf_edit_"))
async def adm_trf_view(c: CallbackQuery):
    t_id = c.data.split("_")[3]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, price, hold_info FROM tariffs WHERE id=?", (t_id,)) as cur: res = await cur.fetchone()
    if not res: await c.answer("Не найдено", show_alert=True); return
    
    text = (f"⚙️ **Редактирование Тарифа**\n\n"
            f"🏷 Имя: **{res[0]}**\n"
            f"💵 Цена: **{res[1]}**\n"
            f"⏳ Холд: **{res[2]}**")
    await c.message.edit_text(text, reply_markup=admin_tariff_edit_kb(t_id), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_act_"))
async def adm_trf_action(c: CallbackQuery, state: FSMContext):
    parts = c.data.split('_')
    act, t_id = parts[2], parts[3]
    
    if act == "del":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("DELETE FROM tariffs WHERE id=?", (t_id,))
            await db.commit()
        await c.answer("🗑 Тариф удален!")
        await adm_trf_menu(c)
        return

    await state.update_data(t_id=t_id, act=act)
    prompts = {"name": "новое НАЗВАНИЕ", "price": "новую ЦЕНУ", "hold": "новый ХОЛД"}
    await c.message.edit_text(f"✏️ Введите {prompts[act]}:")
    await state.set_state(AdminState.trf_editing_value)

@router.message(AdminState.trf_editing_value)
async def adm_trf_save_edit(m: types.Message, state: FSMContext):
    d = await state.get_data()
    cols = {"name": "name", "price": "price", "hold": "hold_info"}
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f"UPDATE tariffs SET {cols[d['act']]} = ? WHERE id = ?", (m.text, d['t_id']))
        await db.commit()
    
    await m.answer("✅ Обновлено!", reply_markup=admin_kb())
    await state.clear()

@router.callback_query(F.data == "admin_close")
async def adm_cls(c: CallbackQuery):
    await c.message.delete()
    await c.message.answer("👋", reply_markup=main_menu_kb(c.from_user.id))

@router.callback_query(F.data == "adm_report")
async def adm_report(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    ts = datetime.combine(date.today(), datetime.min.time()).isoformat()
    total_money = 0.0
    text_report = f"📅 **ОТЧЕТ ЗА {date.today()}**\n\n"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, tariff_price FROM numbers WHERE status='finished' AND end_time >= ?", (ts,)) as cur:
            rows = await cur.fetchall()
            
    if not rows: await c.message.edit_text("📂 Сегодня пусто.", reply_markup=admin_kb()); return

    for r in rows:
        price_val = extract_price(r[1])
        total_money += price_val
        text_report += f"✅ `{r[0]}` | {r[1]}\n"
        
    text_report += f"\n💵 **ИТОГО: {total_money}$**"
    
    if len(text_report) > 4000:
        f = BufferedInputFile(text_report.encode(), filename="report.txt")
        await c.message.answer_document(f, caption=f"💵 ИТОГО: {total_money}$")
    else:
        await c.message.edit_text(text_report, reply_markup=admin_kb(), parse_mode="Markdown")

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

async def main():
    if not TOKEN or not ADMIN_ID: print("❌ NO ENV VARS"); return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    print("🚀 FAST TEAM v21.0 STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
