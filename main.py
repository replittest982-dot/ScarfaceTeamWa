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
DB_NAME = "fast_team_v22.db" # База та же, структура подходит
MSK_OFFSET = 3 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
router = Router()

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    trf_adding_name = State()
    trf_adding_price = State()
    trf_adding_hold = State()
    trf_adding_start = State()
    trf_adding_end = State()
    trf_editing_value = State()

# --- DATABASE ENGINE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, method TEXT, 
            tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, status TEXT, worker_id INTEGER, 
            start_time TIMESTAMP, end_time TIMESTAMP, last_ping TIMESTAMP, 
            is_check_pending INTEGER DEFAULT 0, worker_msg_id INTEGER, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, price TEXT, hold_info TEXT,
            work_start TEXT DEFAULT '00:00', work_end TEXT DEFAULT '23:59'
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        await db.commit()

# --- UTILS ---
def get_msk_time(): 
    return datetime.utcnow() + timedelta(hours=MSK_OFFSET)

def extract_price(price_str):
    match = re.search(r"(\d+(\.\d+)?)", str(price_str))
    return float(match.group(1)) if match else 0.0

async def check_tariff_hours(tariff_name):
    if not tariff_name: return False
    now_msk = get_msk_time().time()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT work_start, work_end FROM tariffs WHERE name=?", (tariff_name,)) as c:
            res = await c.fetchone()
    if not res: return True
    s_str, e_str = res
    try:
        st = datetime.strptime(s_str, "%H:%M").time()
        et = datetime.strptime(e_str, "%H:%M").time()
        if st <= et: return st <= now_msk <= et
        else: return st <= now_msk or now_msk <= et
    except: return True

def clean_phone(phone: str):
    clean = re.sub(r'[^\d+]', '', phone)
    if clean.startswith('8') and len(clean) == 11: clean = '+7' + clean[1:]
    elif clean.startswith('7') and len(clean) == 11: clean = '+' + clean
    elif len(clean) == 10 and clean.isdigit(): clean = '+7' + clean
    if not re.match(r'^\+\d{10,15}$', clean): return None
    return clean

# --- KEYBOARDS ---
def main_menu_kb(user_id: int):
    kb = [[InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
          [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]]
    if user_id == ADMIN_ID: kb.append([InlineKeyboardButton(text="⚡️ ADMIN PANEL", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def tariffs_kb_user():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, price FROM tariffs") as c: rows = await c.fetchall()
    kb = []
    for i in range(0, len(rows), 2):
        row = [InlineKeyboardButton(text=f"{rows[i][0]} | {rows[i][1]}", callback_data=f"trf_pick_{rows[i][0]}")]
        if i+1 < len(rows): row.append(InlineKeyboardButton(text=f"{rows[i+1][0]} | {rows[i+1][1]}", callback_data=f"trf_pick_{rows[i+1][0]}"))
        kb.append(row)
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]])
def cancel_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])
def method_select_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 SMS Код", callback_data="input_sms"), InlineKeyboardButton(text="📷 QR Код", callback_data="input_qr")], [InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]])

def worker_finish_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 ВЫПЛАТА", callback_data=f"w_fin_{num_id}"), InlineKeyboardButton(text="📉 СЛЕТ", callback_data=f"w_drop_{num_id}")],
        [InlineKeyboardButton(text="❌ ОШИБКА", callback_data=f"w_err_{num_id}")]
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Тарифы", callback_data="adm_tariffs_menu")],
        [InlineKeyboardButton(text="📊 Очередь", callback_data="adm_queue_stats"), InlineKeyboardButton(text="💵 Отчет ($)", callback_data="adm_report")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"), InlineKeyboardButton(text="🔙 Выход", callback_data="admin_close")]
    ])

async def admin_tariffs_list_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, price, work_start, work_end FROM tariffs") as c: rows = await c.fetchall()
    kb = []
    for r in rows: kb.append([InlineKeyboardButton(text=f"{r[1]} ({r[2]}) [{r[3]}-{r[4]}]", callback_data=f"adm_trf_edit_{r[0]}")])
    kb.append([InlineKeyboardButton(text="➕ ДОБАВИТЬ ТАРИФ", callback_data="adm_trf_add")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def admin_tariff_edit_kb(t_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Название", callback_data=f"trf_act_name_{t_id}"), InlineKeyboardButton(text="💵 Цена", callback_data=f"trf_act_price_{t_id}")],
        [InlineKeyboardButton(text="⏳ Холд", callback_data=f"trf_act_hold_{t_id}"), InlineKeyboardButton(text="⏰ График", callback_data=f"trf_act_time_{t_id}")],
        [InlineKeyboardButton(text="🗑 УДАЛИТЬ", callback_data=f"trf_act_del_{t_id}")],
        [InlineKeyboardButton(text="🔙 К списку", callback_data="adm_tariffs_menu")]
    ])

def access_request_kb(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{user_id}"), InlineKeyboardButton(text="🚫 Отказать", callback_data=f"acc_no_{user_id}")]
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
            await message.answer("🔒 **Доступ ограничен.**\nОжидайте подтверждения."); return
        is_approved = res[0]
    if is_approved: await message.answer("👋 **FAST TEAM**\nМеню:", reply_markup=main_menu_kb(user.id))
    else: await message.answer("⏳ **На рассмотрении.**")

@router.callback_query(F.data.startswith("acc_"))
async def access_control(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    action, user_id = c.data.split('_')[1], int(c.data.split('_')[2])
    if action == "ok":
        async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE users SET is_approved = 1 WHERE user_id = ?", (user_id,)); await db.commit()
        await c.message.edit_text(f"✅ Доступ выдан ID `{user_id}`")
        try: await bot.send_message(user_id, "✅ **Доступ открыт!**\nЖми /start")
        except: pass
    else: await c.message.edit_text(f"🚫 Отказано ID `{user_id}`")

# --- USER SIDE ---
@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("👋 **Главное меню**", reply_markup=main_menu_kb(c.from_user.id), parse_mode="Markdown")

@router.callback_query(F.data == "menu_profile")
async def show_profile(c: CallbackQuery):
    user_id = c.from_user.id
    today_iso = datetime.combine(date.today(), datetime.min.time()).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (user_id,)) as cur: 
            d = await cur.fetchone()
            reg_date = d[0].split(' ')[0] if d else "?"
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND created_at >= ?", (user_id, today_iso)) as cur: today_count = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ?", (user_id,)) as cur: total_count = (await cur.fetchone())[0]
    text = (f"👤 **Мой Профиль**\n🆔 ID: `{user_id}`\n📅 Регистрация: {reg_date}\n🔥 **Сдано за сегодня:** {today_count} шт.\n📦 Всего сдано: {total_count} шт.")
    await c.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    await c.message.edit_text("💰 **Выберите тариф:**", reply_markup=await tariffs_kb_user(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_pick_"))
async def step_method(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split('trf_pick_')[1]
    if not await check_tariff_hours(t_name): await c.answer(f"💤 Тариф {t_name} не работает!", show_alert=True); return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as c_db: res = await c_db.fetchone()
    if not res: await c.answer("Тариф удален!", show_alert=True); return
    t_price, t_hold = res
    await state.update_data(tariff_name=t_name, tariff_price=t_price, tariff_hold=t_hold)
    await c.message.edit_text(f"💎 Тариф: **{t_name}**\n💵 Цена: **{t_price}**\n⏳ Холд: **{t_hold}**\n\n👇 Выберите способ:", reply_markup=method_select_kb(), parse_mode="Markdown")

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(c: CallbackQuery, state: FSMContext):
    method = 'sms' if c.data == "input_sms" else 'qr'
    await state.update_data(method=method)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT 1 FROM numbers WHERE user_id = ? AND status IN ('queue', 'work', 'active')", (c.from_user.id,)) as cur:
             if await cur.fetchone(): await c.answer("⚠️ Уже есть активная заявка!", show_alert=True); return
    await c.message.edit_text(f"📱 **Введите номер (или список):**\n`+79...`", reply_markup=cancel_kb(), parse_mode="Markdown")
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    t_name, t_price, t_hold, method = data.get('tariff_name'), data.get('tariff_price'), data.get('tariff_hold'), data.get('method')
    phones = [clean_phone(p.strip()) for p in message.text.split(',')]
    valid_phones = [p for p in phones if p]
    if not valid_phones: await message.answer("❌ Некорректный номер.", reply_markup=cancel_kb()); return
    async with aiosqlite.connect(DB_NAME) as db:
        for p in valid_phones:
            async with db.execute("SELECT 1 FROM numbers WHERE phone = ? AND status IN ('queue', 'work', 'active')", (p,)) as cur:
                if await cur.fetchone(): await message.answer(f"⚠️ Номер `{p}` уже в работе!", reply_markup=cancel_kb(), parse_mode="Markdown"); return
        for p in valid_phones:
            await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                (message.from_user.id, p, method, t_name, t_price, t_hold, 'queue', datetime.utcnow().isoformat()))
        await db.commit()
    await message.answer(f"✅ **В очереди!**\n📱 `{valid_phones[0]}`\nОжидайте.", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

@router.callback_query(F.data == "menu_guide")
async def show_guide(c: CallbackQuery):
    await c.message.edit_text("ℹ️ **Помощь**\nСдавай номера кнопкой 'Сдать номер'.", reply_markup=back_kb())

# --- WORKER SECTION ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if message.chat.type not in ['group', 'supergroup']: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    kb = []
    for r in rows: kb.append([InlineKeyboardButton(text=f"📌 {r[0]}", callback_data=f"set_topic_{r[0]}")])
    await message.answer("⚙️ **Привязка Топика**\nВыберите тариф:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.message(Command("stopwork"))
async def worker_stop(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    chat_id = message.chat.id
    thread_id = message.message_thread_id if message.is_topic_message else 0
    key = f"topic_cfg_{chat_id}_{thread_id}"
    async with aiosqlite.connect(DB_NAME) as db: await db.execute("DELETE FROM config WHERE key=?", (key,)); await db.commit()
    await message.answer("🛑 **Топик отвязан!**")

@router.callback_query(F.data.startswith("set_topic_"))
async def set_topic(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_topic_")[1]
    chat_id = c.message.chat.id
    thread_id = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_cfg_{chat_id}_{thread_id}"
    async with aiosqlite.connect(DB_NAME) as db: await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name)); await db.commit()
    
    # ГАЙД ПРИ СТАРТЕ
    guide_text = (f"✅ Топик привязан к: **{t_name}**\n\n"
                  f"📋 **ИНСТРУКЦИЯ:**\n"
                  f"1️⃣ Чтобы взять номер, пиши: **/num**\n"
                  f"2️⃣ Чтобы отправить код: **/sms номер код**\n"
                  f"3️⃣ Управление номером доступно только взявшему!")
    await c.message.edit_text(guide_text, parse_mode="Markdown")

# --- ИСПРАВЛЕННЫЙ ХЭНДЛЕР /NUM ---
@router.message(Command("num"))
async def cmd_num(message: types.Message, bot: Bot):
    chat_id = message.chat.id
    thread_id = message.message_thread_id if message.is_topic_message else 0
    key = f"topic_cfg_{chat_id}_{thread_id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (key,)) as cur: 
            t_res = await cur.fetchone()
        
        if not t_res: return

        # Берем номер
        async with db.execute("SELECT id, user_id, phone, method, tariff_price, tariff_hold FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY id ASC LIMIT 1", (t_res[0],)) as cur:
            row = await cur.fetchone()
        
        if not row:
            await message.reply("📭 Очередь пуста!"); return
        
        num_id, user_id, phone, method, price, hold = row
        
        # --- ФИКС ОШИБКИ ЗДЕСЬ ---
        # Сначала выполняем запрос и получаем курсор
        cursor = await db.execute("UPDATE numbers SET status = 'work', worker_id = ?, start_time = ? WHERE id = ? AND status = 'queue'", 
                                 (message.from_user.id, datetime.utcnow().isoformat(), num_id))
        
        # Проверяем rowcount у курсора, а не у db
        if cursor.rowcount == 0: 
            await message.reply("⚠️ Кто-то успел взять раньше!")
            return
            
        await db.commit()

    m_icon = "📷 QR" if method == 'qr' else "💬 SMS"
    text = (f"🚀 **ВЗЯТ В РАБОТУ!**\n"
            f"📱 `{phone}`\n"
            f"💰 {t_res[0]} | {price}\n"
            f"⏳ {hold} | {m_icon}\n\n"
            f"👇 **Код сюда:**\n`/sms {phone} код`")
            
    msg = await message.answer(text, parse_mode="Markdown", reply_markup=worker_finish_kb(num_id))
    async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE numbers SET worker_msg_id = ? WHERE id = ?", (msg.message_id, num_id)); await db.commit()
    try: await bot.send_message(user_id, f"⚡️ Воркер принял номер `{phone}`. Ждите код.")
    except: pass

@router.callback_query(F.data.startswith("w_fin_") | F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def worker_fin_secure(c: CallbackQuery, bot: Bot):
    num_id = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT worker_id FROM numbers WHERE id = ?", (num_id,)) as cur: 
            res = await cur.fetchone()
            owner_id = res[0] if res else None

    if owner_id != c.from_user.id and c.from_user.id != ADMIN_ID:
        await c.answer("🚫 Это чужой номер!", show_alert=True); return

    if "w_fin_" in c.data: s, m = "finished", "💰 Выплата начислена!"
    elif "w_drop_" in c.data: s, m = "drop", "📉 Номер слетел."
    else: s, m = "dead", "❌ Ошибка."
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status = ?, end_time = ? WHERE id = ?", (s, datetime.utcnow().isoformat(), num_id))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (num_id,)) as cur: p, u = await cur.fetchone()
        await db.commit()

    await c.message.edit_text(f"🏁 Финал [{s}]: `{p}`\n👤 Воркер: {c.from_user.first_name}")
    try: await bot.send_message(u, f"{m}\n📱 `{p}`")
    except: pass

# --- SMS HANDLER ---
@router.message(Command("sms"))
async def sms_h(m: types.Message, cmd: CommandObject, bot: Bot):
    if not cmd.args: return
    try:
        ph, tx = cmd.args.split(' ', 1)
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id, worker_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: r = await cur.fetchone()
        if r and (r[1] == m.from_user.id or m.from_user.id == ADMIN_ID):
            await bot.send_message(r[0], f"🔔 **SMS/Код**\n📱 `{ph}`\n💬 `{tx}`", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else: await m.reply("🚫 Ошибка доступа.")
    except: pass

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("⚡️ **ADMIN PANEL**", reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_queue_stats")
async def adm_queue_s(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT tariff_name, COUNT(*) FROM numbers WHERE status='queue' GROUP BY tariff_name") as cur: stats = await cur.fetchall()
    text = "📊 **Очередь:**\n\n" + ("".join([f"🔹 {t}: **{count}** шт.\n" for t, count in stats]) if stats else "📭 Пусто.")
    await c.message.edit_text(text, reply_markup=admin_kb(), parse_mode="Markdown")

@router.callback_query(F.data == "admin_broadcast")
async def adm_br_start(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📢 **Рассылка**\nВведи текст/фото:")
    await state.set_state(AdminState.waiting_for_broadcast)

@router.message(AdminState.waiting_for_broadcast)
async def adm_br_send(msg: types.Message, state: FSMContext):
    await state.clear()
    status_msg = await msg.answer("⏳ Рассылаю...")
    count, errs = 0, 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cur: users = await cur.fetchall()
    for u in users:
        try: await msg.copy_to(u[0]); count += 1; await asyncio.sleep(0.05)
        except: errs += 1
    await status_msg.edit_text(f"✅ Итог: {count} доставлено, {errs} ошибок.", reply_markup=admin_kb())

@router.callback_query(F.data == "adm_report")
async def adm_report(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    ts = datetime.combine(date.today(), datetime.min.time()).isoformat()
    total = 0.0
    text = f"📅 **ОТЧЕТ ({date.today()})**\n\n"
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, tariff_price FROM numbers WHERE status='finished' AND end_time >= ?", (ts,)) as cur: rows = await cur.fetchall()
    if not rows: await c.message.edit_text("📂 Пусто.", reply_markup=admin_kb()); return
    for r in rows:
        total += extract_price(r[1])
        text += f"✅ `{r[0]}` | {r[1]}\n"
    text += f"\n💵 **ИТОГО: {total}$**"
    if len(text) > 4000:
        f = BufferedInputFile(text.encode(), filename="report.txt")
        await c.message.answer_document(f, caption=f"💵 {total}$")
    else: await c.message.edit_text(text, reply_markup=admin_kb(), parse_mode="Markdown")

# --- TARIFFS ---
@router.callback_query(F.data == "adm_tariffs_menu")
async def adm_trf_menu(c: CallbackQuery):
    await c.message.edit_text("💰 **Тарифы**", reply_markup=await admin_tariffs_list_kb())

@router.callback_query(F.data == "adm_trf_add")
async def adm_trf_add(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("➕ Название (ex: `RU WA`):"); await state.set_state(AdminState.trf_adding_name)
@router.message(AdminState.trf_adding_name)
async def adm_trf_1(m: types.Message, state: FSMContext):
    await state.update_data(name=m.text); await m.answer("💵 Цена (ex: `4$`):"); await state.set_state(AdminState.trf_adding_price)
@router.message(AdminState.trf_adding_price)
async def adm_trf_2(m: types.Message, state: FSMContext):
    await state.update_data(price=m.text); await m.answer("⏳ Холд (ex: `20 мин`):"); await state.set_state(AdminState.trf_adding_hold)
@router.message(AdminState.trf_adding_hold)
async def adm_trf_3(m: types.Message, state: FSMContext):
    await state.update_data(hold=m.text); await m.answer("⏰ Старт (ex: `07:00`):"); await state.set_state(AdminState.trf_adding_start)
@router.message(AdminState.trf_adding_start)
async def adm_trf_4(m: types.Message, state: FSMContext):
    await state.update_data(start=m.text); await m.answer("⏰ Конец (ex: `23:00`):"); await state.set_state(AdminState.trf_adding_end)
@router.message(AdminState.trf_adding_end)
async def adm_trf_5(m: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO tariffs (name, price, hold_info, work_start, work_end) VALUES (?, ?, ?, ?, ?)", (d['name'], d['price'], d['hold'], d['start'], m.text)); await db.commit()
    await m.answer("✅ Создано!", reply_markup=admin_kb()); await state.clear()

@router.callback_query(F.data.startswith("adm_trf_edit_"))
async def adm_trf_v(c: CallbackQuery):
    t_id = c.data.split("_")[3]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, price, hold_info, work_start, work_end FROM tariffs WHERE id=?", (t_id,)) as cur: r = await cur.fetchone()
    text = f"⚙️ **{r[0]}**\n💵 {r[1]}\n⏳ {r[2]}\n⏰ {r[3]}-{r[4]}"
    await c.message.edit_text(text, reply_markup=admin_tariff_edit_kb(t_id), parse_mode="Markdown")

@router.callback_query(F.data.startswith("trf_act_"))
async def adm_trf_act(c: CallbackQuery, state: FSMContext):
    act, t_id = c.data.split('_')[2], c.data.split('_')[3]
    if act == "del":
        async with aiosqlite.connect(DB_NAME) as db: await db.execute("DELETE FROM tariffs WHERE id=?", (t_id,)); await db.commit()
        await c.answer("Удалено!"); await adm_trf_menu(c); return
    await state.update_data(t_id=t_id, act=act)
    msg = "Новое время (09:00-21:00):" if act == "time" else f"Новое {act}:"
    await c.message.edit_text(msg); await state.set_state(AdminState.trf_editing_value)

@router.message(AdminState.trf_editing_value)
async def adm_trf_sv(m: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        if d['act'] == 'time': s, e = m.text.split('-'); await db.execute("UPDATE tariffs SET work_start=?, work_end=? WHERE id=?", (s, e, d['t_id']))
        else: 
            col = {'name': 'name', 'price': 'price', 'hold': 'hold_info'}[d['act']]
            await db.execute(f"UPDATE tariffs SET {col}=? WHERE id=?", (m.text, d['t_id']))
        await db.commit()
    await m.answer("✅", reply_markup=admin_kb()); await state.clear()

@router.callback_query(F.data == "admin_close")
async def adm_cls(c: CallbackQuery): await c.message.delete()

async def main():
    if not TOKEN: print("❌ TOKEN?"); return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    print("🚀 v23.2 FINAL FIX STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
