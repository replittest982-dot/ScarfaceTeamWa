import asyncio
import logging
import sys
import os
import re
import csv
import io
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

try:
    import aiosqlite
    from aiogram import Bot, Dispatcher, Router, F
    from aiogram.filters import Command, CommandStart, CommandObject
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import (
        InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, 
        Message, ReactionTypeEmoji, BufferedInputFile
    )
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
except ImportError:
    sys.exit("❌ pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "bot_mega_v30.db"

# Таймеры (в минутах)
AFK_CHECK_MINUTES = 8   
AFK_KICK_MINUTES = 3    
CODE_WAIT_MINUTES = 4   

SEP = "━━━━━━━━━━━━━━━━━━━━"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

if not TOKEN or "YOUR_TOKEN" in TOKEN:
    sys.exit("❌ FATAL: BOT_TOKEN не указан!")

# ==========================================
# БАЗА ДАННЫХ
# ==========================================
@asynccontextmanager
async def get_db():
    conn = await aiosqlite.connect(DB_NAME, timeout=30)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    try: yield conn
    finally: await conn.close()

async def init_db():
    async with get_db() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
                is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0, 
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
                tariff_name TEXT, tariff_price TEXT, work_time TEXT, 
                status TEXT DEFAULT 'queue', 
                worker_id INTEGER DEFAULT 0, worker_chat_id INTEGER DEFAULT 0, worker_thread_id INTEGER DEFAULT 0, 
                start_time TEXT, end_time TEXT, last_ping TEXT, wait_code_start TEXT, 
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (name TEXT PRIMARY KEY, price TEXT, work_time TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS groups (group_num INTEGER PRIMARY KEY, chat_id INTEGER, title TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        # Дефолт
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('WhatsApp','50₽','10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('MAX','10$','24/7')")
        await db.commit()
    logger.info("✅ Database initialized (Mega V30.0)")

# ==========================================
# УТИЛИТЫ
# ==========================================
def clean_phone(phone):
    if not phone: return None
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    return '+' + clean if re.match(r'^7\d{10}$', clean) else None

def mask_phone(phone, user_id):
    if user_id == ADMIN_ID: return phone
    try: return f"{phone[:5]}***{phone[-4:]}" if len(phone) > 9 else phone
    except: return phone

def get_now(): return datetime.now(timezone.utc).isoformat()

def format_time(iso_str):
    try: return (datetime.fromisoformat(iso_str) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
    except: return "-"

def calc_duration(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: return "0 мин"

# ==========================================
# FSM & КЛАВИАТУРЫ
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_help = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_price = State()
    edit_time = State()
    help_reply = State()
    # Новые стейты для отчета
    report_wait_date = State() 
    report_wait_hour = State()

def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Поддержка", callback_data="ask_help")
    if user_id == ADMIN_ID: kb.button(text="⚡ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 1, 1)
    return kb.as_markup()

def worker_kb_whatsapp(nid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
    kb.button(text="❌ Ошибка", callback_data=f"w_err_{nid}")
    return kb.as_markup()

def worker_kb_max(nid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
    kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{nid}")
    return kb.as_markup()

def worker_active_kb(nid):
    return InlineKeyboardBuilder().button(text="📉 Слет", callback_data=f"w_drop_{nid}").as_markup()

# ==========================================
# БАЗОВЫЕ КОМАНДЫ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            if ADMIN_ID:
                try: await m.bot.send_message(ADMIN_ID, f"👤 Новый: {uid} (@{m.from_user.username})", 
                                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")]]))
                except: pass
            return await m.answer("🔒 Доступ ограничен. Ждите одобрения.")
        
        if res['is_banned']: return await m.answer("🚫 Вы заблокированы.")
        if res['is_approved']: await m.answer(f"👋 Привет, {m.from_user.first_name}!\n{SEP}", reply_markup=main_kb(uid))
        else: await m.answer("⏳ Заявка на рассмотрении.")

@router.message(Command("bindgroup"))
async def cmd_bindgroup(m: Message, command: CommandObject):
    if m.from_user.id != ADMIN_ID: return
    try:
        group_num = int(command.args.strip())
        if group_num not in [1, 2, 3]: raise ValueError
    except: return await m.reply("❌ Формат: /bindgroup 1 (или 2, 3)")
    
    chat_id = m.chat.id
    title = m.chat.title or f"Chat {chat_id}"
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO groups (group_num, chat_id, title) VALUES (?, ?, ?)", (group_num, chat_id, title))
        await db.commit()
    await m.answer(f"✅ Группа {group_num} привязана к этому чату!\nID: {chat_id}")

@router.message(Command("startwork"))
async def cmd_startwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db: tariffs = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in tariffs: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    kb.adjust(1)
    await m.answer("⚙️ Настройка воркера\nВыберите тариф для этого топика:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    tn = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", tn))
        await db.commit()
    await c.message.edit_text(f"✅ Топик привязан к тарифу: {tn}")

# ==========================================
# РАБОТА С НОМЕРАМИ (Воркер)
# ==========================================
@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply("❌ Топик не настроен (/startwork)")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Очередь пуста")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?", 
                         (m.from_user.id, cid, tid, get_now(), row['id']))
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), row['user_id'])) # Reset AFK for user
        await db.commit()
    
    msg = f"🚀 Взят номер\n{SEP}\n📱 {row['phone']}\n💰 {row['tariff_price']}\n"
    if "MAX" in conf['value'].upper():
        msg += "Используй /code для запроса"
        kb = worker_kb_max(row['id'])
    else:
        msg += "SMS: /sms текст"
        kb = worker_kb_whatsapp(row['id'])
    
    await m.answer(msg, reply_markup=kb)
    try: await bot.send_message(row['user_id'], f"⚡ Ваш номер {mask_phone(row['phone'], 0)} взят в работу!")
    except: pass

@router.message(Command("code"))
async def cmd_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Пример: /code +7999...")
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер")
    
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now(), row['id']))
        await db.commit()
    
    try:
        await bot.send_message(row['user_id'], f"🔔 <b>ЗАПРОС КОДА</b>\n{SEP}\nДля номера: {mask_phone(row['phone'], 0)}\nОтправьте код сюда:", parse_mode="HTML")
        await m.reply("✅ Запрос отправлен")
    except: await m.reply("❌ Не доставлено")

# ==========================================
# ЛИЧНЫЙ КАБИНЕТ И МЕНЮ
# ==========================================
@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='active'", (uid,))).fetchone())[0]
    
    await c.message.edit_text(f"👤 Кабинет\n{SEP}\n🆔: {uid}\n📦 Сдано: {total}\n🔥 Активно: {active}", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]]))

@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db: tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in tariffs: kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    kb.button(text="🔙", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("📂 Выберите тариф:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    async with get_db() as db: t = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (tn,))).fetchone()
    await state.update_data(tariff=tn, price=t['price'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)
    await c.message.edit_text(f"💎 Тариф: {tn}\n💰 Цена: {t['price']}\n⏰ Время: {t['work_time']}\n\n👇 Пришлите номера:", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back_main")]]))

@router.message(UserState.waiting_numbers)
async def fsm_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = [clean_phone(x.strip()) for x in raw if clean_phone(x.strip())]
    if not valid: return await m.reply("❌ Нет валидных номеров")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute("INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, work_time, last_ping) VALUES (?, ?, ?, ?, ?, ?)", 
                             (m.from_user.id, ph, data['tariff'], data['price'], data['work_time'], get_now()))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ Принято {len(valid)} номеров!", reply_markup=main_kb(m.from_user.id))

# ==========================================
# АДМИН ПАНЕЛЬ + НОВЫЕ ОТЧЕТЫ
# ==========================================
@router.callback_query(F.data == "admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Отчеты (NEW)", callback_data="adm_reports")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🏢 Группы", callback_data="manage_groups")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡ Админ панель", reply_markup=kb.as_markup())

# --- ЛОГИКА ОТЧЕТОВ ---
@router.callback_query(F.data == "adm_reports")
async def cb_adm_reports(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    
    # 1. Генерируем кнопки с датами (Сегодня + 6 дней назад)
    kb = InlineKeyboardBuilder()
    now = datetime.now()
    for i in range(7):
        d = now - timedelta(days=i)
        d_str = d.strftime("%Y-%m-%d") # Формат 2026-01-15
        kb.button(text=d_str, callback_data=f"rep_date_{d_str}")
    
    kb.button(text="🔙 Отмена", callback_data="admin_main")
    kb.adjust(2) # По 2 в ряд
    await c.message.edit_text("📅 Выберите дату отчета:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_date_"))
async def cb_rep_select_hour(c: CallbackQuery, state: FSMContext):
    date_str = c.data.split("_")[2]
    await state.update_data(rep_date=date_str)
    
    # 2. Генерируем кнопки с часами (00 - 23)
    kb = InlineKeyboardBuilder()
    for h in range(24):
        h_str = f"{h:02d}"
        kb.button(text=f"{h_str}:00", callback_data=f"rep_hour_{h_str}")
    
    kb.button(text="🔙 Назад", callback_data="adm_reports")
    kb.adjust(4) # По 4 в ряд
    await c.message.edit_text(f"📅 Дата: {date_str}\n🕒 Выберите ЧАС:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_hour_"))
async def cb_rep_generate(c: CallbackQuery, state: FSMContext):
    hour_str = c.data.split("_")[2]
    data = await state.get_data()
    date_str = data['rep_date'] # 2026-01-15
    
    # Формируем диапазон времени
    start_dt_str = f"{date_str}T{hour_str}:00:00"
    end_dt_str = f"{date_str}T{hour_str}:59:59"
    
    # Учитываем что в базе UTC (isoformat), а запрос может быть локальным
    # Для упрощения ищем по строковому вхождению или простому сравнению строк ISO
    # (ISO формат отлично сортируется и сравнивается как строки)
    
    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT id, user_id, phone, status, tariff_name, created_at, start_time, end_time 
            FROM numbers 
            WHERE created_at >= ? AND created_at <= ?
            ORDER BY id ASC
        """, (start_dt_str, end_dt_str))).fetchall()
        
    if not rows:
        return await c.answer("📂 За этот час нет данных", show_alert=True)
        
    # Генерируем CSV
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'User', 'Phone', 'Status', 'Tariff', 'Created', 'Start', 'End'])
    
    for r in rows:
        w.writerow([
            r['id'], r['user_id'], r['phone'], r['status'], r['tariff_name'],
            format_time(r['created_at']), format_time(r['start_time']), format_time(r['end_time'])
        ])
        
    out.seek(0)
    filename = f"report_{date_str}_{hour_str}h.csv"
    await c.message.answer_document(
        BufferedInputFile(out.getvalue().encode(), filename=filename),
        caption=f"📊 Отчет\n📅 {date_str}\n🕒 {hour_str}:00 - {hour_str}:59"
    )
    await c.answer()

# --- УПРАВЛЕНИЕ ГРУППАМИ ---
@router.callback_query(F.data == "manage_groups")
async def cb_mgr(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    for i in range(1, 4):
        kb.button(text=f"🛑 Стоп Группа {i}", callback_data=f"stop_group_{i}")
    kb.button(text="🔙", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("🏢 Управление группами", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("stop_group_"))
async def cb_stop_g(c: CallbackQuery, bot: Bot):
    gn = int(c.data.split("_")[-1])
    async with get_db() as db:
        g = await (await db.execute("SELECT * FROM groups WHERE group_num=?", (gn,))).fetchone()
        if not g: return await c.answer("Группа не привязана!", show_alert=True)
        
        nums = await (await db.execute("SELECT * FROM numbers WHERE status IN ('work','active') AND worker_chat_id=?", (g['chat_id'],))).fetchall()
        for n in nums:
            await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (f"finished_group_{gn}", get_now(), n['id']))
            try: await bot.send_message(n['user_id'], f"🛑 Группа {gn} остановлена. Номер завершен.")
            except: pass
        await db.commit()
    await c.answer(f"✅ Группа {gn} остановлена. Завершено: {len(nums)}")

# --- РАССЫЛКА ---
@router.callback_query(F.data == "adm_cast")
async def cb_cast(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 Пришлите сообщение для рассылки (текст/фото):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="admin_main")]]))

@router.message(AdminState.waiting_broadcast)
async def fsm_cast(m: Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳ Рассылка запущена...")
    async with get_db() as db: users = await (await db.execute("SELECT user_id FROM users WHERE is_approved=1")).fetchall()
    ok, bad = 0, 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            ok += 1
            await asyncio.sleep(0.05)
        except: bad += 1
    await msg.edit_text(f"📢 Рассылка завершена!\n✅ {ok}\n❌ {bad}")

# ==========================================
# ВОРКЕР ЭКШЕНЫ
# ==========================================
@router.callback_query(F.data.startswith("w_act_"))
async def cb_w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("🚫 Не ваш номер")
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    await c.message.edit_text("✅ Активен", reply_markup=worker_active_kb(nid))
    try: await bot.send_message(row['user_id'], "✅ Номер активен")
    except: pass

@router.callback_query(F.data.startswith(("w_drop_", "w_err_", "w_skip_")))
async def cb_w_end(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    act = c.data.split("_")[1]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return
        
        if act == "skip":
            await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
            txt, utxt = "⏭ Пропуск", "⚠️ Номер возвращен в очередь"
        else:
            status = "finished" if act == "drop" else "dead"
            await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, get_now(), nid))
            txt = "📉 Слет" if act == "drop" else "❌ Ошибка"
            utxt = f"{txt}\n{SEP}\nНомер завершен."
            
        await db.commit()
    
    await c.message.edit_text(txt)
    try: await bot.send_message(row['user_id'], utxt)
    except: pass

# ==========================================
# ОБРАБОТКА ФОТО/СООБЩЕНИЙ (МОСТ)
# ==========================================
@router.message(F.photo & F.caption)
async def handle_photo(m: Message, bot: Bot):
    if "/sms" in str(m.caption):
        # Воркер -> Юзер
        try:
            ph = clean_phone(m.caption.split()[1])
            async with get_db() as db:
                row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
            if row and row['worker_id'] == m.from_user.id:
                await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"📩 <b>SMS / QR</b>\n{SEP}")
                await m.react([ReactionTypeEmoji(emoji="👌")])
            else: await m.reply("❌ Ошибка доступа")
        except: pass
    else:
        # Юзер -> Воркер
        await handle_user_msg(m, bot)

@router.message(F.text | F.photo)
async def handle_user_msg(m: Message, bot: Bot):
    if m.text and m.text.startswith('/'): return
    if m.from_user.id == ADMIN_ID: return
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
    
    if row and row['worker_chat_id']:
        # Если юзер пишет код - сбрасываем таймер ожидания кода
        if row['wait_code_start']:
            async with get_db() as db:
                await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
                await db.commit()
        
        try:
            txt = f"📩 <b>ОТВЕТ</b> ({row['phone']})\n{m.text if m.text else '[Фото]'}"
            if m.photo:
                await bot.send_photo(row['worker_chat_id'], m.photo[-1].file_id, caption=txt, message_thread_id=row['worker_thread_id'] or None)
            else:
                await bot.send_message(row['worker_chat_id'], txt, message_thread_id=row['worker_thread_id'] or None)
            await m.react([ReactionTypeEmoji(emoji="⚡")])
        except: pass

# ==========================================
# МОНИТОРИНГ (AFK + CODE TIMEOUT)
# ==========================================
@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("✅ Активность подтверждена!")

async def monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            async with get_db() as db:
                # 1. Таймаут кода (CODE_WAIT_MINUTES)
                waiters = await (await db.execute("SELECT * FROM numbers WHERE status='active' AND wait_code_start IS NOT NULL")).fetchall()
                for w in waiters:
                    st = datetime.fromisoformat(w['wait_code_start'])
                    if (now - st).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (get_now(), w['id']))
                        try: await bot.send_message(w['user_id'], "⏰ Время ожидания кода истекло. Номер отменен.")
                        except: pass
                        if w['worker_chat_id']:
                            try: await bot.send_message(w['worker_chat_id'], f"⚠️ Таймаут кода: {w['phone']}", message_thread_id=w['worker_thread_id'] or None)
                            except: pass

                # 2. AFK в очереди
                qrows = await (await db.execute("SELECT * FROM numbers WHERE status='queue'")).fetchall()
                for r in qrows:
                    las = r['last_ping'] if r['last_ping'] else r['created_at']
                    if str(las).startswith("PENDING_"):
                        pt = datetime.fromisoformat(las.split("_")[1])
                        if (now - pt).total_seconds() / 60 >= AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                            try: await bot.send_message(r['user_id'], "❌ Номер удален из очереди (нет активности)")
                            except: pass
                    else:
                        la = datetime.fromisoformat(las)
                        if (now - la).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👋 Я тут!", callback_data=f"afk_ok_{r['id']}")]]).as_markup()
                            try:
                                await bot.send_message(r['user_id'], f"⚠️ Проверка активности!\n{SEP}\nНажмите кнопку:", reply_markup=kb)
                                await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                            except:
                                await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],)) # Юзер заблочил бота
                await db.commit()
        except Exception as e:
            logger.error(f"Monitor error: {e}")
            await asyncio.sleep(5)

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(monitor(bot))
    logger.info("🚀 BOT MEGA FINAL v30.0 STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): logger.info("Bot stopped")
