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
except ImportError:
    sys.exit("❌ pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "fast_team_v31.db" 

# Таймеры (в минутах)
AFK_CHECK_MINUTES = 8   
AFK_KICK_MINUTES = 3    
CODE_WAIT_MINUTES = 4   

SEP = "━━━━━━━━━━━━━━━━━━━━"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

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
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP,
                last_afk_check TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
                tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, work_time TEXT, 
                status TEXT DEFAULT 'queue', 
                worker_id INTEGER DEFAULT 0, worker_chat_id INTEGER DEFAULT 0, worker_thread_id INTEGER DEFAULT 0, 
                start_time TEXT, end_time TEXT, last_ping TEXT, wait_code_start TEXT, 
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Обновленная таблица тарифов с HOLD
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (name TEXT PRIMARY KEY, price TEXT, hold_time TEXT, work_time TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS groups (group_num INTEGER PRIMARY KEY, chat_id INTEGER, title TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        # Дефолт тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('WhatsApp','50₽','1h','10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('MAX','10$','2h','24/7')")
        await db.commit()
    logger.info("✅ Database initialized (v31.0 FINAL)")

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

def format_dt_human(iso_str):
    """Формат: 2026-01-15 14:30 (без секунд и таймзон)"""
    try: 
        dt = datetime.fromisoformat(iso_str) + timedelta(hours=3) # MSK fix
        return dt.strftime("%Y-%m-%d %H:%M")
    except: return "-"

def calc_duration(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: return "0 мин"

# ==========================================
# FSM STATES
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_help = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    help_reply = State()
    # Редактирование тарифа
    edit_select = State()
    edit_price = State()
    edit_hold = State()
    edit_time = State()
    # Отчеты
    report_hours = State()

# ==========================================
# КЛАВИАТУРЫ
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Поддержка", callback_data="ask_help")
    if user_id == ADMIN_ID: kb.button(text="⚡ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 2)
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

def back_kb():
    return InlineKeyboardBuilder().button(text="🔙 Меню", callback_data="back_main").as_markup()

# ==========================================
# АВТОРИЗАЦИЯ И МЕНЮ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", 
                             (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            if ADMIN_ID:
                try: 
                    # ЗАПРОС ДОСТУПА - Текст как просил
                    await m.bot.send_message(
                        ADMIN_ID, 
                        f"👤 Запрос доступа: {uid} (@{m.from_user.username})", 
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), 
                            InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                        ]])
                    )
                except: pass
            return await m.answer("🔒 Доступ ограничен. Ждите одобрения администратора.")
        
        if res['is_banned']: return await m.answer("🚫 Вы заблокированы.")
        if res['is_approved']: 
            await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=main_kb(uid))
        else: 
            await m.answer("⏳ Заявка на рассмотрении.")

@router.callback_query(F.data == "back_main")
async def nav_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Привет, {c.from_user.first_name}!", reply_markup=main_kb(c.from_user.id))
    await c.answer()

# ==========================================
# ПРОФИЛЬ (КРАСИВЫЙ)
# ==========================================
@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        user = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        stats = await (await db.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status='finished' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN status='dead' THEN 1 ELSE 0 END) as bad,
                SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active
            FROM numbers WHERE user_id=?
        """, (uid,))).fetchone()

    reg_date = format_dt_human(user['reg_date']).split()[0]
    
    txt = (
        f"👤 <b>ВАШ ПРОФИЛЬ</b>\n{SEP}\n"
        f"🆔 <b>ID:</b> <code>{uid}</code>\n"
        f"📅 <b>Регистрация:</b> {reg_date}\n"
        f"⭐️ <b>Статус:</b> {'✅ Доверенный' if user['is_approved'] else '⏳ Новичок'}\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"├ 📦 Всего загружено: <b>{stats['total']}</b>\n"
        f"├ ✅ Успешно (Выплата): <b>{stats['done']}</b>\n"
        f"├ 📉 Слеты/Ошибки: <b>{stats['bad']}</b>\n"
        f"└ 🔥 Активно сейчас: <b>{stats['active']}</b>\n{SEP}"
    )
    
    await c.message.edit_text(txt, reply_markup=back_kb(), parse_mode="HTML")

# ==========================================
# СДАЧА НОМЕРОВ
# ==========================================
@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db: tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in tariffs: 
        kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("📂 <b>ВЫБЕРИТЕ ТАРИФ:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    async with get_db() as db: t = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (tn,))).fetchone()
    
    await state.update_data(tariff=tn, price=t['price'], hold=t['hold_time'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)
    
    txt = (
        f"💎 <b>Тариф: {tn}</b>\n"
        f"💰 Прайс: {t['price']}\n"
        f"⏳ Холд: {t['hold_time']}\n"
        f"⏰ Время работы: {t['work_time']}\n{SEP}\n"
        f"👇 <b>Пришлите список номеров:</b>"
    )
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="back_main")]]), parse_mode="HTML")

@router.message(UserState.waiting_numbers)
async def fsm_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = []
    for x in raw:
        cp = clean_phone(x.strip())
        if cp: valid.append(cp)
    
    if not valid: 
        return await m.reply("❌ Не вижу валидных номеров.\nФормат: 79991234567")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute("""
                INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_hold, work_time, last_ping) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (m.from_user.id, ph, data['tariff'], data['price'], data['hold'], data['work_time'], get_now()))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ Принято в очередь: {len(valid)} шт.", reply_markup=main_kb(m.from_user.id))

# ==========================================
# ПОМОЩЬ И ПОДДЕРЖКА
# ==========================================
@router.callback_query(F.data == "guide")
async def cb_guide(c: CallbackQuery):
    txt = (
        f"📲 <b>Что делает бот</b>\n"
        f"Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
        f"📦 <b>Требования к номерам</b>\n"
        f"✔️ Активный и чистый номер\n"
        f"✔️ Доступ к SMS\n"
        f"❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
        f"⏳ <b>Холд и выплаты</b>\n"
        f"Холд — время проверки номера\n"
        f"💰 Выплата производится после успешного завершения холда\n\n"
        f"⚠️ Отправляя номер, вы подтверждаете, что ознакомились с правилами"
    )
    await c.message.edit_text(txt, reply_markup=back_kb(), parse_mode="HTML")

@router.callback_query(F.data == "ask_help")
async def cb_ask(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_help)
    await c.message.edit_text("🆘 <b>Напишите ваш вопрос:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="back_main")]]), parse_mode="HTML")

@router.message(UserState.waiting_help)
async def fsm_help(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    # Админу
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Ответить", callback_data=f"ans_help_{m.from_user.id}")
    
    msg = f"🆘 <b>НОВЫЙ ВОПРОС</b>\n{SEP}\nОт: @{m.from_user.username} ({m.from_user.id})\nТекст: {m.text}"
    await bot.send_message(ADMIN_ID, msg, reply_markup=kb.as_markup(), parse_mode="HTML")
    await m.answer("✅ Сообщение отправлено администрации.")

@router.callback_query(F.data.startswith("ans_help_"))
async def cb_ans_help(c: CallbackQuery, state: FSMContext):
    uid = c.data.split("_")[2]
    await state.update_data(target_id=uid)
    await state.set_state(AdminState.help_reply)
    await c.message.answer("✍️ Пиши ответ:")
    await c.answer()

@router.message(AdminState.help_reply)
async def fsm_ans_send(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    try:
        await bot.send_message(data['target_id'], f"🆘 <b>Ответ поддержки:</b>\n{SEP}\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ ушел.")
    except: await m.answer("❌ Не доставлено (юзер блокнул бота)")

# ==========================================
# ВОРКЕР (ЛОГИКА WHATSAPP / MAX)
# ==========================================
@router.message(Command("bindgroup"))
async def cmd_bind(m: Message, command: CommandObject):
    if m.from_user.id != ADMIN_ID: return
    try: gn = int(command.args.strip())
    except: return
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO groups (group_num, chat_id, title) VALUES (?, ?, ?)", (gn, m.chat.id, m.chat.title))
        await db.commit()
    
    txt = (
        f"✅ Чат привязан!\n\n"
        f"👨‍💻 <b>Гайд по использованию:</b>\n\n"
        f"1️⃣ Пиши /num -> Получишь номер.\n\n"
        f"2️⃣ Вбей номер в WhatsApp Web.\n\n"
        f"3️⃣ Если просят QR: Сфоткай QR с экрана.\n"
        f"   Скинь фото сюда и подпиши: <code>/sms +77... Сканируй</code>\n\n"
        f"4️⃣ Если просят Код (по номеру): Сфоткай код с экрана.\n"
        f"   Скинь фото сюда и подпиши: <code>/sms +77... Вводи этот код</code>\n\n"
        f"5️⃣ Когда зашел -> жми ✅ Встал.\n"
        f"6️⃣ Когда номер слетел -> жми 📉 Слет."
    )
    await m.answer(txt, parse_mode="HTML")

@router.message(Command("stopwork"))
async def cmd_stopwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (f"topic_{cid}_{tid}",))
        await db.commit()
    await m.answer("🛑 Работа в топике остановлена. Привязка снята.")

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
        await db.commit()
    
    # Сообщение Воркеру
    is_max = "MAX" in row['tariff_name'].upper()
    
    msg = (
        f"🚀 <b>Вы взяли номер.</b>\n"
        f"📱 {row['phone']}\n"
        f"💰 {row['tariff_price']} | ⏳ {row['tariff_hold']}\n"
        f"{SEP}\n"
    )
    
    if is_max:
        msg += f"Код: <code>/code {row['phone']}</code>"
        kb = worker_kb_max(row['id'])
    else:
        msg += f"Код: <code>/sms {row['phone']} текст</code>"
        kb = worker_kb_whatsapp(row['id'])
    
    await m.answer(msg, reply_markup=kb, parse_mode="HTML")

    # Сообщение Юзеру
    try: 
        await bot.send_message(row['user_id'], f"⚡ <b>Номер в работе!</b>\n📱 {mask_phone(row['phone'], 0)}\nОжидайте код.", parse_mode="HTML")
    except: pass

# --- ОБРАБОТКА SMS (WhatsApp) ---
@router.message(Command("sms"))
async def cmd_sms(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return
    args = command.args.split(maxsplit=1)
    ph = clean_phone(args[0])
    txt_to_send = args[1] if len(args) > 1 else "Фото"
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер")
    
    caption = f"📩 <b>SMS / QR</b>\n{SEP}\n{txt_to_send}"
    try:
        if m.photo:
            await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=caption, parse_mode="HTML")
        else:
            await bot.send_message(row['user_id'], caption, parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="👌")])
    except: await m.reply("❌ Не доставлено")

# --- ОБРАБОТКА CODE (MAX) ---
@router.message(Command("code"))
async def cmd_code_req(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер")
    
    # Ставим метку ожидания
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now(), row['id']))
        await db.commit()
        
    try:
        await bot.send_message(row['user_id'], f"🔔 <b>Офис запросил номер</b>\nОтветьте ниже сообщением чтобы дать код.", parse_mode="HTML")
        await m.reply("✅ Запрос отправлен юзеру")
    except: await m.reply("❌ Не доставлено")

# --- КНОПКИ ВОРКЕРА ---
@router.callback_query(F.data.startswith("w_"))
async def cb_worker_action(c: CallbackQuery, bot: Bot):
    parts = c.data.split("_")
    act = parts[1] # act, err, skip, drop
    nid = parts[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return await c.answer("Номер не найден")
        if row['worker_id'] != c.from_user.id: return await c.answer("🔒 Не ты брал этот номер!", show_alert=True)
        
        user_msg = ""
        admin_msg = ""
        
        if act == "act":
            await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
            admin_msg = "✅ Номер встал"
            user_msg = "✅ <b>Номер встал</b>"
            new_kb = worker_active_kb(nid)
            
        elif act == "err":
            await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (get_now(), nid))
            admin_msg = "❌ Ошибка"
            user_msg = f"❌ <b>Ошибка</b>\n📱 {mask_phone(row['phone'], 0)}"
            new_kb = None
            
        elif act == "skip":
            await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
            admin_msg = "⏭ Пропуск (возврат в очередь)"
            user_msg = "⚠️ Офис пропустил ваш номер."
            new_kb = None
            
        elif act == "drop":
            await db.execute("UPDATE numbers SET status='finished', end_time=? WHERE id=?", (get_now(), nid))
            dur = calc_duration(row['start_time'], get_now())
            admin_msg = f"📉 Слет ({dur})"
            user_msg = f"📉 <b>Ваш номер слетел</b>\nВремя работы: {dur}"
            new_kb = None
            
        await db.commit()
    
    if new_kb: await c.message.edit_text(admin_msg, reply_markup=new_kb)
    else: await c.message.edit_text(admin_msg)
    
    try: await bot.send_message(row['user_id'], user_msg, parse_mode="HTML")
    except: pass
    await c.answer()

# ==========================================
# МОСТ (ЮЗЕР -> ВОРКЕР)
# ==========================================
@router.message(F.text | F.photo)
async def bridge_msg(m: Message, bot: Bot):
    if m.text and m.text.startswith('/'): return
    if m.from_user.id == ADMIN_ID: return
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
    
    if row and row['worker_chat_id']:
        # Если ждали код (MAX) - сбрасываем таймер
        if row['wait_code_start']:
            async with get_db() as db:
                await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
                await db.commit()
        
        txt = f"📩 <b>ОТВЕТ ЮЗЕРА</b> ({row['phone']})\n{m.text if m.text else '[Фото]'}"
        try:
            if m.photo:
                await bot.send_photo(row['worker_chat_id'], m.photo[-1].file_id, caption=txt, message_thread_id=row['worker_thread_id'] or None, parse_mode="HTML")
            else:
                await bot.send_message(row['worker_chat_id'], txt, message_thread_id=row['worker_thread_id'] or None, parse_mode="HTML")
            await m.react([ReactionTypeEmoji(emoji="⚡")])
        except: pass

# ==========================================
# АДМИН ПАНЕЛЬ (НОВАЯ)
# ==========================================
@router.callback_query(F.data == "admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="⚙️ Изменить тарифы", callback_data="adm_tariffs")
    kb.button(text="📊 Отчеты", callback_data="adm_reports")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("acc_"))
async def cb_acc(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    
    async with get_db() as db:
        # Получаем данные юзера для красивого ответа
        u_data = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        name_str = f"{u_data['first_name']} (@{u_data['username']})" if u_data else str(uid)
        
        if act == "ok":
            await db.execute("UPDATE users SET is_approved=1, is_banned=0 WHERE user_id=?", (uid,))
            adm_text = f"✅ Пользователь {uid} принят.\nИмя: {name_str}"
            user_text = "✅ Вам одобрен доступ! Жмите /start"
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            adm_text = f"🚫 Пользователь {uid} забанен."
            user_text = "🚫 Отказ."
        await db.commit()
    
    await c.message.edit_text(adm_text)
    try: await bot.send_message(uid, user_text)
    except: pass
    await c.answer()

# --- РЕДАКТОР ТАРИФОВ ---
@router.callback_query(F.data == "adm_tariffs")
async def cb_adm_tar(c: CallbackQuery):
    async with get_db() as db: ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'], callback_data=f"edtar_{t['name']}")
    kb.button(text="🔙", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("Выберите тариф для изменения:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("edtar_"))
async def cb_ed_sel(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    await state.update_data(target_tariff=tn)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Прайс", callback_data="chg_price")
    kb.button(text="⏳ Холд", callback_data="chg_hold")
    kb.button(text="⏰ Время работы", callback_data="chg_time")
    kb.button(text="🔙", callback_data="adm_tariffs")
    kb.adjust(1)
    await c.message.edit_text(f"⚙️ Настройка: <b>{tn}</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("chg_"))
async def cb_chg_field(c: CallbackQuery, state: FSMContext):
    field = c.data.split("_")[1]
    await state.update_data(field=field)
    
    if field == "price": await state.set_state(AdminState.edit_price)
    elif field == "hold": await state.set_state(AdminState.edit_hold)
    elif field == "time": await state.set_state(AdminState.edit_time)
    
    await c.message.edit_text(f"✍️ Введите новое значение для <b>{field.upper()}</b>:", parse_mode="HTML")

@router.message(AdminState.edit_price, F.text)
@router.message(AdminState.edit_hold, F.text)
@router.message(AdminState.edit_time, F.text)
async def fsm_save_tariff(m: Message, state: FSMContext):
    data = await state.get_data()
    tn = data['target_tariff']
    field = data['field']
    
    col_map = {"price": "price", "hold": "hold_time", "time": "work_time"}
    col = col_map[field]
    
    async with get_db() as db:
        await db.execute(f"UPDATE tariffs SET {col}=? WHERE name=?", (m.text, tn))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ Тариф {tn} обновлен!\n{field.upper()} -> {m.text}")
    # Возврат в меню можно сделать кнопкой, но админу проще так

# --- ОТЧЕТЫ ---
@router.callback_query(F.data == "adm_reports")
async def cb_rep_ask(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.report_hours)
    await c.message.edit_text("📊 <b>Генерация отчета</b>\n\nВведите период в часах (например: 24, 48, 120):", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="admin_main")]]), 
                              parse_mode="HTML")

@router.message(AdminState.report_hours)
async def fsm_rep_gen(m: Message, state: FSMContext):
    try: hours = int(m.text)
    except: return await m.reply("❌ Введите число (часы).")
    
    if hours > 120: hours = 120 # Ограничение как просил
    
    dt_start = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT * FROM numbers WHERE created_at >= ? ORDER BY id DESC
        """, (dt_start,))).fetchall()
        
    if not rows:
        await state.clear()
        return await m.answer("📂 За этот период пусто.")
    
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'User', 'Phone', 'Status', 'Tariff', 'Created', 'Start', 'End', 'Worker'])
    
    for r in rows:
        w.writerow([
            r['id'], r['user_id'], r['phone'], r['status'], r['tariff_name'],
            format_dt_human(r['created_at']), format_dt_human(r['start_time']), format_dt_human(r['end_time']), r['worker_id']
        ])
        
    out.seek(0)
    await m.answer_document(
        BufferedInputFile(out.getvalue().encode(), filename=f"report_{hours}h.csv"),
        caption=f"📊 Отчет за последние {hours} часов"
    )
    await state.clear()

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 BOT STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
