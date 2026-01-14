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
    from aiogram.exceptions import TelegramForbiddenError
except ImportError:
    sys.exit("❌ pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "bot_v32_final.db" # Новая БД для поддержки Холда

# Таймеры (минуты)
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
        # Users
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
                is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Numbers (Added tariff_hold)
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
        # Tariffs (Added hold_time)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY, price TEXT, hold_time TEXT, work_time TEXT
            )
        """)
        # Groups
        await db.execute("CREATE TABLE IF NOT EXISTS groups (group_num INTEGER PRIMARY KEY, chat_id INTEGER, title TEXT)")
        # Config
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        # Defaults
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '20 мин', '10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '1 час', '24/7')")
        
        await db.commit()
    logger.info("✅ Database v32 initialized")

# ==========================================
# УТИЛИТЫ
# ==========================================
def clean_phone(phone):
    # Очистка от мусора
    clean = re.sub(r'[^\d]', '', str(phone))
    
    # Авто-чекер длины (защита от коротких/длинных)
    if len(clean) < 10 or len(clean) > 15:
        return None

    # Форматирование под СНГ (7/8)
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    
    # Проверка на валидность (только цифры)
    return '+' + clean if clean.isdigit() else None

def mask_phone(phone, user_id):
    if user_id == ADMIN_ID: return phone
    try:
        if len(phone) < 9: return phone
        return f"{phone[:5]}***{phone[-4:]}"
    except: return phone

def get_now(): return datetime.now(timezone.utc).isoformat()

def format_time(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        return (dt + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
    except: return "-"

def calc_duration(start_iso, end_iso):
    try:
        if not start_iso or not end_iso: return "0 мин"
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: return "0 мин"

# ==========================================
# FSM STATE
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_help = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    # Редактирование тарифа
    edit_price = State()
    edit_hold = State()
    edit_time = State()
    # Поддержка и отчеты
    help_reply = State()
    report_hours = State()

# ==========================================
# KEYBOARDS
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Поддержка", callback_data="ask_help")
    if user_id == ADMIN_ID:
        kb.button(text="⚡ Админ панель", callback_data="admin_main")
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

def back_kb():
    return InlineKeyboardBuilder().button(text="🔙 Меню", callback_data="back_main").as_markup()

# ==========================================
# START & AUTH
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
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"),
                        InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                    ]])
                    await m.bot.send_message(ADMIN_ID, f"👤 Запрос доступа: {uid} (@{m.from_user.username})", reply_markup=kb)
                except: pass
            return await m.answer("🔒 Доступ ограничен. Ожидайте одобрения.")
        
        if res['is_banned']: return await m.answer("🚫 Вы заблокированы.")
        if res['is_approved']: await m.answer(f"👋 Привет, {m.from_user.first_name}!\n{SEP}", reply_markup=main_kb(uid))
        else: await m.answer("⏳ Заявка на рассмотрении.")

@router.callback_query(F.data == "back_main")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Главное меню\n{SEP}", reply_markup=main_kb(c.from_user.id))

# ==========================================
# PROFILE & QUEUE
# ==========================================
@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='active'", (uid,))).fetchone())[0]
        queue_count = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='queue'", (uid,))).fetchone())[0]

    kb = InlineKeyboardBuilder()
    kb.button(text=f"🔢 Моя очередь ({queue_count})", callback_data="my_queue")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text(
        f"👤 <b>Личный кабинет</b>\n{SEP}\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📦 Всего загружено: {total}\n"
        f"🔥 В работе: {active}\n"
        f"⏳ В ожидании: {queue_count}",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )

@router.callback_query(F.data == "my_queue")
async def cb_my_queue(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        # Получаем все номера юзера в очереди, отсортированные по ID (кто раньше загрузил)
        my_rows = await (await db.execute("SELECT id, phone, tariff_name FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC", (uid,))).fetchall()
        
        if not my_rows:
            return await c.answer("📭 Ваша очередь пуста", show_alert=True)
            
        # Теперь нам нужно узнать глобальную позицию для каждого номера
        txt = f"🔢 <b>ВАША ОЧЕРЕДЬ</b>\n{SEP}\n"
        for row in my_rows:
            # Считаем сколько людей ПЕРЕД этим номером глобально
            pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?", (row['id'],))).fetchone())[0] + 1
            txt += f"📱 {mask_phone(row['phone'], uid)} - <b>{pos}#</b>\n"

    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="profile")
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

# ==========================================
# UPLOAD NUMBERS (ОЧЕРЕДЬ + ПОЗИЦИЯ)
# ==========================================
@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db: tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in tariffs: kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(f"📂 Выберите тариф\n{SEP}", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    async with get_db() as db: t = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (tn,))).fetchone()
    
    await state.update_data(tariff=tn, price=t['price'], hold=t['hold_time'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)
    
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text(
        f"💎 Тариф: {tn}\n"
        f"💰 Прайс: {t['price']}\n"
        f"⏳ Холд: {t['hold_time']}\n"
        f"⏰ Время: {t['work_time']}\n{SEP}\n"
        f"📱 Отправьте номера списком:",
        reply_markup=kb.as_markup()
    )

@router.message(UserState.waiting_numbers)
async def fsm_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    
    valid = []
    invalid_count = 0
    
    for x in raw:
        cp = clean_phone(x.strip())
        if cp: valid.append(cp)
        elif x.strip(): invalid_count += 1
    
    if not valid:
        return await m.reply("❌ Нет валидных номеров.\nПроверьте длину и формат (79xxxxxxxxx).")
    
    report = f"✅ <b>Принято в очередь: {len(valid)}</b>\n{SEP}\n"
    
    async with get_db() as db:
        for ph in valid:
            # Вставляем
            cursor = await db.execute(
                "INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_hold, work_time, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (m.from_user.id, ph, data['tariff'], data['price'], data['hold'], data['work_time'], get_now())
            )
            nid = cursor.lastrowid
            
            # Считаем позицию (глобальную)
            pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id <= ?", (nid,))).fetchone())[0]
            
            # Добавляем в отчет
            report += f"📱 {ph} — <b>{pos}#</b>\n"
            
        await db.commit()
    
    if invalid_count > 0:
        report += f"\n⚠️ <i>Не прошло проверку: {invalid_count} шт.</i>"
    
    await state.clear()
    await m.answer(report, reply_markup=main_kb(m.from_user.id), parse_mode="HTML")

# ==========================================
# WORKER LOGIC
# ==========================================
@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply("❌ Топик не настроен")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Очередь пуста")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?",
                         (m.from_user.id, cid, tid, get_now(), row['id']))
        await db.commit()
    
    # Msg to Worker
    if "MAX" in row['tariff_name'].upper():
        msg = f"🚀 <b>Взят номер</b>\n{SEP}\n📱 {row['phone']}\n⏳ Холд: {row['tariff_hold']}\n\nКод: <code>/code {row['phone']}</code>"
        kb = worker_kb_max(row['id'])
    else:
        msg = f"🚀 <b>Взят номер</b>\n{SEP}\n📱 {row['phone']}\n⏳ Холд: {row['tariff_hold']}\n\nКод: <code>/sms {row['phone']} текст</code>"
        kb = worker_kb_whatsapp(row['id'])
    
    await m.answer(msg, reply_markup=kb, parse_mode="HTML")
    
    # Msg to User
    try: await bot.send_message(row['user_id'], f"⚡ <b>Ваш номер взяли!</b>\n📱 {mask_phone(row['phone'], 0)}\nОжидайте код.", parse_mode="HTML")
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
        await bot.send_message(row['user_id'], f"🔔 <b>Офис запросил код</b>\nОтветьте сообщением ниже.", parse_mode="HTML")
        await m.reply("✅ Запрос отправлен")
    except: await m.reply("❌ Не доставлено")

@router.callback_query(F.data.startswith("w_"))
async def cb_worker_actions(c: CallbackQuery, bot: Bot):
    parts = c.data.split("_")
    act = parts[1] # act, skip, err, drop
    nid = parts[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return await c.answer("Номер не найден")
        if row['worker_id'] != c.from_user.id: return await c.answer("🔒 Не ты брал!", show_alert=True)
        
        user_msg = ""
        adm_msg = ""
        kb = None
        
        if act == "act":
            await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
            adm_msg = "✅ Номер встал"
            user_msg = "✅ Номер встал!"
            kb = worker_active_kb(nid)
            
        elif act == "skip":
            await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
            adm_msg = "⏭ Пропуск"
            user_msg = "⚠️ Офис пропустил ваш номер."
            
        elif act == "err":
            await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (get_now(), nid))
            adm_msg = "❌ Ошибка"
            user_msg = "❌ Ошибка"
            
        elif act == "drop":
            await db.execute("UPDATE numbers SET status='finished', end_time=? WHERE id=?", (get_now(), nid))
            dur = calc_duration(row['start_time'], get_now())
            adm_msg = f"📉 Слет ({dur})"
            user_msg = f"📉 Ваш номер слетел\nВремя: {dur}"
            
        await db.commit()
    
    await c.message.edit_text(adm_msg, reply_markup=kb)
    try: await bot.send_message(row['user_id'], user_msg)
    except: pass
    await c.answer()

# ==========================================
# ADMIN PANEL (EDIT TARIFFS FIXED)
# ==========================================
@router.callback_query(F.data == "admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Изменить тарифы", callback_data="adm_tariffs")
    kb.button(text="📋 Общая очередь", callback_data="all_queue")
    kb.button(text="📊 Отчеты", callback_data="adm_reports")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡ Админ панель", reply_markup=kb.as_markup())

@router.callback_query(F.data == "adm_tariffs")
async def cb_adm_t(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with get_db() as db: ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("🛠 Выберите тариф для редактирования:", reply_markup=kb.as_markup())

# --- ЦЕПОЧКА РЕДАКТИРОВАНИЯ (Price -> Hold -> Time) ---
@router.callback_query(F.data.startswith("ed_"))
async def cb_ed_start(c: CallbackQuery, state: FSMContext):
    target = c.data.split("_")[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_price)
    await c.message.edit_text(f"1️⃣ Введите новую **ЦЕНУ** для {target}\n(Например: 55₽, 12$):", parse_mode="Markdown")
    await c.answer()

@router.message(AdminState.edit_price)
async def fsm_price(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_hold)
    await m.answer("2️⃣ Введите новый **ХОЛД** (время удержания)\n(Например: 20 мин, 1 час):", parse_mode="Markdown")

@router.message(AdminState.edit_hold)
async def fsm_hold(m: Message, state: FSMContext):
    await state.update_data(hold=m.text)
    await state.set_state(AdminState.edit_time)
    await m.answer("3️⃣ Введите **ВРЕМЯ РАБОТЫ**\n(Например: 24/7, 10:00-22:00):", parse_mode="Markdown")

@router.message(AdminState.edit_time)
async def fsm_time(m: Message, state: FSMContext):
    data = await state.get_data()
    target = data['target']
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, hold_time=?, work_time=? WHERE name=?",
                         (data['price'], data['hold'], m.text, target))
        await db.commit()
    
    await state.clear()
    await m.answer(
        f"✅ <b>Тариф {target} обновлен!</b>\n{SEP}\n"
        f"💰 Прайс: {data['price']}\n"
        f"⏳ Холд: {data['hold']}\n"
        f"⏰ Время: {m.text}",
        parse_mode="HTML"
    )

# --- QUEUE VIEW ---
@router.callback_query(F.data == "all_queue")
async def cb_all_queue(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        q = await (await db.execute("SELECT * FROM numbers WHERE status='queue' ORDER BY id ASC")).fetchall()
    
    txt = f"📋 <b>ОБЩАЯ ОЧЕРЕДЬ ({len(q)})</b>\n{SEP}\n"
    if not q: txt += "Пусто"
    else:
        for i, r in enumerate(q[:20], 1):
            txt += f"{i}. {r['phone']} ({r['tariff_name']})\n"
        if len(q) > 20: txt += f"... и еще {len(q)-20}"
        
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="admin_main")
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

# ==========================================
# HELP & SUPPORT
# ==========================================
@router.callback_query(F.data == "guide")
async def cb_guide(c: CallbackQuery):
    await c.message.edit_text(
        f"📲 <b>Гайд</b>\n{SEP}\n"
        f"1. Выберите тариф.\n"
        f"2. Загрузите номера (валидатор проверит длину 10-15 цифр).\n"
        f"3. Получите ID позиции в очереди (например 5#).\n"
        f"4. Ждите выплаты после холда.",
        reply_markup=back_kb(), parse_mode="HTML"
    )

@router.callback_query(F.data == "ask_help")
async def cb_ask(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_help)
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text("🆘 Напишите ваш вопрос:", reply_markup=kb.as_markup())

@router.message(UserState.waiting_help)
async def fsm_help(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="Ответить", callback_data=f"reply_{m.from_user.id}")
    await bot.send_message(ADMIN_ID, f"🆘 <b>Вопрос от {m.from_user.id}</b>:\n{m.text}", reply_markup=kb.as_markup(), parse_mode="HTML")
    await m.answer("✅ Вопрос отправлен.", reply_markup=main_kb(m.from_user.id))

@router.callback_query(F.data.startswith("reply_"))
async def cb_reply(c: CallbackQuery, state: FSMContext):
    uid = c.data.split("_")[1]
    await state.update_data(ruid=uid)
    await state.set_state(AdminState.help_reply)
    await c.message.answer(f"✍️ Ответ для {uid}:")
    await c.answer()

@router.message(AdminState.help_reply)
async def fsm_reply_send(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    await state.clear()
    try:
        await bot.send_message(d['ruid'], f"👨‍💻 <b>Поддержка:</b>\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Отправлено")
    except: await m.answer("❌ Не доставлено")

# ==========================================
# MONITOR & MAIN
# ==========================================
async def monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            async with get_db() as db:
                # Timeout Code
                waiters = await (await db.execute("SELECT * FROM numbers WHERE status='active' AND wait_code_start IS NOT NULL")).fetchall()
                for w in waiters:
                    st = datetime.fromisoformat(w['wait_code_start'])
                    if (now - st).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (get_now(), w['id']))
                        try: await bot.send_message(w['user_id'], f"⏰ Время вышло. Номер {w['phone']} отменен.")
                        except: pass
                
                # AFK
                q = await (await db.execute("SELECT * FROM numbers WHERE status='queue'")).fetchall()
                for r in q:
                    lp = r['last_ping'] if r['last_ping'] else r['created_at']
                    if "PENDING" in str(lp):
                         pt = datetime.fromisoformat(lp.split("_")[1])
                         if (now - pt).total_seconds() / 60 >= AFK_KICK_MINUTES:
                             await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                    else:
                        la = datetime.fromisoformat(lp)
                        if (now - la).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                             kb = InlineKeyboardBuilder().button(text="👋 Я тут", callback_data=f"afk_ok_{r['id']}").as_markup()
                             try:
                                 await bot.send_message(r['user_id'], "⚠️ Проверка активности!", reply_markup=kb)
                                 await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                             except:
                                 await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                await db.commit()
        except Exception as e:
            logger.error(f"Monitor: {e}")

@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("✅ Подтверждено")

@router.message(F.text | F.photo)
async def msg_handler(m: Message, bot: Bot):
    if m.text and m.text.startswith('/'): return
    if m.from_user.id == ADMIN_ID: return
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
    
    if row and row['worker_chat_id']:
        if row['wait_code_start']:
             async with get_db() as db:
                 await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
                 await db.commit()
        
        txt = f"📩 ({row['phone']})\n{m.text if m.text else '[Фото]'}"
        try:
            if m.photo: await bot.send_photo(row['worker_chat_id'], m.photo[-1].file_id, caption=txt, message_thread_id=row['worker_thread_id'])
            else: await bot.send_message(row['worker_chat_id'], txt, message_thread_id=row['worker_thread_id'])
            await m.react([ReactionTypeEmoji(emoji="⚡")])
        except: pass

async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(monitor(bot))
    logger.info("🚀 BOT v32.0 FINAL STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
