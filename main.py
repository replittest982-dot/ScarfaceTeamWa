import asyncio
import logging
import sys
import os
import re
import csv
import io
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

# Проверка библиотек
try:
    import aiosqlite
    from aiogram import Bot, Dispatcher, Router, F, types
    from aiogram.filters import Command, CommandStart, CommandObject
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import (
        InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, 
        Message, ReactionTypeEmoji, BufferedInputFile, ReplyKeyboardRemove
    )
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from aiogram.exceptions import TelegramForbiddenError
except ImportError:
    sys.exit("❌ Установите библиотеки: pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_ТОКЕН_СЮДА")
ADMIN_ID = int(os.getenv("ADMIN_ID", "12345678"))
DB_NAME = "fast_team_v65.db"

# Настройки AFK (Анти-сон)
AFK_CHECK_MINUTES = 8  
AFK_TIMEOUT_MINUTES = 3 

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
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
            reg_date TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            tariff_name TEXT, tariff_price TEXT, tariff_time TEXT, tariff_hold INTEGER,
            status TEXT DEFAULT 'queue', worker_id INTEGER DEFAULT 0, 
            start_time TEXT, end_time TEXT, last_ping TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        # Тарифы: имя, цена, время работы, холд (часы)
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            name TEXT PRIMARY KEY, price TEXT, work_time TEXT, hold_hours INTEGER DEFAULT 1)""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Дефолт
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '10-22 МСК', 24)")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '24/7', 2)")
        
        await db.commit()
    logger.info("✅ DB Loaded v65.0")

# ==========================================
# УТИЛИТЫ
# ==========================================
def clean_phone(phone: str):
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    return '+' + clean if re.match(r'^7\d{10}$', clean) else None

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
        return (dt + timedelta(hours=3)).strftime("%d.%m %H:%M")
    except: return "-"

def calc_duration_mins(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        return int((e - s).total_seconds() / 60)
    except: return 0

# ==========================================
# FSM
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_support = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_time = State()
    edit_price = State()
    edit_hold = State()
    support_reply = State()

# ==========================================
# КЛАВИАТУРЫ
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="📦 Очередь (Мои)", callback_data="my_queue")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Задать вопрос", callback_data="ask_supp")
    if user_id == ADMIN_ID: kb.button(text="⚡ Админ", callback_data="admin_main")
    kb.adjust(1, 1, 2, 1, 1)
    return kb.as_markup()

def worker_kb(nid, tariff):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
    if "MAX" in tariff.upper():
        kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{nid}")
    else:
        kb.button(text="❌ Ошибка", callback_data=f"w_err_{nid}")
    return kb.as_markup()

def worker_active_kb(nid):
    return InlineKeyboardBuilder().button(text="📉 Слет", callback_data=f"w_drop_{nid}").as_markup()

# ==========================================
# AFK MONITOR
# ==========================================
async def afk_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            async with get_db() as db:
                rows = await (await db.execute("SELECT id, user_id, phone, created_at, last_ping FROM numbers WHERE status='queue'")).fetchall()
                for r in rows:
                    last_act = r['last_ping'] if r['last_ping'] else r['created_at']
                    if last_act.startswith("PENDING_"): continue
                    
                    diff = (now - datetime.fromisoformat(last_act)).total_seconds() / 60
                    if diff >= AFK_CHECK_MINUTES:
                        kb = InlineKeyboardBuilder().button(text="👋 Я тут!", callback_data=f"afk_{r['id']}").as_markup()
                        try:
                            await bot.send_message(r['user_id'], f"⚠️ <b>AFK Проверка!</b>\nПодтвердите, что вы тут, или номер {mask_phone(r['phone'], r['user_id'])} удалится.", reply_markup=kb, parse_mode="HTML")
                            await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                            await db.commit()
                        except:
                            await db.execute("UPDATE numbers SET status='deleted' WHERE id=?", (r['id'],))
                            await db.commit()

                # Удаление PENDING
                pend = await (await db.execute("SELECT id, user_id, phone, last_ping FROM numbers WHERE status='queue' AND last_ping LIKE 'PENDING_%'")).fetchall()
                for r in pend:
                    pt = datetime.fromisoformat(r['last_ping'].replace("PENDING_", ""))
                    if (now - pt).total_seconds() / 60 >= AFK_TIMEOUT_MINUTES:
                        await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                        await db.commit()
                        try: await bot.send_message(r['user_id'], f"🗑 Номер {mask_phone(r['phone'], r['user_id'])} удален (AFK).", parse_mode="HTML")
                        except: pass
        except Exception as e:
            logger.error(f"AFK Error: {e}")

@router.callback_query(F.data.startswith("afk_"))
async def afk_ok(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("✅")

# ==========================================
# ЮЗЕР ХЕНДЛЕРЫ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with get_db() as db:
        u = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        if not u:
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", (uid, m.from_user.username, m.from_user.first_name))
            await db.commit()
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫", callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>Запрос:</b> {uid} (@{m.from_user.username})", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 Ожидайте доступа.")
        
        if u['is_banned']: return await m.answer("🚫 Бан.")
        if u['is_approved']: await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=main_kb(uid))
        else: await m.answer("⏳ На проверке.")

@router.callback_query(F.data == "sel_tariff")
async def sel_t(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=f"{r['name']} | {r['price']}", callback_data=f"pick_{r['name']}")
    kb.button(text="🔙", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("📂 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def pick_t(c: CallbackQuery, state: FSMContext):
    t = c.data.split("_")[1]
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (t,))).fetchone()
    
    await state.update_data(tariff=t, price=res['price'], time=res['work_time'], hold=res['hold_hours'])
    kb = InlineKeyboardBuilder().button(text="🔙", callback_data="back_main")
    
    msg = (f"💎 Тариф: <b>{t}</b>\n💰 Прайс: {res['price']}\n⏰ Время: {res['work_time']}\n⏳ Холд: {res['hold_hours']}ч\n\n"
           "📱 <b>Введите номера (списком):</b>")
    await c.message.edit_text(msg, reply_markup=kb.as_markup(), parse_mode="HTML")
    await state.set_state(UserState.waiting_numbers)

@router.message(UserState.waiting_numbers)
async def proc_nums(m: Message, state: FSMContext):
    d = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = []
    for x in raw:
        ph = clean_phone(x.strip())
        if ph: valid.append(ph)
    
    if not valid: return await m.reply("❌ Нет номеров.")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute("INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_time, tariff_hold, last_ping) VALUES (?, ?, ?, ?, ?, ?, ?)",
                             (m.from_user.id, ph, d['tariff'], d['price'], d['time'], d['hold'], get_now()))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ <b>Принято {len(valid)} шт.</b>", reply_markup=main_kb(m.from_user.id), parse_mode="HTML")

# --- ОЧЕРЕДЬ (МОИ) ---
@router.callback_query(F.data == "my_queue")
async def my_queue(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        # Всего в очереди
        glob = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue'")).fetchone())[0]
        # Мои
        rows = await (await db.execute("SELECT id, phone FROM numbers WHERE user_id=? AND status='queue' LIMIT 10", (uid,))).fetchall()
    
    txt = f"🌍 <b>Всего в очереди:</b> {glob}\n\n📝 <b>Ваши в ожидании (нажми чтобы удалить):</b>"
    if not rows: txt += "\n(Пусто)"
    
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"❌ {mask_phone(r['phone'], uid)}", callback_data=f"del_{r['id']}")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("del_"))
async def del_q(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        await db.execute("DELETE FROM numbers WHERE id=? AND user_id=? AND status='queue'", (nid, c.from_user.id))
        await db.commit()
    await c.answer("Удалено")
    await my_queue(c)

# --- ПРОФИЛЬ ---
@router.callback_query(F.data == "profile")
async def show_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        paid = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='finished'", (uid,))).fetchone())[0]
    
    txt = f"👤 <b>Профиль</b>\n\n📦 Всего сдано: {total}\n✅ Выплачено (завершено): {paid}"
    kb = InlineKeyboardBuilder().button(text="🔙", callback_data="back_main")
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

# --- ПОМОЩЬ ---
@router.callback_query(F.data == "guide")
async def guide(c: CallbackQuery):
    txt = ("📲 <b>Гайд</b>\n\n"
           "1. 📥 <b>Сдать номер:</b> Выбери тариф -> Отправь номера.\n"
           "2. 📦 <b>Очередь:</b> Следи за позицией, удаляй если передумал.\n"
           "3. 🔔 <b>Код:</b> Когда бот запросит код — ответь НА СООБЩЕНИЕ бота.\n"
           "4. ⚠️ <b>AFK:</b> Не спи! Бот удалит номер, если не подтвердить активность.")
    await c.message.edit_text(txt, reply_markup=main_kb(c.from_user.id), parse_mode="HTML")

# ==========================================
# ВОРКЕР (КОМАНДЫ)
# ==========================================
@router.message(Command("num"))
async def w_get(m: Message, bot: Bot):
    # Топик фикс
    tid = m.message_thread_id if m.is_topic_message else 0
    cid = m.chat.id
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"t_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply("❌ Топик не настроен (/startwork).")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Пусто.")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", (m.from_user.id, get_now(), row['id']))
        await db.commit()
        
        # Увед 3-му
        third = await (await db.execute("SELECT user_id FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1 OFFSET 2", (conf['value'],))).fetchone()
        if third:
            try: await bot.send_message(third['user_id'], "🔔 <b>Готовься!</b> Скоро твоя очередь (3-й).", parse_mode="HTML")
            except: pass

    await m.answer(f"🚀 <b>Взял:</b> <code>{row['phone']}</code>\n⏳ Холд: {row['tariff_hold']}ч", 
                   reply_markup=worker_kb(row['id'], row['tariff_name']), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"⚡ <b>Номер {mask_phone(row['phone'], row['user_id'])} в работе!</b>\nЖди код/QR.", parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def w_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ <code>/code +7...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не твой.")
    
    try:
        await bot.send_message(row['user_id'], 
                               f"🔔 <b>Офис просит код!</b>\n📱 {mask_phone(ph, row['user_id'])}\n👇 <b>ОТВЕТЬ НА ЭТО СООБЩЕНИЕ КОДОМ!</b>", 
                               reply_markup=types.ForceReply(selective=True), parse_mode="HTML")
        await m.reply("✅ Запросил.")
    except: await m.reply("❌ Ошибка.")

# --- РЕПЛАЙ ЮЗЕРА (MAX) ---
@router.message(F.reply_to_message)
async def user_reply(m: Message, bot: Bot):
    if m.from_user.id == ADMIN_ID: return 
    # Если реплай на бота
    if m.reply_to_message.from_user.id == bot.id:
        async with get_db() as db:
            row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
        
        if row:
            # Берем ВЕСЬ текст как код
            code = m.text or "[Файл]"
            try:
                await bot.send_message(row['worker_id'], f"📩 <b>КОД ОТ ЮЗЕРА:</b>\n📱 {row['phone']}\n💬 <code>{code}</code>", parse_mode="HTML")
                await m.answer("✅ Отправил.")
            except: pass

# --- SMS (WHATSAPP) ---
@router.message(F.photo)
async def w_sms(m: Message, bot: Bot):
    if not m.caption or "/sms" not in m.caption: return
    try:
        parts = m.caption.split()
        idx = next(i for i, p in enumerate(parts) if "/sms" in p)
        ph = clean_phone(parts[idx+1])
        txt = " ".join(parts[idx+2:]) or "Скан QR"
    except: return await m.reply("⚠️ /sms +7... текст")
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row: return await m.reply("❌ Нет такого.")
    
    try:
        await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"🔔 <b>SMS/QR</b>\n{txt}", parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except: await m.reply("❌ Ошибка.")

# --- КНОПКИ ---
@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text(f"✅ <b>Встал!</b>\nХолд: {row['tariff_hold']}ч", reply_markup=worker_active_kb(nid), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], "✅ <b>Номер встал!</b>", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text("⏭ <b>Пропуск</b>", parse_mode="HTML")
    try: await bot.send_message(row['user_id'], "⚠️ Офис пропустил номер.", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith(("w_drop_", "w_err_")))
async def w_end(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    drop = "drop" in c.data
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return
        
        st = "finished" if drop else "dead"
        now = get_now()
        dur = calc_duration_mins(row['start_time'], now)
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (st, now, nid))
        await db.commit()
    
    msg = f"📉 Слет ({dur} мин)" if drop else "❌ Ошибка"
    await c.message.edit_text(msg)
    try: await bot.send_message(row['user_id'], msg)
    except: pass

# ==========================================
# АДМИН
# ==========================================
@router.callback_query(F.data == "admin_main")
async def adm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы", callback_data="adm_tariffs")
    kb.button(text="📄 Отчеты", callback_data="adm_reps")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("👑 Админ", reply_markup=kb.as_markup())

# --- ТАРИФЫ (ВРЕМЯ -> ПРАЙС -> ХОЛД) ---
@router.callback_query(F.data == "adm_tariffs")
async def adm_t(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}", callback_data=f"edt_{t['name']}")
    kb.button(text="🔙", callback_data="admin_main")
    await c.message.edit_text("Какой менять?", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("edt_"))
async def edt1(c: CallbackQuery, state: FSMContext):
    await state.update_data(tag=c.data.split("_")[1])
    await state.set_state(AdminState.edit_time)
    await c.message.edit_text("1️⃣ <b>Время работы:</b> (напр. <code>10-22 МСК</code>)", parse_mode="HTML")

@router.message(AdminState.edit_time)
async def edt2(m: Message, state: FSMContext):
    await state.update_data(time=m.text)
    await state.set_state(AdminState.edit_price)
    await m.answer("2️⃣ <b>Прайс:</b> (напр. <code>50₽</code>)", parse_mode="HTML")

@router.message(AdminState.edit_price)
async def edt3(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_hold)
    await m.answer("3️⃣ <b>Холд (часов):</b> (только число, напр. <code>24</code>)", parse_mode="HTML")

@router.message(AdminState.edit_hold)
async def edt4(m: Message, state: FSMContext):
    d = await state.get_data()
    try: h = int(m.text)
    except: return await m.answer("❌ Число надо!")
    
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, work_time=?, hold_hours=? WHERE name=?", (d['price'], d['time'], h, d['tag']))
        await db.commit()
    await state.clear()
    await m.answer("✅ Сохранено!", reply_markup=main_kb(ADMIN_ID))

# --- ОТЧЕТЫ ---
@router.callback_query(F.data == "adm_reps")
async def adm_r(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for h in [1, 3, 12, 24, 48, 168]: kb.button(text=f"{h}ч", callback_data=f"rep_{h}")
    kb.button(text="🔙", callback_data="admin_main")
    await c.message.edit_text("Период:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_"))
async def get_rep(c: CallbackQuery):
    h = int(c.data.split("_")[1])
    cut = (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat()
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM numbers WHERE created_at >= ? ORDER BY id DESC", (cut,))).fetchall()
    
    f = io.StringIO()
    w = csv.writer(f)
    w.writerow(['ID', 'Phone', 'Status', 'Tariff', 'Duration(m)', 'Hold(h)', 'Hold OK?'])
    for r in rows:
        dur = 0
        ok = "NO"
        if r['end_time'] and r['start_time']:
            dur = calc_duration_mins(r['start_time'], r['end_time'])
            if dur >= (r['tariff_hold'] * 60): ok = "YES"
        w.writerow([r['id'], r['phone'], r['status'], r['tariff_name'], dur, r['tariff_hold'], ok])
    
    f.seek(0)
    await c.message.answer_document(BufferedInputFile(f.getvalue().encode(), filename="rep.csv"), caption=f"📊 {h}ч")
    await c.answer()

# --- СИСТЕМНЫЕ ---
@router.message(Command("startwork"))
async def sys_on(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    await m.answer("Тариф:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def sys_bind(c: CallbackQuery):
    t = c.data.split("_")[1]
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    cid = c.message.chat.id
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"t_{cid}_{tid}", t))
        await db.commit()
    await c.message.edit_text(f"✅ Топик настроен на <b>{t}</b>!", parse_mode="HTML")

# --- РАССЫЛКА / ПОДДЕРЖКА / ДОСТУП ---
# (Код аналогичен прошлому, сократил для лимита, но функции есть)
@router.callback_query(F.data == "adm_cast")
async def cast1(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("Пост:")

@router.message(AdminState.waiting_broadcast)
async def cast2(m: Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳")
    async with get_db() as db:
        us = await (await db.execute("SELECT user_id FROM users")).fetchall()
    n = 0
    for u in us:
        try:
            await m.copy_to(u['user_id'])
            n+=1
            await asyncio.sleep(0.05)
        except: pass
    await msg.edit_text(f"✅ {n}")

@router.callback_query(F.data == "ask_supp")
async def sup1(c: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙", callback_data="back_main")
    await c.message.edit_text("Вопрос:", reply_markup=kb.as_markup())
    await state.set_state(UserState.waiting_support)

@router.message(UserState.waiting_support)
async def sup2(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="Отв", callback_data=f"reply_{m.from_user.id}")
    try: await bot.send_message(ADMIN_ID, f"🆘 {m.from_user.id}:\n{m.text}", reply_markup=kb.as_markup())
    except: pass
    await m.answer("✅")

@router.callback_query(F.data.startswith("reply_"))
async def sup3(c: CallbackQuery, state: FSMContext):
    await state.update_data(ruid=c.data.split("_")[1])
    await state.set_state(AdminState.support_reply)
    await c.message.answer("Ответ:")

@router.message(AdminState.support_reply)
async def sup4(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    try: await bot.send_message(d['ruid'], f"👨‍💻 {m.text}")
    except: pass
    await state.clear()
    await m.answer("✅")

@router.callback_query(F.data == "back_main")
async def back(c: CallbackQuery): await c.message.edit_text("Меню", reply_markup=main_kb(c.from_user.id))

@router.callback_query(F.data.startswith("acc_"))
async def acc(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        if act=="ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await bot.send_message(uid, "✅ Доступ!")
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
        await db.commit()
    await c.message.delete()

# --- START ---
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(afk_monitor(bot))
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
