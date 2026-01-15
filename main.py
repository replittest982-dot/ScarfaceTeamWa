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
    sys.exit("pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================

TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "fast_team_final.db"

# Таймеры
AFK_CHECK_MINUTES = 8
AFK_KICK_MINUTES = 3
CODE_WAIT_MINUTES = 4
SEP = "━━━━━━━━━━━━━━━━━━━━"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
                last_afk_check TEXT, reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT,
                tariff_name TEXT, tariff_price TEXT, work_time TEXT,
                status TEXT DEFAULT 'queue',
                worker_id INTEGER DEFAULT 0, worker_chat_id INTEGER DEFAULT 0,
                worker_thread_id INTEGER DEFAULT 0,
                start_time TEXT, end_time TEXT, wait_code_start TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY, price TEXT, work_time TEXT
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_num INTEGER PRIMARY KEY, chat_id INTEGER, title TEXT
            )""")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")

        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('WhatsApp','50₽','10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('MAX','10$','24/7')")
        await db.commit()
    logger.info("✅ Database initialized (FINAL MERGED)")

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

def get_now():
    return datetime.now(timezone.utc).isoformat()

def format_time(iso_str):
    try: return (datetime.fromisoformat(iso_str) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
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
# FSM СОСТОЯНИЯ
# ==========================================

class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_help = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_price = State()
    edit_time = State()
    help_reply = State()
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
# КОМАНДЫ
# ==========================================

@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    username = m.from_user.username or "NoUsername"
    first_name = m.from_user.first_name or "User"

    async with get_db() as db:
        user = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()

        if not user:
            await db.execute(
                "INSERT INTO users (user_id, username, first_name, last_afk_check) VALUES (?, ?, ?, ?)", 
                (uid, username, first_name, get_now())
            )
            await db.commit()
            
            if ADMIN_ID:
                try:
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), 
                        InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                    ]])
                    await m.bot.send_message(ADMIN_ID, f"👤 <b>Новый запрос:</b>\nID: {uid}\n@{username}", reply_markup=kb, parse_mode="HTML")
                except: pass
            
            return await m.answer("🔒 <b>Доступ ограничен.</b>\nОжидайте одобрения администратора.", parse_mode="HTML")

        if user['is_banned']:
            return await m.answer("🚫 Вы заблокированы.")
        
        if user['is_approved']:
            return await m.answer(f"👋 Привет, <b>{first_name}</b>!\n{SEP}", reply_markup=main_kb(uid), parse_mode="HTML")
        else:
            return await m.answer("⏳ Ваша заявка все еще на рассмотрении.")

@router.message(Command("bindgroup"))
async def cmd_bindgroup(m: Message, command: CommandObject):
    if m.from_user.id != ADMIN_ID: return

    if not command.args:
        return await m.reply("❌ Использование: /bindgroup 1")

    try:
        group_num = int(command.args.strip())
        if group_num not in [1, 2, 3]: raise ValueError
    except:
        return await m.reply("❌ Номер группы: 1, 2 или 3")

    chat_id = m.chat.id
    title = m.chat.title or f"Chat {chat_id}"

    async with get_db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO groups (group_num, chat_id, title) VALUES (?, ?, ?)",
            (group_num, chat_id, title)
        )
        await db.commit()

    await m.answer(
        f"✅ Чат привязан к группе {group_num}!\n\n"
        f"👨‍💻 Гайд:\n"
        f"1️⃣ /num -> Получить номер\n"
        f"2️⃣ /sms +77... текст -> Отправить сообщение\n"
        f"3️⃣ /code +77... -> Запросить код\n"
        f"4️⃣ ✅ Встал -> Подтвердить\n"
        f"5️⃣ 📉 Слет -> Отметить слет"
    )

@router.message(Command("startwork"))
async def cmd_startwork(m: Message):
    if m.from_user.id != ADMIN_ID: return

    async with get_db() as db:
        tariffs = await (await db.execute("SELECT name FROM tariffs")).fetchall()

    kb = InlineKeyboardBuilder()
    for t in tariffs: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    kb.adjust(1)

    await m.answer("⚙️ Выберите тариф для топика:", reply_markup=kb.as_markup())

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    tid = m.message_thread_id if m.is_topic_message else 0
    key = f"topic_{m.chat.id}_{tid}"

    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (key,))).fetchone()
        if not conf: return await m.reply(f"❌ Топик не настроен. Используйте /startwork")
        
        tariff_name = conf['value']
        
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1",
            (tariff_name,)
        )).fetchone()
        
        if not row: return await m.reply("📭 Очередь пуста")
        
        await db.execute("""
            UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?
        """, (m.from_user.id, m.chat.id, tid, get_now(), row['id']))
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), row['user_id']))
        await db.commit()

    # Сообщение воркеру
    if "MAX" in tariff_name.upper():
        msg = (
            f"🚀 <b>Вы взяли номер</b>\n{SEP}\n"
            f"📱 <code>{row['phone']}</code>\n"
            f"💰 {row['tariff_price']}\n\n"
            f"Код: <code>/code {row['phone']}</code>"
        )
        kb = worker_kb_max(row['id'])
    else:
        msg = (
            f"🚀 <b>Вы взяли номер</b>\n{SEP}\n"
            f"📱 <code>{row['phone']}</code>\n"
            f"💰 {row['tariff_price']}\n\n"
            f"Код: <code>/sms {row['phone']} текст</code>"
        )
        kb = worker_kb_whatsapp(row['id'])

    await m.answer(msg, reply_markup=kb, parse_mode="HTML")

    try:
        await bot.send_message(
            row['user_id'],
            f"⚡ <b>Ваш номер взяли!</b>\n📱 {mask_phone(row['phone'], row['user_id'])}\nОжидайте код.",
            parse_mode="HTML"
        )
    except: pass

@router.message(Command("code"))
async def cmd_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args:
        return await m.reply("⚠️ Пример: <code>/code +7999…</code>", parse_mode="HTML")

    ph = clean_phone(command.args.split()[0])

    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')",
            (ph,)
        )).fetchone()

    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Не ваш номер")

    async with get_db() as db:
        await db.execute(
            "UPDATE numbers SET wait_code_start=? WHERE id=?",
            (get_now(), row['id'])
        )
        await db.commit()

    try:
        await bot.send_message(
            row['user_id'],
            f"🔔 <b>Офис запросил код</b>\n{SEP}\n"
            f"📱 {mask_phone(row['phone'], row['user_id'])}\n\n"
            f"Ответьте сообщением ниже",
            parse_mode="HTML"
        )
        await m.reply("✅ Запрос отправлен юзеру")
    except:
        await m.reply("❌ Ошибка доставки")

# ==========================================
# CALLBACK HANDLERS
# ==========================================

@router.callback_query(F.data == "back_main")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Главное меню\n{SEP}", reply_markup=main_kb(c.from_user.id))
    await c.answer()

@router.callback_query(F.data == "guide")
async def cb_guide(c: CallbackQuery):
    kb = InlineKeyboardBuilder().button(text="🔙 Меню", callback_data="back_main")
    await c.message.edit_text(
        f"📲 <b>Что делает бот</b>\n"
        f"Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
        f"📦 <b>Требования к номерам</b>\n"
        f"✔️ Активный и чистый номер\n"
        f"✔️ Доступ к SMS\n"
        f"❌ Виртуальные номера не принимаются\n\n"
        f"⏳ <b>Холд и выплаты</b>\n"
        f"Холд — время проверки номера\n"
        f"💰 Выплата после успешного завершения холда",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status IN ('work','active')", (uid,))).fetchone())[0]
        queue = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='queue'", (uid,))).fetchone())[0]

    kb = InlineKeyboardBuilder()
    if queue > 0: kb.button(text="📝 Мои номера", callback_data="my_nums")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)

    await c.message.edit_text(
        f"👤 <b>Личный кабинет</b>\n{SEP}\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📦 Всего сдано: {total}\n"
        f"🔥 В работе: {active}\n"
        f"🟡 В очереди: {queue}",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data == "my_nums")
async def cb_my_nums(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        rows = await (await db.execute(
            "SELECT id, phone, status, tariff_price FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 10",
            (uid,)
        )).fetchall()

    kb = InlineKeyboardBuilder()
    txt = f"📝 <b>Ваши номера в очереди</b>\n{SEP}\n"

    if not rows:
        txt += "📭 Очередь пуста"
    else:
        for i, r in enumerate(rows, 1):
            txt += f"{i}. {mask_phone(r['phone'], uid)} | {r['tariff_price']}\n"
            kb.button(text=f"🗑 Удалить #{i}", callback_data=f"del_{r['id']}")

    kb.button(text="🔙 Назад", callback_data="profile")
    kb.adjust(1)

    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("del_"))
async def cb_del(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT status FROM numbers WHERE id=? AND user_id=?",
            (nid, c.from_user.id)
        )).fetchone()
        
        if row and row['status'] == 'queue':
            await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
            await db.commit()
            await c.answer("✅ Номер удален")
            await cb_my_nums(c)
        else:
            await c.answer("❌ Номер уже в работе!", show_alert=True)

@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()

    kb = InlineKeyboardBuilder()
    for t in tariffs:
        kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)

    await c.message.edit_text(f"📂 <b>Выберите тариф</b>\n{SEP}", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_", 1)[1]
    async with get_db() as db:
        t = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (tn,))).fetchone()

    await state.update_data(tariff=tn, price=t['price'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)

    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")

    await c.message.edit_text(
        f"💎 <b>Тариф: {tn}</b>\n{SEP}\n"
        f"💰 Прайс: {t['price']}\n"
        f"⏰ Время работы: {t['work_time']}\n\n"
        f"📱 Отправьте номера списком (каждый с новой строки)",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data == "ask_help")
async def cb_ask_help(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_help)
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")

    await c.message.edit_text(
        f"🆘 <b>Поддержка</b>\n{SEP}\nНапишите ваш вопрос:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    tn = c.data.split("_", 1)[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0

    async with get_db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (f"topic_{cid}_{tid}", tn)
        )
        await db.commit()

    await c.message.edit_text(
        f"✅ <b>Топик привязан к тарифу: {tn}</b>\n\n"
        f"Используйте /num для получения номера",
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data.startswith("w_act_"))
async def cb_w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ваш номер!", show_alert=True)
        
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()

    await c.message.edit_text(
        f"✅ <b>Номер встал</b>\n📱 {row['phone']}",
        reply_markup=worker_active_kb(nid),
        parse_mode="HTML"
    )

    try:
        await bot.send_message(row['user_id'], "✅ Номер встал и работает!")
    except: pass
    await c.answer()

@router.callback_query(F.data.startswith("w_skip_"))
async def cb_w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ваш номер!", show_alert=True)
        
        await db.execute(
            "UPDATE numbers SET status='queue', worker_id=0, worker_chat_id=0, worker_thread_id=0 WHERE id=?",
            (nid,)
        )
        await db.commit()

    await c.message.edit_text("⏭ <b>Пропуск</b>\nНомер вернулся в очередь", parse_mode="HTML")

    try:
        await bot.send_message(row['user_id'], "⏭ Офис пропустил ваш номер")
    except: pass
    await c.answer()

@router.callback_query(F.data.startswith(("w_drop_", "w_err_")))
async def cb_w_finish(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row or row['worker_id'] != c.from_user.id:
            return await c.answer("🚫 Не ваш номер!", show_alert=True)
        
        status = "finished" if is_drop else "dead"
        end_time = get_now()
        duration = calc_duration(row['start_time'], end_time)
        
        await db.execute(
            "UPDATE numbers SET status=?, end_time=? WHERE id=?",
            (status, end_time, nid)
        )
        await db.commit()

    if is_drop:
        msg = f"📉 <b>Слет</b>\n⏱ {duration}"
        user_msg = f"📉 Ваш номер слетел\nВремя работы: {duration}"
    else:
        msg = "❌ <b>Ошибка</b>"
        user_msg = "❌ Произошла ошибка с вашим номером"

    await c.message.edit_text(msg, parse_mode="HTML")

    try:
        await bot.send_message(row['user_id'], user_msg)
    except: pass
    await c.answer()

@router.callback_query(F.data.startswith("acc_"))
async def cb_acc(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    action, uid = c.data.split("_")[1], int(c.data.split("_")[2])

    async with get_db() as db:
        if action == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await db.commit()
            await c.message.edit_text(f"✅ Юзер {uid} принят")
            
            try:
                await bot.send_message(uid, "✅ Доступ открыт!\nЖмите /start")
            except: pass
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            await db.commit()
            await c.message.edit_text(f"🚫 Юзер {uid} забанен")

    await c.answer()

@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    if c.from_user.id != uid:
        return await c.answer("🚫 Не для вас!", show_alert=True)
        
    async with get_db() as db:
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
        await db.commit()

    try: await c.message.delete()
    except: pass

    await c.answer("✅ Активность подтверждена!")

# ==========================================
# АДМИНКА
# ==========================================

@router.callback_query(F.data == "admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы", callback_data="adm_tariffs")
    kb.button(text="📊 Отчеты", callback_data="adm_reports")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🏢 Группы", callback_data="manage_groups")
    kb.button(text="📋 Общая очередь", callback_data="all_queue")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)

    await c.message.edit_text("⚡ <b>Админ панель</b>\n{SEP}", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "all_queue")
async def cb_all_queue(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        queue = await (await db.execute(
            "SELECT id, phone, tariff_name FROM numbers WHERE status='queue' ORDER BY id ASC"
        )).fetchall()
        
        active = await (await db.execute(
            "SELECT id, phone, tariff_name, worker_id FROM numbers WHERE status IN ('work', 'active') ORDER BY id ASC"
        )).fetchall()

    txt = f"📋 <b>ОБЩАЯ ОЧЕРЕДЬ</b>\n{SEP}\n\n"

    txt += f"🟡 <b>В ОЧЕРЕДИ ({len(queue)}):</b>\n"
    if queue:
        for i, r in enumerate(queue[:20], 1):
            txt += f"{i}. {r['phone']} | {r['tariff_name']}\n"
        if len(queue) > 20:
            txt += f"...и еще {len(queue) - 20} номеров\n"
    else:
        txt += "Пусто\n"

    txt += f"\n🟢 <b>В РАБОТЕ ({len(active)}):</b>\n"
    if active:
        for r in active[:20]:
            txt += f"📱 {r['phone']} | {r['tariff_name']} | Воркер: {r['worker_id']}\n"
        if len(active) > 20:
            txt += f"...и еще {len(active) - 20} номеров\n"
    else:
        txt += "Пусто\n"

    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="admin_main")

    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "manage_groups")
async def cb_mgr(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        groups = await (await db.execute("SELECT * FROM groups ORDER BY group_num")).fetchall()

    kb = InlineKeyboardBuilder()

    for i in range(1, 4):
        g_name = "Не привязана"
        for g in groups:
            if g['group_num'] == i:
                g_name = g['title']
                break
        
        kb.button(text=f"🛑 Стоп: {g_name}", callback_data=f"stop_group_{i}")

    kb.button(text="📊 Статус", callback_data="groups_status")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)

    await c.message.edit_text(
        "🏢 <b>Управление группами</b>\n{SEP}",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data.startswith("stop_group_"))
async def cb_stop_g(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    gn = int(c.data.split("_")[-1])
    stop_time = get_now()

    async with get_db() as db:
        g = await (await db.execute("SELECT * FROM groups WHERE group_num=?", (gn,))).fetchone()
        
        if not g:
            return await c.answer(f"❌ Группа {gn} не привязана!", show_alert=True)
        
        cid, title = g['chat_id'], g['title']
        
        nums = await (await db.execute("""
            SELECT id, user_id, phone, start_time 
            FROM numbers 
            WHERE status IN ('work','active') AND worker_chat_id=?
        """, (cid,))).fetchall()
        
        stopped = 0
        for num in nums:
            await db.execute(
                "UPDATE numbers SET status=?, end_time=? WHERE id=?",
                (f"finished_group_{gn}", stop_time, num['id'])
            )
            stopped += 1
            
            duration = calc_duration(num['start_time'], stop_time)
            try:
                await bot.send_message(
                    num['user_id'],
                    f"🛑 <b>{title} остановлен</b>\n{SEP}\n"
                    f"📱 {mask_phone(num['phone'], num['user_id'])}\n"
                    f"⏰ {format_time(stop_time)}\n"
                    f"⏱ Работа: {duration}",
                    parse_mode="HTML"
                )
            except: pass
        
        await db.commit()

    await c.message.edit_text(
        f"🛑 <b>Группа {gn} остановлена</b>\n{SEP}\n"
        f"🏢 {title}\n"
        f"⏰ {format_time(stop_time)}\n"
        f"📦 Остановлено: {stopped}",
        parse_mode="HTML"
    )
    await c.answer()

@router.callback_query(F.data == "groups_status")
async def cb_g_stat(c: CallbackQuery):
    async with get_db() as db:
        stats = {}
        for i in range(1, 4):
            stats[f"Группа {i}"] = (await (await db.execute(
                "SELECT COUNT(*) FROM numbers WHERE status=?", 
                (f"finished_group_{i}",)
            )).fetchone())[0]

        active = (await (await db.execute(
            "SELECT COUNT(*) FROM numbers WHERE status IN ('work','active')"
        )).fetchone())[0]
        
        queue = (await (await db.execute(
            "SELECT COUNT(*) FROM numbers WHERE status='queue'"
        )).fetchone())[0]

    txt = f"📊 <b>СТАТУС</b>\n{SEP}\n"
    for g, cnt in stats.items():
        txt += f"🏁 {g}: {cnt}\n"
    txt += f"\n🔥 Активно: {active}\n🟡 Очередь: {queue}"

    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="manage_groups")

    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data == "adm_tariffs")
async def cb_adm_t(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()

    kb = InlineKeyboardBuilder()
    for t in ts:
        kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)

    await c.message.edit_text("🛠 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")
    await c.answer()

@router.callback_query(F.data.startswith("ed_"))
async def cb_ed_t(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    target = c.data.split("_", 1)[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_price)

    await c.message.edit_text(
        f"1️⃣ Введите ЦЕНУ для {target}\nПример: 50₽, 10$"
    )
    await c.answer()

@router.callback_query(F.data == "adm_reports")
async def cb_adm_r(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.report_hours)

    await c.message.edit_text(
        "📊 Введите количество часов для отчета (до 120):"
    )
    await c.answer()

@router.callback_query(F.data == "adm_cast")
async def cb_cast(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 Пришлите пост для рассылки:")
    await c.answer()

@router.callback_query(F.data.startswith("helpreply_"))
async def cb_helpreply(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    uid = c.data.split("_")[1]
    await state.update_data(help_uid=uid)
    await state.set_state(AdminState.help_reply)

    await c.message.answer(f"✍️ Введите ответ для {uid}:")
    await c.answer()

# ==========================================
# FSM HANDLERS
# ==========================================

@router.message(UserState.waiting_numbers)
async def fsm_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = [clean_phone(x.strip()) for x in raw if clean_phone(x.strip())]

    if not valid:
        return await m.reply("❌ Не найдено валидных номеров")

    async with get_db() as db:
        for ph in valid:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, work_time) VALUES (?, ?, ?, ?, ?)",
                (m.from_user.id, ph, data['tariff'], data['price'], data.get('work_time', ''))
            )
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), m.from_user.id))
        await db.commit()

    await state.clear()
    await m.answer(
        f"✅ Принято: {len(valid)} шт\n{SEP}\nДобавлено в очередь",
        reply_markup=main_kb(m.from_user.id)
    )

@router.message(UserState.waiting_help)
async def fsm_help(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(
        text="💬 Ответить",
        callback_data=f"helpreply_{m.from_user.id}"
    )

    try:
        await bot.send_message(
            ADMIN_ID,
            f"🆘 <b>Новый запрос</b>\n{SEP}\n"
            f"От: {m.from_user.id} (@{m.from_user.username})\n\n"
            f"{m.text}",
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
        await m.answer(
            "✅ Запрос отправлен\nОтвет будет направлен вам",
            reply_markup=main_kb(m.from_user.id)
        )
    except Exception as e:
        logger.error(f"Help error: {e}")
        await m.answer("❌ Ошибка отправки")

@router.message(AdminState.help_reply)
async def fsm_helpreply(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()

    try:
        await bot.send_message(
            data['help_uid'],
            f"👨‍💻 <b>Ответ на ваш запрос:</b>\n{SEP}\n{m.text}",
            parse_mode="HTML"
        )
        await m.answer("✅ Ответ отправлен")
    except:
        await m.answer("❌ Не доставлено")

@router.message(AdminState.waiting_broadcast)
async def fsm_cast(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    msg = await m.answer("⏳ Рассылка...")

    async with get_db() as db:
        users = await (await db.execute(
            "SELECT user_id FROM users WHERE is_approved=1"
        )).fetchall()

    success, fail = 0, 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            success += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            fail += 1
        except:
            fail += 1

    await msg.edit_text(
        f"📢 <b>Рассылка завершена</b>\n{SEP}\n"
        f"✅ Доставлено: {success}\n"
        f"❌ Ошибок: {fail}\n"
        f"📊 Всего: {len(users)}",
        parse_mode="HTML"
    )

@router.message(AdminState.edit_price)
async def fsm_ep(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_time)
    await m.answer("2️⃣ Введите ВРЕМЯ РАБОТЫ\nПример: 10:00-22:00 МСК, 24/7")

@router.message(AdminState.edit_time)
async def fsm_et(m: Message, state: FSMContext):
    data = await state.get_data()
    async with get_db() as db:
        await db.execute(
            "UPDATE tariffs SET price=?, work_time=? WHERE name=?",
            (data['price'], m.text, data['target'])
        )
        await db.commit()

    await state.clear()
    await m.answer(
        f"✅ <b>Тариф обновлен!</b>\n{SEP}\n"
        f"💰 {data['price']}\n"
        f"⏰ {m.text}",
        parse_mode="HTML"
    )

@router.message(AdminState.report_hours)
async def fsm_rep(m: Message, state: FSMContext):
    await state.clear()
    try:
        hours = int(m.text)
        if hours < 1 or hours > 120:
            return await m.answer("❌ Введите число от 1 до 120")
    except:
        return await m.answer("❌ Введите корректное число")

    cut_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT * FROM numbers 
            WHERE created_at >= ? 
            ORDER BY id DESC
        """, (cut_time,))).fetchall()

    if not rows:
        return await m.answer("📂 Пусто")

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'UserID', 'Phone', 'Status', 'Tariff', 'Created', 'Start', 'End', 'Duration'])

    for r in rows:
        duration = calc_duration(r['start_time'], r['end_time'])
        w.writerow([
            r['id'], r['user_id'], r['phone'], r['status'],
            r['tariff_name'], format_time(r['created_at']),
            format_time(r['start_time']), format_time(r['end_time']), duration
        ])

    out.seek(0)
    await m.answer_document(
        BufferedInputFile(out.getvalue().encode(), filename=f"report_{hours}h.csv"),
        caption=f"📊 Отчет за {hours}ч"
    )

# ==========================================
# РАБОТА С ФОТО И СООБЩЕНИЯМИ
# ==========================================

@router.message(F.photo & F.caption)
async def handle_photo(m: Message, bot: Bot):
    # Обработка фото от воркера
    if m.chat.type != "private":
        match = re.search(r'/sms\s+([+\d]+)\s*(.*)', m.caption, flags=re.DOTALL)
        if match:
            ph = clean_phone(match.group(1))
            text_for_user = match.group(2).strip() or "Вам сообщение от офиса"
            
            if not ph: return await m.reply("❌ Неверный номер")
            
            async with get_db() as db:
                row = await (await db.execute(
                    "SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')",
                    (ph,)
                )).fetchone()
            
            if not row: return await m.reply("❌ Номер не в работе")
            if row['worker_id'] != m.from_user.id: return await m.reply("🚫 Не ваш номер")
            
            try:
                await bot.send_photo(
                    chat_id=row['user_id'],
                    photo=m.photo[-1].file_id,
                    caption=f"📩 <b>Сообщение от офиса:</b>\n{SEP}\n{text_for_user}",
                    parse_mode="HTML"
                )
                await m.react([ReactionTypeEmoji(emoji="👌")])
            except Exception as e:
                await m.reply(f"❌ Не доставлено: {e}")

# ==========================================
# ГЛАВНЫЙ ОБРАБОТЧИК СООБЩЕНИЙ (ПОСЛЕДНИЙ!)
# ==========================================

@router.message(F.chat.type == "private")
async def handle_msg(m: Message, bot: Bot, state: FSMContext):
    # Пропускаем команды
    if m.text and m.text.startswith('/'): return

    # Пропускаем админа
    if m.from_user.id == ADMIN_ID: return

    # Проверяем FSM
    cs = await state.get_state()
    if cs: return

    # Ищем активный номер юзера
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')",
            (m.from_user.id,)
        )).fetchone()

    if row and row['worker_chat_id']:
        # Сбрасываем таймер кода если был запрос
        if row['wait_code_start']:
            async with get_db() as db:
                await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
                await db.commit()
        
        # Отправляем в топик воркера
        try:
            tc = row['worker_chat_id']
            tt = row['worker_thread_id'] if row['worker_thread_id'] else None
            hdr = f"📩 <b>ОТВЕТ ЮЗЕРА</b>\n📱 {row['phone']}\n{SEP}\n"
            
            if m.text:
                await bot.send_message(
                    tc,
                    message_thread_id=tt,
                    text=f"{hdr}💬 {m.text}",
                    parse_mode="HTML"
                )
            elif m.photo:
                await bot.send_photo(
                    tc,
                    message_thread_id=tt,
                    photo=m.photo[-1].file_id,
                    caption=f"{hdr}📸",
                    parse_mode="HTML"
                )
            
            await m.react([ReactionTypeEmoji(emoji="⚡")])
            await m.reply("✅ Сообщение передано в офис")
        except Exception as e:
            logger.error(f"Bridge error: {e}")
            await m.reply("❌ Ошибка доставки")

# ==========================================
# МОНИТОРИНГ
# ==========================================

async def monitor(bot: Bot):
    logger.info("👀 Monitor started (FINAL)")
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        
        try:
            async with get_db() as db:
                # 1. Таймаут кода
                waiters = await (await db.execute("""
                    SELECT id, user_id, phone, worker_chat_id, worker_thread_id, wait_code_start 
                    FROM numbers 
                    WHERE status='active' AND wait_code_start IS NOT NULL
                """)).fetchall()
                
                for w in waiters:
                    st = datetime.fromisoformat(w['wait_code_start'])
                    if (now - st).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute(
                            "UPDATE numbers SET status='dead', end_time=?, wait_code_start=NULL WHERE id=?",
                            (get_now(), w['id'])
                        )
                        
                        try:
                            await bot.send_message(
                                w['user_id'],
                                f"⏰ Время истекло\n{w['phone']} отменен"
                            )
                            
                            if w['worker_chat_id']:
                                await bot.send_message(
                                    chat_id=w['worker_chat_id'],
                                    message_thread_id=w['worker_thread_id'] if w['worker_thread_id'] else None,
                                    text="⚠️ Таймаут кода!"
                                )
                        except: pass
                
                # 2. AFK проверка
                users = await (await db.execute("""
                    SELECT DISTINCT u.user_id, u.last_afk_check 
                    FROM users u 
                    JOIN numbers n ON u.user_id = n.user_id 
                    WHERE n.status = 'queue'
                """)).fetchall()
                
                for u in users:
                    uid = u['user_id']
                    last = u['last_afk_check']
                    
                    if not last or (not str(last).startswith("PENDING") and (now - datetime.fromisoformat(last)).total_seconds() / 60 > AFK_CHECK_MINUTES):
                        kb = InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text="👋 Я тут!", callback_data=f"afk_ok_{uid}")
                        ]])
                        try:
                            await bot.send_message(
                                uid,
                                f"⚠️ <b>Проверка активности!</b>\n{SEP}\nНажмите кнопку",
                                reply_markup=kb,
                                parse_mode="HTML"
                            )
                            await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (f"PENDING_{get_now()}", uid))
                        except TelegramForbiddenError:
                            await db.execute("DELETE FROM numbers WHERE user_id=? AND status='queue'", (uid,))
                        except: pass
                    
                    elif str(last).startswith("PENDING_"):
                        pt = datetime.fromisoformat(last.split("_")[1])
                        if (now - pt).total_seconds() / 60 > AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE user_id=? AND status='queue'", (uid,))
                            await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
                            try:
                                await bot.send_message(uid, "❌ Заявки удалены из-за неактивности")
                            except: pass
                
                await db.commit()
                
        except Exception as e:
            logger.exception(f"Monitor Error: {e}")
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

    logger.info("🚀 BOT STARTED - FINAL MERGED VERSION")

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
