import asyncio
import logging
import sys
import os
import re
import csv
import io
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

# --- ИМПОРТЫ ---
try:
    import aiosqlite
    from aiogram import Bot, Dispatcher, Router, F, types
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
    sys.exit("❌ Установите библиотеки: pip install aiogram aiosqlite")

# ==========================================
# 1. КОНФИГУРАЦИЯ
# ==========================================
# Вставь свой токен и ID админа
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_ТОКЕН_СЮДА") 
ADMIN_ID = int(os.getenv("ADMIN_ID", "12345678")) 
DB_NAME = "bot_v80_final.db" 

# Таймеры (в минутах)
AFK_CHECK_MINUTES = 8      # Проверка AFK в очереди
CODE_WAIT_MINUTES = 4      # Ожидание кода от юзера

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

# ==========================================
# 2. БАЗА ДАННЫХ
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
        # Юзеры
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
            reg_date TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        # Номера (полная структура)
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            tariff_name TEXT, tariff_price TEXT, tariff_time TEXT, 
            status TEXT DEFAULT 'queue', 
            worker_id INTEGER DEFAULT 0, 
            worker_chat_id INTEGER DEFAULT 0,
            worker_thread_id INTEGER DEFAULT 0,
            start_time TEXT, end_time TEXT, last_ping TEXT,
            wait_code_start TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        # Тарифы
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            name TEXT PRIMARY KEY, price TEXT, work_time TEXT)""")
        
        # Конфиг топиков
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '24/7')")
        
        await db.commit()
    logger.info("✅ DB Loaded v80.1 (Final)")

# ==========================================
# 3. УТИЛИТЫ
# ==========================================
def clean_phone(phone: str):
    """Чистит номер, приводит к +7..."""
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    return '+' + clean if re.match(r'^7\d{10}$', clean) else None

def mask_phone(phone, user_id):
    """Скрывает номер от посторонних"""
    if user_id == ADMIN_ID: return phone
    try:
        if len(phone) < 9: return phone
        return f"{phone[:5]}***{phone[-4:]}"
    except: return phone

def get_now(): return datetime.now(timezone.utc).isoformat()

def format_time(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        return (dt + timedelta(hours=3)).strftime("%d.%m %H:%M") # МСК
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
# 4. FSM И КЛАВИАТУРЫ
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_support = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_time = State()
    edit_price = State()
    support_reply = State()

def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Задать вопрос", callback_data="ask_supp")
    if user_id == ADMIN_ID: kb.button(text="⚡ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 1, 1)
    return kb.as_markup()

def worker_kb(nid, tariff_name):
    kb = InlineKeyboardBuilder()
    if "MAX" in tariff_name.upper():
        kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
        kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{nid}")
    else:
        kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
        kb.button(text="❌ Ошибка", callback_data=f"w_err_{nid}")
    return kb.as_markup()

def worker_active_kb(nid):
    return InlineKeyboardBuilder().button(text="📉 Слет", callback_data=f"w_drop_{nid}").as_markup()

# ==========================================
# 5. ЮЗЕРСКАЯ ЧАСТЬ
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
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), 
                    InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                ]])
                try: 
                    await m.bot.send_message(ADMIN_ID, 
                        f"👤 <b>Запрос доступа:</b>\nID: {uid}\n@{m.from_user.username}", 
                        reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте подтверждения доступа.</b>", parse_mode="HTML")
        
        if res['is_banned']: 
            return await m.answer("🚫 <b>Вы забанены.</b>", parse_mode="HTML")
        if res['is_approved']: 
            await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=main_kb(uid))
        else: 
            await m.answer("⏳ <b>Ваша заявка на рассмотрении.</b>", parse_mode="HTML")

@router.callback_query(F.data == "guide")
async def show_guide(c: CallbackQuery):
    txt = ("📲 <b>Что делает бот</b>\n"
           "Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
           "📦 <b>Требования к номерам</b>\n"
           "✔️ Активный и чистый номер\n"
           "✔️ Доступ к SMS\n"
           "❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
           "⏳ <b>Холд и выплаты</b>\n"
           "Холд — время проверки номера\n"
           "💰 Выплата производится после успешного завершения холда\n\n"
           "⚠️ <i>Отправляя номер, вы подтверждаете, что ознакомились с правилами</i>")
    await c.message.edit_text(txt, reply_markup=main_kb(c.from_user.id), parse_mode="HTML")

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='active'", (uid,))).fetchone())[0]
        q_pos = 0
        my_first = await (await db.execute("SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1", (uid,))).fetchone()
        if my_first:
            q_pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?", (my_first[0],))).fetchone())[0] + 1
        
    txt = (f"👤 <b>Профиль</b>\n"
           f"📦 Всего номеров: {total}\n"
           f"🔥 В работе: {active}\n"
           f"🕒 <b>Очередь:</b> Перед вами заявок: {q_pos}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Мои номера", callback_data="my_nums")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "my_nums")
async def my_nums(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        rows = await (await db.execute(
            "SELECT id, phone, status, tariff_price FROM numbers WHERE user_id=? ORDER BY id DESC LIMIT 10", 
            (uid,)
        )).fetchall()
    
    kb = InlineKeyboardBuilder()
    txt = "📝 <b>Ваши последние 10 номеров:</b>\n\n"
    if not rows: txt += "Пусто."
    for r in rows:
        icon = "🟡" if r['status']=='queue' else "🟢" if r['status']=='active' else "✅" if r['status']=='finished' else "❌"
        txt += f"{icon} {mask_phone(r['phone'], uid)} | {r['tariff_price']}\n"
        if r['status'] == 'queue':
             kb.button(text=f"🗑 Удалить {mask_phone(r['phone'], uid)}", callback_data=f"del_{r['id']}")

    kb.button(text="🔙 Назад", callback_data="profile")
    kb.adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("del_"))
async def delete_num(c: CallbackQuery):
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
            await my_nums(c)
        else:
            await c.answer("❌ Номер уже в работе или не найден!", show_alert=True)

# --- СДАЧА НОМЕРА ---
@router.callback_query(F.data == "sel_tariff")
async def sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for r in rows: 
        kb.button(text=f"{r['name']} | {r['price']}", callback_data=f"pick_{r['name']}")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("📂 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def pick_t(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("_")[1]
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (t_name,))).fetchone()
    
    await state.update_data(tariff=t_name, price=res['price'], time=res['work_time'])
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text(
        f"💎 Тариф: <b>{t_name}</b>\n💰 Прайс: {res['price']}\n⏰ Время: {res['work_time']}\n\n"
        f"📱 <b>Введите номера (списком или +7...):</b>", 
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )
    await state.set_state(UserState.waiting_numbers)

@router.message(UserState.waiting_numbers)
async def proc_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]', m.text)
    valid = []
    for x in raw:
        ph = clean_phone(x.strip())
        if ph: valid.append(ph)
    
    if not valid: 
        return await m.reply("❌ Не найдено валидных номеров.")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_time, last_ping) VALUES (?, ?, ?, ?, ?, ?)",
                (m.from_user.id, ph, data['tariff'], data['price'], data['time'], get_now())
            )
        await db.commit()
    
    await state.clear()
    await m.answer(
        f"✅ <b>Принято: {len(valid)} шт.</b>\nОжидайте обработки.", 
        reply_markup=main_kb(m.from_user.id), 
        parse_mode="HTML"
    )

# ==========================================
# 6. ВОРКЕР: СИСТЕМА
# ==========================================
@router.message(Command("startwork"))
async def sys_start(m: Message):
    if m.from_user.id != ADMIN_ID: return 
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: 
        kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    await m.answer("⚙️ <b>Выберите тариф для привязки:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("bind_"))
async def sys_bind(c: CallbackQuery):
    t = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", t))
        await db.commit()
    
    guide_txt = (f"✅ <b>Чат привязан!</b> Тариф: {t}\n\n"
                 f"👨‍💻 <b>Гайд по использованию:</b>\n\n"
                 f"1️⃣ Пиши /num -> Получишь номер.\n\n"
                 f"2️⃣ Вбей номер в WhatsApp Web.\n\n"
                 f"3️⃣ Если просят QR: Сфоткай QR с экрана.\n"
                 f"   Скинь фото сюда и подпиши: <code>/sms +77... Сканируй</code>\n\n"
                 f"4️⃣ Если просят Код (по номеру): Сфоткай код с экрана.\n"
                 f"   Скинь фото сюда и подпиши: <code>/sms +77... Вводи этот код</code>\n\n"
                 f"5️⃣ Когда зашел -> жми ✅ Встал.\n"
                 f"6️⃣ Когда номер слетел -> жми 📉 Слет.")
    
    await c.message.edit_text(guide_txt, parse_mode="HTML")

@router.message(Command("stopwork"))
async def sys_stop(m: Message):
    if m.from_user.id != ADMIN_ID: return
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (f"topic_{cid}_{tid}",))
        await db.commit()
    await m.reply("🛑 Топик отключен.")

# --- ВЗЯТИЕ НОМЕРА ---
@router.message(Command("num"))
async def worker_get_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: 
            return await m.reply(f"❌ Топик не привязан к тарифу.")
        
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", 
            (conf['value'],)
        )).fetchone()
        if not row: 
            return await m.reply("📭 Очередь пуста.")
        
        await db.execute(
            "UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?", 
            (m.from_user.id, cid, tid, get_now(), row['id'])
        )
        await db.commit()
    
    await m.answer(
        f"🚀 <b>В работе:</b>\n📱 <code>{row['phone']}</code>\nТариф: {row['tariff_name']}", 
        reply_markup=worker_kb(row['id'], row['tariff_name']), 
        parse_mode="HTML"
    )
    try: 
        await bot.send_message(
            row['user_id'], 
            f"⚡ <b>Ваш номер {mask_phone(row['phone'], row['user_id'])} в работе!</b>\nОжидайте пуш-уведомление / QR.", 
            parse_mode="HTML"
        )
    except: pass

# --- ЗАПРОС КОДА (MAX) ---
@router.message(Command("code"))
async def worker_code_req(m: Message, command: CommandObject, bot: Bot):
    if not command.args: 
        return await m.reply("⚠️ Пример: <code>/code +7999...</code>", parse_mode="HTML")
    
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", 
            (ph,)
        )).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: 
        return await m.reply("❌ Не ваш номер или не в работе.")
    
    # Ставим таймер для мониторинга
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now(), row['id']))
        await db.commit()

    try:
        await bot.send_message(
            row['user_id'], 
            f"🔔 <b>Офис запросил код!</b>\n📱 Номер: {mask_phone(row['phone'], row['user_id'])}\n\n"
            f"👇 <b>Напишите код ниже (текстом или фото).</b>", 
            parse_mode="HTML"
        )
        await m.reply(f"✅ Запрос отправлен юзеру.")
    except Exception as e:
        logger.error(f"Code request error: {e}")
        await m.reply("❌ Ошибка доставки (юзер блок?).")

# --- SMS ФОТО (WHATSAPP) ---
@router.message(F.photo & F.caption)
async def worker_photo_sms(m: Message, bot: Bot):
    if "/sms" not in m.caption.lower(): 
        return
    
    parts = m.caption.split()
    try:
        idx = next(i for i, p in enumerate(parts) if p.lower().startswith("/sms"))
        ph_raw = parts[idx+1]
        text = " ".join(parts[idx+2:]) if len(parts) > idx+2 else "Сканируй/Вводи код"
    except: 
        return await m.reply("⚠️ Формат: <code>/sms +7... текст</code>", parse_mode="HTML")
    
    ph = clean_phone(ph_raw)
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", 
            (ph,)
        )).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: 
        return await m.reply("❌ Это не ваш номер.")
    
    try:
        await bot.send_photo(
            row['user_id'], 
            m.photo[-1].file_id, 
            caption=f"🔔 <b>SMS / QR</b>\n{text}", 
            parse_mode="HTML"
        )
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except Exception as e:
        logger.error(f"Photo forward error: {e}")
        await m.reply("❌ Не доставлено.")

# --- ОТВЕТ ЮЗЕРА (ЛЮБОЕ СООБЩЕНИЕ) ---
@router.message(F.chat.type == "private")
async def user_any_msg(m: Message, bot: Bot, state: FSMContext):
    # Игнор команд и админа
    if m.text and m.text.startswith('/'): return
    if m.from_user.id == ADMIN_ID: return
    
    # Проверяем, не в FSM ли юзер (поддержка)
    current_state = await state.get_state()
    if current_state: return 
    
    # Ищем активный номер
    async with get_db() as db:
        row = await (await db.execute(
            "SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", 
            (m.from_user.id,)
        )).fetchone()
    
    # Если номер активен и есть чат воркера -> пересылаем
    if row and row['worker_chat_id']:
        msg_text = m.text or m.caption or "[Медиафайл]"
        
        # Сбрасываем таймер ожидания кода
        async with get_db() as db:
            await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
            await db.commit()
            
        try:
            target_chat = row['worker_chat_id']
            target_thread = row['worker_thread_id'] if row['worker_thread_id'] else None
            
            header = f"📩 <b>КОД ОТ ЮЗЕРА:</b>\n📱 {row['phone']}\n"
            
            if m.text:
                await bot.send_message(
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                    text=f"{header}💬 <code>{m.text}</code>", 
                    parse_mode="HTML"
                )
            elif m.photo:
                await bot.send_photo(
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                    photo=m.photo[-1].file_id,
                    caption=f"{header}📸 {msg_text}",
                    parse_mode="HTML"
                )
            else:
                await m.copy_to(
                    chat_id=target_chat,
                    message_thread_id=target_thread,
                    caption=f"{header}💬 {msg_text}",
                    parse_mode="HTML"
                )
                
            await m.answer("✅ Отправлено в офис.")
        except Exception as e:
            logger.error(f"❌ Forward Error: {e}")
            await m.answer("❌ Ошибка отправки.")

# --- КНОПКИ ВОРКЕРА ---
@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return await c.message.delete()
        if row['worker_id'] != c.from_user.id: 
            return await c.answer("🚫 Не ты брал!", show_alert=True)
            
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text(
        f"✅ <b>Встал:</b> {row['phone']}", 
        reply_markup=worker_active_kb(nid), 
        parse_mode="HTML"
    )
    try: 
        await bot.send_message(row['user_id'], "✅ <b>Номер успешно встал!</b>", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return await c.message.delete()
        if row['worker_id'] != c.from_user.id: 
            return await c.answer("🚫 Не ты брал!", show_alert=True)

        await db.execute("UPDATE numbers SET status='queue', worker_id=0, worker_chat_id=0 WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text("⏭ <b>Пропуск</b> (вернулся в очередь)", parse_mode="HTML")
    try: await bot.send_message(row['user_id'], "⚠️ Офис пропустил ваш номер.", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith(("w_drop_", "w_err_")))
async def w_finish(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return await c.message.delete()
        if row['worker_id'] != c.from_user.id: 
            return await c.answer("🚫 Не ты брал!", show_alert=True)
        
        status = "finished" if is_drop else "dead"
        dur = calc_duration(row['start_time'], get_now())
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, get_now(), nid))
        await db.commit()
    
    if is_drop:
        msg = f"📉 <b>Слет.</b> Время работы: {dur}"
        user_msg = f"📉 <b>Номер слетел.</b>\nВремя в работе: {dur}"
    else:
        msg = "❌ <b>Ошибка/Отмена.</b>"
        user_msg = "❌ <b>Номер отменен офисом.</b>"

    await c.message.edit_text(msg, parse_mode="HTML")
    try: await bot.send_message(row['user_id'], user_msg, parse_mode="HTML")
    except: pass

# ==========================================
# 7. АДМИНКА
# ==========================================
@router.callback_query(F.data == "admin_main")
async def adm_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы", callback_data="adm_tariffs")
    kb.button(text="📄 Отчеты (CSV)", callback_data="adm_reports")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- ОТЧЕТЫ CSV ---
@router.callback_query(F.data == "adm_reports")
async def adm_reports(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for h in [1, 24, 48]: 
        kb.button(text=f"За {h}ч", callback_data=f"rep_{h}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(3, 1)
    await c.message.edit_text("📅 <b>Выберите период отчета:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("rep_"))
async def adm_get_rep(c: CallbackQuery):
    h = int(c.data.split("_")[1])
    cut_time = (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat()
    
    async with get_db() as db:
        rows = await (await db.execute(
            "SELECT * FROM numbers WHERE created_at >= ? ORDER BY id DESC", 
            (cut_time,)
        )).fetchall()
    
    if not rows:
        return await c.answer("📂 Данных за этот период нет.")

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'User ID', 'Phone', 'Status', 'Tariff', 'Created', 'Start', 'End', 'Duration'])
    
    for r in rows:
        dur = calc_duration(r['start_time'], r['end_time'])
        w.writerow([
            r['id'], r['user_id'], r['phone'], r['status'], 
            r['tariff_name'], format_time(r['created_at']), 
            format_time(r['start_time']), format_time(r['end_time']), dur
        ])
    
    out.seek(0)
    file_data = BufferedInputFile(out.getvalue().encode(), filename=f"report_{h}h.csv")
    await c.message.answer_document(file_data, caption=f"📊 Отчет за последние {h} часов.")
    await c.answer()

# --- РЕДАКТОР ТАРИФОВ ---
@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in ts: 
        kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("🛠 <b>Выберите тариф для редактирования:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("ed_"))
async def ed_t1(c: CallbackQuery, state: FSMContext):
    target = c.data.split("_")[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_time)
    await c.message.edit_text(f"1️⃣ Введите новое <b>время работы</b> для {target}:\n(Например: 10:00-22:00)", parse_mode="HTML")

@router.message(AdminState.edit_time)
async def ed_t2(m: Message, state: FSMContext):
    await state.update_data(time=m.text)
    await state.set_state(AdminState.edit_price)
    await m.answer("2️⃣ Введите новую <b>цену</b>:\n(Например: 50₽ или 10$)", parse_mode="HTML")

@router.message(AdminState.edit_price)
async def ed_t3(m: Message, state: FSMContext):
    d = await state.get_data()
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, work_time=? WHERE name=?", (m.text, d['time'], d['target']))
        await db.commit()
    
    await state.clear()
    await m.answer("✅ <b>Тариф обновлен!</b>", reply_markup=main_kb(ADMIN_ID), parse_mode="HTML")

# --- РАССЫЛКА ---
@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 <b>Пришлите сообщение</b> (текст, фото, видео), которое нужно разослать всем юзерам:", parse_mode="HTML")

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: Message, state: FSMContext):
    await state.clear()
    status_msg = await m.answer("⏳ Начинаю рассылку...")
    
    async with get_db() as db:
        users = await (await db.execute("SELECT user_id FROM users")).fetchall()
    
    success = 0
    fail = 0
    
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            success += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            fail += 1
        except Exception:
            fail += 1
            
    await status_msg.edit_text(f"📢 <b>Рассылка завершена!</b>\n\n✅ Доставлено: {success}\n❌ Недоставлено: {fail}", parse_mode="HTML")

# ==========================================
# 8. ПОДДЕРЖКА И МОДЕРАЦИЯ
# ==========================================
@router.callback_query(F.data == "ask_supp")
async def ask_supp(c: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text("📝 <b>Напишите ваш вопрос одним сообщением:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")
    await state.set_state(UserState.waiting_support)

@router.message(UserState.waiting_support)
async def send_supp(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="💬 Ответить", callback_data=f"reply_{m.from_user.id}")
    
    try:
        await bot.send_message(
            ADMIN_ID, 
            f"🆘 <b>ВОПРОС от {m.from_user.id}</b>\n@{m.from_user.username}:\n\n{m.text}", 
            reply_markup=kb.as_markup(), 
            parse_mode="HTML"
        )
        await m.answer("✅ Ваш вопрос отправлен администратору.")
    except Exception as e:
        logger.error(f"Supp send error: {e}")
        await m.answer("❌ Ошибка отправки.")

@router.callback_query(F.data.startswith("reply_"))
async def adm_reply(c: CallbackQuery, state: FSMContext):
    user_id = c.data.split("_")[1]
    await state.update_data(ruid=user_id)
    await state.set_state(AdminState.support_reply)
    await c.message.answer(f"✍️ Введите ответ для пользователя <code>{user_id}</code>:", parse_mode="HTML")

@router.message(AdminState.support_reply)
async def send_reply(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    target_id = data.get('ruid')
    
    try:
        await bot.send_message(target_id, f"👨‍💻 <b>Ответ поддержки:</b>\n\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ доставлен.")
    except Exception:
        await m.answer("❌ Не удалось доставить.")
    
    await state.clear()

@router.callback_query(F.data.startswith("acc_"))
async def acc_dec(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    action, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    
    async with get_db() as db:
        if action == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await db.commit()
            await c.message.edit_text(f"✅ Пользователь {uid} принят.")
            try: await bot.send_message(uid, "✅ <b>Вам выдан доступ!</b> Нажмите /start", parse_mode="HTML")
            except: pass
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            await db.commit()
            await c.message.edit_text(f"🚫 Пользователь {uid} забанен.")

# ==========================================
# 9. ГЛОБАЛЬНЫЙ МОНИТОР
# ==========================================
async def global_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60) 
            now = datetime.now(timezone.utc)
            
            async with get_db() as db:
                # 1. Таймаут кода (4 минуты)
                waiters = await (await db.execute(
                    "SELECT id, user_id, phone, worker_chat_id, worker_thread_id, wait_code_start FROM numbers WHERE status='active' AND wait_code_start IS NOT NULL"
                )).fetchall()
                
                for w in waiters:
                    start_time = datetime.fromisoformat(w['wait_code_start'])
                    if (now - start_time).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead', end_time=?, wait_code_start=NULL WHERE id=?", (get_now(), w['id']))
                        
                        try:
                            await bot.send_message(w['user_id'], f"⏳ <b>Время ожидания кода истекло.</b>\nНомер {w['phone']} снят.", parse_mode="HTML")
                        except: pass
                        
                        if w['worker_chat_id']:
                            try:
                                await bot.send_message(
                                    chat_id=w['worker_chat_id'], 
                                    message_thread_id=w['worker_thread_id'] if w['worker_thread_id'] else None, 
                                    text=f"⚠️ <b>Таймаут!</b> Юзер не прислал код для {w['phone']}. Номер снят.", 
                                    parse_mode="HTML"
                                )
                            except: pass

                # 2. AFK в очереди
                queue_rows = await (await db.execute("SELECT id, user_id, created_at, last_ping FROM numbers WHERE status='queue'")).fetchall()
                for r in queue_rows:
                    last_act_str = r['last_ping'] if r['last_ping'] else r['created_at']
                    
                    if not str(last_act_str).startswith("PENDING_"):
                        last_act = datetime.fromisoformat(last_act_str)
                        if (now - last_act).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            kb = InlineKeyboardBuilder().button(text="👋 Я тут!", callback_data=f"afk_ok_{r['id']}").as_markup()
                            try:
                                await bot.send_message(r['user_id'], "⚠️ <b>Вы все еще ждете?</b>\nПодтвердите, что вы на связи.", reply_markup=kb, parse_mode="HTML")
                                await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                            except: 
                                await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                
                await db.commit()
                
        except Exception as e:
            logger.error(f"Monitor Error: {e}")
            await asyncio.sleep(5)

@router.callback_query(F.data.startswith("afk_ok_"))
async def afk_confirm(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("✅ Отлично, вы в очереди!")

# ==========================================
# 10. ЗАПУСК
# ==========================================
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(global_monitor(bot))
    
    logger.info("🚀 BOT v80.1 STARTED SUCCESSFULLY")
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
