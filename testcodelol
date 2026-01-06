import asyncio
import logging
import sys
import os
import re
import io
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

try:
    import aiosqlite
    from aiogram import Bot, Dispatcher, Router, F, types
    from aiogram.filters import Command, CommandStart, CommandObject
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import (
        InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, 
        Message, ReactionTypeEmoji, ReplyKeyboardRemove
    )
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from aiogram.exceptions import TelegramForbiddenError
except ImportError:
    sys.exit("❌ Установи библиотеки: pip install aiogram aiosqlite")

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_СЮДА_ТОКЕН")
ADMIN_ID = int(os.getenv("ADMIN_ID", "12345678")) # Твой ID цифрами
DB_NAME = "bot_v55.db"

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
        # Юзеры
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
            reg_date TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        # Номера
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            tariff_name TEXT, tariff_price TEXT, tariff_time TEXT, 
            status TEXT DEFAULT 'queue', worker_id INTEGER DEFAULT 0, 
            start_time TEXT, end_time TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        # Тарифы (name, price, work_time)
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            name TEXT PRIMARY KEY, price TEXT, work_time TEXT)""")
        
        # Конфиг (привязка топиков)
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '24/7')")
        
        await db.commit()
    logger.info("✅ База данных загружена (v55.0)")

# ==========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================
def clean_phone(phone: str):
    """Очистка номера: оставляет цифры, добавляет +"""
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    return '+' + clean if re.match(r'^7\d{10}$', clean) else None

def mask_phone(phone, user_id):
    """Маскировка: +79991234567 -> +7999***4567"""
    if user_id == ADMIN_ID: return phone
    try:
        if len(phone) < 8: return phone
        return f"{phone[:5]}***{phone[-4:]}"
    except: return phone

def get_now(): return datetime.now(timezone.utc).isoformat()

def format_time(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        return (dt + timedelta(hours=3)).strftime("%d.%m %H:%M") # +3 часа (МСК/Ориентир)
    except: return "-"

def calc_duration(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: return "?"

# ==========================================
# СОСТОЯНИЯ (FSM)
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_support = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_time = State() # Сначала время
    edit_price = State() # Потом цена
    support_reply = State()

# ==========================================
# КЛАВИАТУРЫ
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Задать вопрос", callback_data="ask_supp")
    if user_id == ADMIN_ID: kb.button(text="⚡ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 1, 1)
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
# ОБРАБОТЧИКИ: СТАРТ И ЮЗЕР
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
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>Запрос доступа:</b>\nID: {uid}\n@{m.from_user.username}", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте подтверждения доступа.</b>", parse_mode="HTML")
        
        if res['is_banned']: return await m.answer("🚫 <b>Вы забанены.</b>", parse_mode="HTML")
        if res['is_approved']: await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=main_kb(uid))
        else: await m.answer("⏳ <b>Ваша заявка на рассмотрении.</b>", parse_mode="HTML")

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
        # Позиция в очереди
        q_pos = 0
        my_first = await (await db.execute("SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1", (uid,))).fetchone()
        if my_first:
            q_pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?", (my_first[0],))).fetchone())[0]
        
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
        rows = await (await db.execute("SELECT phone, status, tariff_price FROM numbers WHERE user_id=? ORDER BY id DESC LIMIT 10", (uid,))).fetchall()
    
    txt = "📝 <b>Ваши последние 10 номеров:</b>\n\n"
    if not rows: txt += "Пусто."
    for r in rows:
        icon = "🟡" if r['status']=='queue' else "🟢" if r['status']=='active' else "✅" if r['status']=='finished' else "❌"
        txt += f"{icon} {mask_phone(r['phone'], uid)} | {r['tariff_price']}\n"
        
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Назад", callback_data="profile")
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

# ==========================================
# СДАЧА НОМЕРОВ
# ==========================================
@router.callback_query(F.data == "sel_tariff")
async def sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for r in rows: kb.button(text=f"{r['name']} | {r['price']}", callback_data=f"pick_{r['name']}")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("📂 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def pick_t(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("_")[1]
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (t_name,))).fetchone()
    
    await state.update_data(tariff=t_name, price=res['price'], time=res['work_time'])
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Отмена", callback_data="back_main")
    
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
    
    if not valid: return await m.reply("❌ Не найдено валидных номеров.")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute("INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_time) VALUES (?, ?, ?, ?, ?)",
                             (m.from_user.id, ph, data['tariff'], data['price'], data['time']))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ <b>Принято: {len(valid)} шт.</b>\nОжидайте обработки.", reply_markup=main_kb(m.from_user.id), parse_mode="HTML")

# ==========================================
# РАБОТА ВОРКЕРА (MAX / WHATSAPP)
# ==========================================
@router.message(Command("startwork"))
async def startwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    await m.answer("⚙️ Выберите тариф для топика:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def bind_topic(c: CallbackQuery):
    t_name = c.data.split("_")[1]
    cid, tid = c.message.chat.id, (c.message.message_thread_id if c.message.is_topic_message else 0)
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", t_name))
        await db.commit()
    
    guide = (f"✅ <b>Чат привязан!</b> Тариф: {t_name}\n\n"
             "👨‍💻 <b>Гайд по использованию:</b>\n"
             "1. Пиши <code>/num</code> -> Получишь номер.\n"
             "2. Вбей номер в WhatsApp Web / Эмулятор.\n"
             "3. <b>Если WhatsApp (QR/Скан):</b>\n"
             "   • Сфоткай QR.\n   • Скинь фото и подпиши: <code>/sms +7... Сканируй</code>\n"
             "4. <b>Если MAX (Код):</b>\n"
             "   • Пиши команду: <code>/code +7...</code>\n"
             "   • Юзер получит увед и ответит реплаем.\n"
             "5. Жми <b>✅ Встал</b> или <b>📉 Слет</b>.")
    await c.message.edit_text(guide, parse_mode="HTML")

@router.message(Command("stopwork"))
async def stopwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (f"topic_{cid}_{tid}",))
        await db.commit()
    await m.reply("🛑 Топик отвязан.")

@router.message(Command("num"))
async def get_num(m: Message, bot: Bot):
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply("❌ Топик не настроен.")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Очередь пуста.")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", (m.from_user.id, get_now(), row['id']))
        await db.commit()
    
    await m.answer(f"🚀 <b>Вы взяли номер</b>\n📱 <code>{row['phone']}</code>\nОжидайте код.", 
                   reply_markup=worker_kb(row['id'], row['tariff_name']), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"⚡ <b>Ваш номер {mask_phone(row['phone'], row['user_id'])} взяли!</b>\nОжидайте код.", parse_mode="HTML")
    except: pass

# --- SMS / ФОТО (WHATSAPP) ---
@router.message(F.photo)
async def handle_photo(m: Message, bot: Bot):
    if not m.caption or "/sms" not in m.caption.lower(): return
    parts = m.caption.split()
    try:
        idx = next(i for i, p in enumerate(parts) if p.lower().startswith("/sms"))
        ph_raw = parts[idx+1]
        text = " ".join(parts[idx+2:]) if len(parts) > idx+2 else "Фото от офиса"
    except: return await m.reply("⚠️ Формат: /sms +7... текст")
    
    ph = clean_phone(ph_raw)
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row: return await m.reply("❌ Номер не в работе.")
    
    try:
        await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"🔔 <b>SMS/QR</b>\n{text}", parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except: await m.reply("❌ Ошибка отправки (юзер блок?)")

# --- CODE (MAX) ---
@router.message(Command("code"))
async def handle_code_cmd(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Формат: <code>/code +7...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Номер не ваш или не в работе.")
    
    try:
        await bot.send_message(row['user_id'], 
                               "🔔 <b>Офис запросил номер</b>\nответьте ниже сообщением чтобы дать код.", 
                               reply_markup=types.ForceReply(selective=True), parse_mode="HTML")
        await m.reply("✅ Запрос отправлен юзеру.")
    except: await m.reply("❌ Ошибка отправки юзеру.")

# --- ОТВЕТ ЮЗЕРА НА CODE ---
@router.message(F.reply_to_message)
async def user_reply(m: Message, bot: Bot):
    if m.from_user.id == ADMIN_ID: return # Игнор админа здесь
    if "Офис запросил" in m.reply_to_message.text:
        async with get_db() as db:
            row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
        if row:
            txt = m.text or "[Файл]"
            try:
                await bot.send_message(row['worker_id'], f"📩 <b>ОТВЕТ ЮЗЕРА</b>\n📱 {row['phone']}\n💬 <code>{txt}</code>", parse_mode="HTML")
                await m.answer("✅ Отправлено офису.")
            except: pass

# --- КНОПКИ ВОРКЕРА ---
@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("❌ Не твой номер!", show_alert=True)
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text("✅ <b>номер встал и все</b>", reply_markup=worker_active_kb(nid), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], "✅ <b>Номер успешно встал!</b>", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("❌ Не твой номер!", show_alert=True)
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
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
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("❌ Не твой номер!", show_alert=True)
        
        status = "finished" if is_drop else "dead"
        dur = calc_duration(row['start_time'], get_now())
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, get_now(), nid))
        await db.commit()
    
    if is_drop:
        await c.message.edit_text(f"📉 Номер слетел. Время: {dur}")
        try: await bot.send_message(row['user_id'], f"📉 ваш номер слетел и его время работы: {dur}")
        except: pass
    else:
        await c.message.edit_text("❌ Ошибка (отмена)")
        try: await bot.send_message(row['user_id'], "❌ Ошибка номера.")
        except: pass

# ==========================================
# АДМИН ПАНЕЛЬ
# ==========================================
@router.callback_query(F.data == "admin_main")
async def adm_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы (Изм.)", callback_data="adm_tariffs")
    kb.button(text="📦 Очередь (Текст)", callback_data="adm_queue")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- РАССЫЛКА ---
@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 <b>Отправьте сообщение (Текст/Фото) для рассылки:</b>", parse_mode="HTML")

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳ Рассылка запущена...")
    async with get_db() as db:
        users = await (await db.execute("SELECT user_id FROM users")).fetchall()
    
    good, bad = 0, 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            good += 1
            await asyncio.sleep(0.05) # Антиспам
        except TelegramForbiddenError: bad += 1
        except: bad += 1
    
    await msg.edit_text(f"✅ <b>Рассылка завершена!</b>\n\n📩 Доставлено: {good}\n🚫 Блоки/Ошибки: {bad}", parse_mode="HTML")

# --- ОЧЕРЕДЬ (ТЕКСТ) ---
@router.callback_query(F.data == "adm_queue")
async def adm_queue(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM numbers WHERE status='queue' ORDER BY id ASC")).fetchall()
    
    if not rows: return await c.answer("Очередь пуста!", show_alert=True)
    
    txt = "📦 <b>Очередь:</b>\n\n"
    for r in rows:
        txt += f"🆔 {r['id']} | {r['phone']} | {r['tariff_name']}\n"
        if len(txt) > 3800: break # Лимит ТГ
    
    await c.message.answer(txt, parse_mode="HTML")
    await c.answer()

# --- ИЗМЕНЕНИЕ ТАРИФОВ ---
@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    await c.message.edit_text("Выберите тариф для изменения:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("ed_"))
async def ed_t_step1(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("_")[1]
    await state.update_data(target=t_name)
    await state.set_state(AdminState.edit_time)
    await c.message.edit_text(f"⚙️ Изменяем <b>{t_name}</b>\n\n1️⃣ Введите <b>ВРЕМЯ РАБОТЫ (МСК)</b>:\n(например: <code>10:00-22:00</code>)", parse_mode="HTML")

@router.message(AdminState.edit_time)
async def ed_t_step2(m: Message, state: FSMContext):
    await state.update_data(new_time=m.text)
    await state.set_state(AdminState.edit_price)
    await m.answer("2️⃣ Теперь введите <b>ПРАЙС</b>:\n(например: <code>50₽</code> или <code>10$</code>)", parse_mode="HTML")

@router.message(AdminState.edit_price)
async def ed_t_finish(m: Message, state: FSMContext):
    d = await state.get_data()
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, work_time=? WHERE name=?", (m.text, d['new_time'], d['target']))
        await db.commit()
    await state.clear()
    await m.answer(f"✅ Тариф <b>{d['target']}</b> сохранен!\n🕒 {d['new_time']}\n💰 {m.text}", parse_mode="HTML")

# ==========================================
# ПОДДЕРЖКА И ПРОЧЕЕ
# ==========================================
@router.callback_query(F.data == "ask_supp")
async def ask_supp(c: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text("📝 <b>Напишите ваш вопрос одним сообщением:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")
    await state.set_state(UserState.waiting_support)

@router.message(UserState.waiting_support)
async def send_supp(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="Ответить", callback_data=f"reply_{m.from_user.id}")
    try:
        await bot.send_message(ADMIN_ID, f"🆘 <b>Вопрос от {m.from_user.id}:</b>\n{m.text}", reply_markup=kb.as_markup(), parse_mode="HTML")
        await m.answer("✅ Отправлено админу.")
    except: await m.answer("❌ Ошибка отправки.")

@router.callback_query(F.data.startswith("reply_"))
async def adm_reply(c: CallbackQuery, state: FSMContext):
    uid = c.data.split("_")[1]
    await state.update_data(ruid=uid)
    await state.set_state(AdminState.support_reply)
    await c.message.answer(f"✍️ Введите ответ для {uid}:")

@router.message(AdminState.support_reply)
async def send_reply(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    try:
        await bot.send_message(d['ruid'], f"👨‍💻 <b>Ответ поддержки:</b>\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ ушел.")
    except: await m.answer("❌ Не доставлено (юзер блокнул бота).")
    await state.clear()

@router.callback_query(F.data == "back_main")
async def back_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Главное меню", reply_markup=main_kb(c.from_user.id))

@router.callback_query(F.data.startswith("acc_"))
async def access_action(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split("_")[1], int(c.data.split('_')[2])
    async with get_db() as db:
        if act == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await bot.send_message(uid, "✅ <b>Доступ открыт! Жми /start</b>", parse_mode="HTML")
            await c.message.edit_text(f"✅ Юзер {uid} принят")
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            await c.message.edit_text(f"🚫 Юзер {uid} забанен")
        await db.commit()

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
