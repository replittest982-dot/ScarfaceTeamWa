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
        Message, ReactionTypeEmoji, BufferedInputFile, ForceReply
    )
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from aiogram.exceptions import TelegramForbiddenError
except ImportError:
    sys.exit("❌ Установите библиотеки: pip install aiogram aiosqlite")

# ==========================================
# 1. КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_ТОКЕН_СЮДА")
ADMIN_ID = int(os.getenv("ADMIN_ID", "12345678"))
DB_NAME = "fast_team_v70.db"

# Таймеры (в минутах)
AFK_CHECK_MINUTES = 8      # Обычный AFK
CODE_WAIT_MINUTES = 4      # Ожидание кода

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
        
        # Номера (Добавили worker_chat_id, worker_thread_id для пересылки ответов)
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
    logger.info("✅ DB Loaded v70.0 (Fixed Duration & Reply)")

# ==========================================
# 3. УТИЛИТЫ
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

def calc_duration(start_iso, end_iso):
    try:
        if not start_iso or not end_iso: return "0 мин"
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: return "0 мин"

# ==========================================
# 4. КЛАВИАТУРЫ
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
    # Разные кнопки для разных тарифов
    kb = InlineKeyboardBuilder()
    
    if "MAX" in tariff_name.upper():
        # Для MAX: Встал, Пропуск
        kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
        kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{nid}")
    else:
        # Для WhatsApp: Встал, Ошибка
        kb.button(text="✅ Встал", callback_data=f"w_act_{nid}")
        kb.button(text="❌ Ошибка", callback_data=f"w_err_{nid}")
        
    return kb.as_markup()

def worker_active_kb(nid):
    return InlineKeyboardBuilder().button(text="📉 Слет", callback_data=f"w_drop_{nid}").as_markup()

# ==========================================
# 5. ХЕНДЛЕРЫ: ЮЗЕР И МЕНЮ
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
                try: await m.bot.send_message(ADMIN_ID, f"👤 <b>Новый юзер:</b> {uid} (@{m.from_user.username})", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 <b>Ожидайте подтверждения доступа.</b>", parse_mode="HTML")
        
        if res['is_banned']: return await m.answer("🚫 <b>Вы забанены.</b>", parse_mode="HTML")
        if res['is_approved']: await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=main_kb(uid))
        else: await m.answer("⏳ <b>Заявка на рассмотрении.</b>", parse_mode="HTML")

@router.callback_query(F.data == "guide")
async def show_guide(c: CallbackQuery):
    # Текст помощи из ТЗ
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
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    
    await c.message.edit_text(
        f"💎 Тариф: <b>{t_name}</b>\n💰 Прайс: {res['price']}\n⏰ Время: {res['work_time']}\n\n"
        f"📱 <b>Введите номера (списком):</b>", 
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
            await db.execute("INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, tariff_time, last_ping) VALUES (?, ?, ?, ?, ?, ?)",
                             (m.from_user.id, ph, data['tariff'], data['price'], data['time'], get_now()))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ <b>Принято: {len(valid)} шт.</b>\nДобавлено в очередь.", reply_markup=main_kb(m.from_user.id), parse_mode="HTML")

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        q_all = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue'")).fetchone())[0]
        q_mine = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='queue'", (uid,))).fetchone())[0]
        my_first = await (await db.execute("SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1", (uid,))).fetchone()
        pos = 0 if not my_first else (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?", (my_first[0],))).fetchone())[0] + 1
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
    
    txt = (f"👤 <b>Профиль</b>\n📦 Всего сдано: {total}\n\n"
           f"🕒 <b>ОЧЕРЕДЬ</b>\n🌍 Всего в очереди: {q_all}\n👤 Ваших номеров: {q_mine}\n🔢 Ваша позиция: {pos if q_mine > 0 else '-'}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Мои номера (Удалить)", callback_data="my_nums")
    kb.button(text="🔙 Назад", callback_data="back_main")
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "my_nums")
async def my_nums(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        rows = await (await db.execute("SELECT id, phone, status FROM numbers WHERE user_id=? AND status IN ('queue','active','work') ORDER BY id ASC LIMIT 15", (uid,))).fetchall()
    
    kb = InlineKeyboardBuilder()
    for r in rows:
        st_icon = "⏳" if r['status']=='queue' else "🔥"
        kb.button(text=f"❌ {st_icon} {mask_phone(r['phone'], uid)}", callback_data=f"del_{r['id']}")
    kb.button(text="🔙", callback_data="profile")
    kb.adjust(1)
    
    if not rows: await c.message.edit_text("📭 Активных номеров нет.", reply_markup=kb.as_markup())
    else: await c.message.edit_text("Нажмите для удаления:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("del_"))
async def del_num_user(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        row = await (await db.execute("SELECT status FROM numbers WHERE id=? AND user_id=?", (nid, c.from_user.id))).fetchone()
        if row and row['status'] == 'queue':
            await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
            await db.commit()
            await c.answer("✅ Удален")
            await my_nums(c)
        else:
            await c.answer("❌ Уже в работе, нельзя удалить!", show_alert=True)

# ==========================================
# 6. ВОРКЕР: СИСТЕМА (НОВАЯ)
# ==========================================
@router.message(Command("startwork"))
async def sys_start(m: Message):
    if m.from_user.id != ADMIN_ID: return 
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    await m.answer("⚙️ <b>Выберите тариф для привязки:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("bind_"))
async def sys_bind(c: CallbackQuery):
    t = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", t))
        await db.commit()
    
    # ТЕКСТЫ ГАЙДОВ (РАЗДЕЛЬНЫЕ)
    if "MAX" in t.upper():
        guide_txt = (f"✅ <b>Чат привязан!</b> Тариф: {t}\n\n"
                     f"👨‍💻 <b>Гайд по использованию (MAX):</b>\n"
                     f"1. Пиши <code>/num</code> -> Получишь номер.\n"
                     f"2. Если нужно запросить код: Пиши <code>/code +7...</code>\n"
                     f"3. Юзер получит уведомление 'Офис запросил номер'.\n"
                     f"4. Ответ юзера придет СЮДА.\n"
                     f"5. Кнопки: <b>✅ Встал</b> или <b>⏭ Пропуск</b>.")
    else:
        guide_txt = (f"✅ <b>Чат привязан!</b> Тариф: {t}\n\n"
                     f"👨‍💻 <b>Гайд по использованию (WhatsApp):</b>\n"
                     f"1. Пиши <code>/num</code> -> Получишь номер.\n"
                     f"2. Вбей номер в WhatsApp Web.\n"
                     f"3. Если просят QR: Сфоткай QR с экрана.\n"
                     f"   Скинь фото сюда и подпиши: <code>/sms +7... Сканируй</code>\n"
                     f"4. Если просят Код: Сфоткай и подпиши: <code>/sms +7... Вводи этот код</code>\n"
                     f"5. Жми <b>✅ Встал</b> или <b>📉 Слет</b>.")
    
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
        if not conf: return await m.reply(f"❌ Топик не привязан к тарифу.")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Очередь пуста.")
        
        # ЗАПОМИНАЕМ CHAT_ID и THREAD_ID ЧТОБЫ ЗНАТЬ КУДА ПЕРЕСЫЛАТЬ ОТВЕТ
        await db.execute("UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?", 
                         (m.from_user.id, cid, tid, get_now(), row['id']))
        await db.commit()
    
    await m.answer(f"🚀 <b>В работе:</b>\n📱 <code>{row['phone']}</code>\nТариф: {row['tariff_name']}", 
                   reply_markup=worker_kb(row['id'], row['tariff_name']), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"⚡ <b>Ваш номер {mask_phone(row['phone'], row['user_id'])} в работе!</b>\nОжидайте пуш-уведомление / QR.", parse_mode="HTML")
    except: pass

# --- ЗАПРОС КОДА (MAX) ---
@router.message(Command("code"))
async def worker_code_req(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Пример: <code>/code +7999...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер.")
    
    # Ставим таймер в БД
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now(), row['id']))
        await db.commit()

    # FORCE REPLY - Чтобы юзер ответил реплаем
    try:
        await bot.send_message(row['user_id'], 
                               f"🔔 <b>Офис запросил номер</b>\n\n👇 <b>Ответьте ниже сообщением, чтобы дать код.</b>", 
                               reply_markup=ForceReply(selective=True), parse_mode="HTML")
        await m.reply(f"✅ Запрос отправлен юзеру.")
    except: await m.reply("❌ Ошибка доставки (юзер блок?).")

# --- ОТВЕТ ЮЗЕРА НА КОД (ПЕРЕСЫЛКА В ЧАТ) ---
@router.message(F.reply_to_message)
async def user_text_reply(m: Message, bot: Bot):
    if m.from_user.id == ADMIN_ID: return 
    
    # Проверяем, есть ли у этого юзера активный номер
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
    
    # Если юзер отвечает на "Офис запросил номер" или просто пишет, когда номер в работе
    if row and row['worker_chat_id']:
        msg_text = m.text or "[Файл]"
        
        # Сбрасываем таймер ожидания (код получен)
        async with get_db() as db:
            await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
            await db.commit()
            
        # ОТПРАВЛЯЕМ В ТОТ ЧАТ И ТОПИК, ГДЕ БЫЛ ВЗЯТ НОМЕР
        try:
            target_chat = row['worker_chat_id']
            target_thread = row['worker_thread_id']
            
            await bot.send_message(
                chat_id=target_chat,
                message_thread_id=target_thread if target_thread else None,
                text=f"📩 <b>КОД ОТ ЮЗЕРА:</b>\n📱 {row['phone']}\n💬 <code>{msg_text}</code>", 
                parse_mode="HTML"
            )
            await m.answer("✅ Отправлено в офис.")
        except Exception as e:
            logger.error(f"Forward Error: {e}")
            await m.answer("❌ Ошибка доставки.")

# --- SMS ФОТО (WHATSAPP) ---
@router.message(F.photo)
async def worker_photo_sms(m: Message, bot: Bot):
    if not m.caption or "/sms" not in m.caption.lower(): return
    parts = m.caption.split()
    try:
        idx = next(i for i, p in enumerate(parts) if p.lower().startswith("/sms"))
        ph_raw = parts[idx+1]
        text = " ".join(parts[idx+2:]) if len(parts) > idx+2 else "Сканируй/Вводи код"
    except: return await m.reply("⚠️ Формат: /sms +7... текст")
    
    ph = clean_phone(ph_raw)
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    # Защита от чужих
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Это не ваш номер.")
    
    try:
        await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"🔔 <b>SMS / QR</b>\n{text}", parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except: await m.reply("❌ Не доставлено.")

# --- КНОПКИ ВОРКЕРА (ЗАЩИТА) ---
@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        # ЗАЩИТА ОТ ЧУЖАКА
        if not row or row['worker_id'] != c.from_user.id: 
            return await c.answer("🚫 Не ты брал — не тебе жать!", show_alert=True)
            
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text(f"✅ <b>Встал:</b> {row['phone']}", reply_markup=worker_active_kb(nid), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], "✅ <b>Номер успешно встал!</b>", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        # ЗАЩИТА ОТ ЧУЖАКА
        if not row or row['worker_id'] != c.from_user.id: 
            return await c.answer("🚫 Не ты брал — не тебе жать!", show_alert=True)

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
        
        # ЗАЩИТА ОТ ЧУЖАКА
        if not row or row['worker_id'] != c.from_user.id: 
            return await c.answer("🚫 Не ты брал — не тебе жать!", show_alert=True)
        
        status = "finished" if is_drop else "dead"
        dur = calc_duration(row['start_time'], get_now())
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, get_now(), nid))
        await db.commit()
    
    msg = f"📉 Слет. Время: {dur}" if is_drop else "❌ Ошибка"
    await c.message.edit_text(msg)
    try: await bot.send_message(row['user_id'], msg)
    except: pass

# ==========================================
# 7. АДМИНКА И ПРОЧЕЕ
# ==========================================
@router.callback_query(F.data == "admin_main")
async def adm_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы", callback_data="adm_tariffs")
    kb.button(text="📄 Отчеты", callback_data="adm_reports")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ Админ панель", reply_markup=kb.as_markup())

@router.callback_query(F.data == "adm_reports")
async def adm_reports(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for h in [1, 24, 48]: kb.button(text=f"{h}ч", callback_data=f"rep_{h}")
    kb.button(text="🔙", callback_data="admin_main")
    await c.message.edit_text("Период:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_"))
async def adm_get_rep(c: CallbackQuery):
    h = int(c.data.split("_")[1])
    cut = (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat()
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM numbers WHERE created_at >= ? ORDER BY id DESC", (cut,))).fetchall()
    
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'Phone', 'Status', 'Tariff', 'Time', 'Duration'])
    for r in rows:
        dur = calc_duration(r['start_time'], r['end_time'])
        w.writerow([r['id'], r['phone'], r['status'], r['tariff_name'], format_time(r['created_at']), dur])
    
    out.seek(0)
    await c.message.answer_document(BufferedInputFile(out.getvalue().encode(), filename=f"rep_{h}h.csv"), caption=f"Отчет {h}ч")
    await c.answer()

@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙", callback_data="admin_main")
    await c.message.edit_text("Тарифы:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("ed_"))
async def ed_t1(c: CallbackQuery, state: FSMContext):
    await state.update_data(target=c.data.split("_")[1])
    await state.set_state(AdminState.edit_time)
    await c.message.edit_text("Введи время (10:00-22:00):")

@router.message(AdminState.edit_time)
async def ed_t2(m: Message, state: FSMContext):
    await state.update_data(time=m.text)
    await state.set_state(AdminState.edit_price)
    await m.answer("Введи цену (50р):")

@router.message(AdminState.edit_price)
async def ed_t3(m: Message, state: FSMContext):
    d = await state.get_data()
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, work_time=? WHERE name=?", (m.text, d['time'], d['target']))
        await db.commit()
    await state.clear()
    await m.answer("✅ Сохранено", reply_markup=main_kb(ADMIN_ID))

@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 Пришлите сообщение для рассылки:")

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳ Рассылаю...")
    async with get_db() as db:
        users = await (await db.execute("SELECT user_id FROM users")).fetchall()
    cnt = 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            cnt += 1
            await asyncio.sleep(0.05)
        except: pass
    await msg.edit_text(f"✅ Доставлено {cnt} юзерам.")

@router.callback_query(F.data == "back_main")
async def back_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Меню", reply_markup=main_kb(c.from_user.id))

@router.callback_query(F.data == "ask_supp")
async def ask_supp(c: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙", callback_data="back_main")
    await c.message.edit_text("Вопрос:", reply_markup=kb.as_markup())
    await state.set_state(UserState.waiting_support)

@router.message(UserState.waiting_support)
async def send_supp(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="Ответить", callback_data=f"reply_{m.from_user.id}")
    try:
        await bot.send_message(ADMIN_ID, f"🆘 {m.from_user.id}:\n{m.text}", reply_markup=kb.as_markup())
        await m.answer("✅ Отправлено")
    except: pass

@router.callback_query(F.data.startswith("reply_"))
async def adm_reply(c: CallbackQuery, state: FSMContext):
    await state.update_data(ruid=c.data.split("_")[1])
    await state.set_state(AdminState.support_reply)
    await c.message.answer("Ответ:")

@router.message(AdminState.support_reply)
async def send_reply(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    try:
        await bot.send_message(d['ruid'], f"👨‍💻 <b>Поддержка:</b>\n{m.text}", parse_mode="HTML")
        await m.answer("✅")
    except: pass
    await state.clear()

@router.callback_query(F.data.startswith("acc_"))
async def acc_dec(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        if act == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await bot.send_message(uid, "✅ Доступ открыт!")
            await c.message.edit_text(f"✅ {uid} принят")
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            await c.message.edit_text(f"🚫 {uid} забанен")
        await db.commit()

# --- МОНИТОРИНГ ---
async def global_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            async with get_db() as db:
                # Таймер ожидания кода (4 мин)
                waiters = await (await db.execute("SELECT id, user_id, phone, worker_chat_id, worker_thread_id, wait_code_start FROM numbers WHERE status='active' AND wait_code_start IS NOT NULL")).fetchall()
                for w in waiters:
                    start = datetime.fromisoformat(w['wait_code_start'])
                    if (now - start).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead', end_time=?, wait_code_start=NULL WHERE id=?", (get_now(), w['id']))
                        try:
                            await bot.send_message(w['user_id'], f"⏳ Время вышло. Номер удален.")
                            if w['worker_chat_id']:
                                await bot.send_message(chat_id=w['worker_chat_id'], message_thread_id=w['worker_thread_id'] if w['worker_thread_id'] else None, text=f"⚠️ <b>Таймаут кода!</b> Номер {w['phone']} снят.", parse_mode="HTML")
                        except: pass
                
                # AFK
                q_rows = await (await db.execute("SELECT id, user_id, created_at, last_ping FROM numbers WHERE status='queue'")).fetchall()
                for r in q_rows:
                    last = r['last_ping'] if r['last_ping'] else r['created_at']
                    if not last.startswith("PENDING_"):
                        if (now - datetime.fromisoformat(last)).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            kb = InlineKeyboardBuilder().button(text="👋 Я тут", callback_data=f"afk_ok_{r['id']}").as_markup()
                            try:
                                await bot.send_message(r['user_id'], "Вы тут?", reply_markup=kb)
                                await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                            except: pass
                await db.commit()
        except Exception as e:
            logger.error(f"Mon Error: {e}")

@router.callback_query(F.data.startswith("afk_ok_"))
async def afk_confirm(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("Ok")

# --- ЗАПУСК ---
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(global_monitor(bot))
    logger.info("🚀 BOT v70.0 STARTED")
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
