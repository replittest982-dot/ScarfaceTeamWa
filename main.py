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
# КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_ТОКЕН_СЮДА")
ADMIN_ID = int(os.getenv("ADMIN_ID", "12345678")) 
DB_NAME = "fast_team_v60.db"

# Настройки AFK
AFK_CHECK_MINUTES = 8  # Проверка каждые 8 минут
AFK_TIMEOUT_MINUTES = 3 # Время на нажатие кнопки "Я тут"

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
        
        # Добавил last_ping для AFK проверок
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            tariff_name TEXT, tariff_price TEXT, tariff_time TEXT, 
            status TEXT DEFAULT 'queue', worker_id INTEGER DEFAULT 0, 
            start_time TEXT, end_time TEXT, last_ping TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            name TEXT PRIMARY KEY, price TEXT, work_time TEXT)""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '24/7')")
        
        await db.commit()
    logger.info("✅ DB Loaded v60.0")

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
        # Оставляем начало и конец, центр скрываем
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
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins} мин"
    except: return "?"

# ==========================================
# FSM И КЛАВИАТУРЫ
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
# AFK MONITOR (Фоновая задача)
# ==========================================
async def afk_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60) # Проверка каждую минуту
            now = datetime.now(timezone.utc)
            
            async with get_db() as db:
                # 1. Ищем тех, кто долго стоит в очереди без проверки (AFK Check)
                rows = await (await db.execute("SELECT id, user_id, phone, created_at, last_ping FROM numbers WHERE status='queue'")).fetchall()
                
                for r in rows:
                    # Время создания или последней проверки
                    last_action_str = r['last_ping'] if r['last_ping'] else r['created_at']
                    last_action = datetime.fromisoformat(last_action_str)
                    
                    diff_min = (now - last_action).total_seconds() / 60
                    
                    # Если прошло больше 8 минут - кидаем проверку
                    if diff_min >= AFK_CHECK_MINUTES:
                        kb = InlineKeyboardBuilder().button(text="👋 Я тут!", callback_data=f"afk_ok_{r['id']}").as_markup()
                        try:
                            await bot.send_message(r['user_id'], f"⚠️ <b>Проверка активности!</b>\n\nВы тут? Номер {mask_phone(r['phone'], r['user_id'])} будет удален через {AFK_TIMEOUT_MINUTES} мин.", reply_markup=kb, parse_mode="HTML")
                            # Обновляем last_ping на "сейчас", но ставим метку (можно было бы статус поменять, но упростим)
                            # Просто обновим пинг, если он не нажмет кнопку, в след раз удалим по логике ниже? 
                            # Нет, нужна метка удаления.
                            # Для простоты: Обновляем last_ping. Если юзер не нажмет кнопку, в следующий проход...
                            # Реализуем через 'pending' статус
                            await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                            await db.commit()
                        except: 
                            # Если бот заблочен - удаляем сразу
                            await db.execute("UPDATE numbers SET status='deleted' WHERE id=?", (r['id'],))
                            await db.commit()

                # 2. Удаление тех, кто не нажал кнопку (статус PENDING)
                rows_pending = await (await db.execute("SELECT id, user_id, phone, last_ping FROM numbers WHERE status='queue' AND last_ping LIKE 'PENDING_%'")).fetchall()
                for r in rows_pending:
                    ping_time_str = r['last_ping'].replace("PENDING_", "")
                    ping_time = datetime.fromisoformat(ping_time_str)
                    
                    if (now - ping_time).total_seconds() / 60 >= AFK_TIMEOUT_MINUTES:
                        await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                        await db.commit()
                        try:
                            await bot.send_message(r['user_id'], f"🗑 <b>Номер {mask_phone(r['phone'], r['user_id'])} удален из очереди (AFK).</b>", parse_mode="HTML")
                        except: pass

        except Exception as e:
            logger.error(f"AFK Monitor Error: {e}")

@router.callback_query(F.data.startswith("afk_ok_"))
async def afk_confirm(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT id FROM numbers WHERE id=?", (nid,))).fetchone()
        if row:
            # Снимаем статус PENDING, обновляем время
            await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
            await db.commit()
            await c.message.delete()
            await c.answer("✅ Подтверждено! Вы в очереди.")
        else:
            await c.answer("Номер уже удален или взят.", show_alert=True)
            await c.message.delete()

# ==========================================
# ОСНОВНЫЕ ХЕНДЛЕРЫ
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

# --- СДАЧА НОМЕРОВ ---
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

# --- ПРОФИЛЬ И ОЧЕРЕДЬ ---
@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        # Всего в очереди (глобально)
        q_all = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue'")).fetchone())[0]
        # Мои в очереди
        q_mine = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='queue'", (uid,))).fetchone())[0]
        # Позиция первого моего номера
        my_first = await (await db.execute("SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1", (uid,))).fetchone()
        pos = 0
        if my_first:
            pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id < ?", (my_first[0],))).fetchone())[0] + 1
        
        # Всего номеров
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
    
    txt = (f"👤 <b>Профиль</b>\n"
           f"📦 Всего сдано: {total}\n\n"
           f"🕒 <b>ОЧЕРЕДЬ</b>\n"
           f"🌍 Всего в очереди: {q_all}\n"
           f"👤 Ваших номеров в ожидании: {q_mine}\n"
           f"🔢 Ваша позиция (ближайшая): {pos if q_mine > 0 else '-'}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Мои номера (Удалить)", callback_data="my_nums")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "my_nums")
async def my_nums(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        rows = await (await db.execute("SELECT id, phone, status FROM numbers WHERE user_id=? AND status IN ('queue','active','work') ORDER BY id ASC LIMIT 15", (uid,))).fetchall()
    
    if not rows: return await c.message.edit_text("📭 У вас нет активных номеров.", reply_markup=InlineKeyboardBuilder().button(text="🔙", callback_data="profile").as_markup())
    
    kb = InlineKeyboardBuilder()
    txt = "📝 <b>Ваши активные номера:</b>\n"
    for r in rows:
        st_icon = "⏳" if r['status']=='queue' else "🔥"
        # Кнопка удаления для очереди
        btn_text = f"❌ {mask_phone(r['phone'], uid)}"
        kb.button(text=btn_text, callback_data=f"del_{r['id']}")
    
    kb.button(text="🔙 Назад", callback_data="profile")
    kb.adjust(1)
    await c.message.edit_text("Нажмите на номер, чтобы удалить из очереди:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("del_"))
async def del_num_user(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        row = await (await db.execute("SELECT status FROM numbers WHERE id=? AND user_id=?", (nid, c.from_user.id))).fetchone()
        if row and row['status'] == 'queue':
            await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
            await db.commit()
            await c.answer("✅ Номер удален из очереди")
            await my_nums(c) # Обновить список
        else:
            await c.answer("❌ Нельзя удалить (уже в работе или не ваш)", show_alert=True)

# ==========================================
# ВОРКЕР (ЛОГИКА)
# ==========================================
@router.message(Command("num"))
async def worker_get_num(m: Message, bot: Bot):
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply("❌ Топик не настроен.")
        
        # Берем номер
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Очередь пуста.")
        
        # Обновляем статус
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", (m.from_user.id, get_now(), row['id']))
        await db.commit()
        
        # Уведомляем 3-го в очереди
        next_in_line = await (await db.execute("SELECT user_id FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1 OFFSET 2", (conf['value'],))).fetchone()
        if next_in_line:
            try: await bot.send_message(next_in_line['user_id'], "🔔 <b>Приготовьтесь!</b>\nСкоро ваша очередь (вы 3-й).")
            except: pass

    await m.answer(f"🚀 <b>Вы взяли номер</b>\n📱 <code>{row['phone']}</code>\nОжидайте код.", 
                   reply_markup=worker_kb(row['id'], row['tariff_name']), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"⚡ <b>Ваш номер {mask_phone(row['phone'], row['user_id'])} взяли!</b>\nОжидайте код/QR.", parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def worker_code_req(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Формат: <code>/code +7...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер.")
    
    try:
        await bot.send_message(row['user_id'], 
                               f"🔔 <b>Офис запросил номер</b>\n📱 {mask_phone(ph, row['user_id'])}\n👇 <b>Ответьте ниже сообщением чтобы дать код.</b>", 
                               reply_markup=types.ForceReply(selective=True), parse_mode="HTML")
        await m.reply("✅ Запрос отправлен юзеру.")
    except: await m.reply("❌ Ошибка отправки (юзер блок?).")

# --- ОТВЕТ ЮЗЕРА (ПЕРЕСЫЛКА) ---
@router.message(F.reply_to_message)
async def user_reply_handler(m: Message, bot: Bot):
    # Проверка: сообщение юзера, есть реплай, и этот реплай от бота
    if m.from_user.id == ADMIN_ID: return 
    if m.reply_to_message.from_user.id == bot.id:
        async with get_db() as db:
            # Ищем активный номер этого юзера
            row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
        
        if row:
            content = m.text or "[Файл/Фото]"
            try:
                await bot.send_message(row['worker_id'], f"📩 <b>ОТВЕТ ЮЗЕРА</b>\n📱 {row['phone']}\n💬 <code>{content}</code>", parse_mode="HTML")
                await m.answer("✅ Отправлено офису.")
            except: 
                await m.answer("❌ Ошибка доставки офису.")

# --- ФОТО ОТ ВОРКЕРА (/sms) ---
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
    
    if not row: return await m.reply("❌ Номер не в работе.")
    
    try:
        await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"🔔 <b>SMS / QR</b>\n{text}", parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except: await m.reply("❌ Не доставлено.")

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
        # Возврат в очередь
        await db.execute("UPDATE numbers SET status='queue', worker_id=0 WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text("⏭ <b>Пропуск</b> (номер вернулся в очередь)", parse_mode="HTML")
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
        await c.message.edit_text("❌ Ошибка")
        try: await bot.send_message(row['user_id'], "❌ Ошибка номера.")
        except: pass

# ==========================================
# АДМИН ПАНЕЛЬ
# ==========================================
@router.callback_query(F.data == "admin_main")
async def adm_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы (Изм)", callback_data="adm_tariffs")
    kb.button(text="📄 Отчеты", callback_data="adm_reports")
    kb.button(text="📦 Очередь (Текст)", callback_data="adm_queue")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡️ <b>Админ панель</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- ОТЧЕТЫ (ПЕРИОДЫ) ---
@router.callback_query(F.data == "adm_reports")
async def adm_reports(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    periods = [1, 3, 5, 9, 12, 15, 24, 48, 72, 168] # Часы (168 = неделя)
    for h in periods:
        lbl = f"{h}ч" if h < 25 else f"{h//24}д"
        kb.button(text=lbl, callback_data=f"rep_{h}")
    kb.button(text="🔙", callback_data="admin_main")
    kb.adjust(4, 4, 2, 1)
    await c.message.edit_text("📊 Выберите период отчета:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_"))
async def adm_get_rep(c: CallbackQuery):
    hours = int(c.data.split("_")[1])
    cut = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM numbers WHERE created_at >= ? ORDER BY id DESC", (cut,))).fetchall()
    
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'Phone', 'Status', 'Tariff', 'Date'])
    for r in rows:
        w.writerow([r['id'], r['phone'], r['status'], r['tariff_name'], format_time(r['created_at'])])
    
    out.seek(0)
    await c.message.answer_document(BufferedInputFile(out.getvalue().encode(), filename=f"report_{hours}h.csv"), caption=f"📊 Отчет за {hours}ч: {len(rows)} строк")
    await c.answer()

# --- ТАРИФЫ (ВРЕМЯ -> ПРАЙС) ---
@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙", callback_data="admin_main")
    await c.message.edit_text("Выберите тариф:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("ed_"))
async def ed_t1(c: CallbackQuery, state: FSMContext):
    await state.update_data(target=c.data.split("_")[1])
    await state.set_state(AdminState.edit_time)
    await c.message.edit_text("1️⃣ Введите <b>ВРЕМЯ РАБОТЫ</b> (например: <code>10:00-22:00 МСК</code>):", parse_mode="HTML")

@router.message(AdminState.edit_time)
async def ed_t2(m: Message, state: FSMContext):
    await state.update_data(time=m.text)
    await state.set_state(AdminState.edit_price)
    await m.answer("2️⃣ Введите <b>ПРАЙС</b> (например: <code>10$</code> или <code>100₽</code>):", parse_mode="HTML")

@router.message(AdminState.edit_price)
async def ed_t3(m: Message, state: FSMContext):
    d = await state.get_data()
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, work_time=? WHERE name=?", (m.text, d['time'], d['target']))
        await db.commit()
    await state.clear()
    await m.answer("✅ Тариф сохранен!", reply_markup=main_kb(ADMIN_ID))

# --- РАССЫЛКА И ОЧЕРЕДЬ ---
@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 Пришлите пост для рассылки:")

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: Message, state: FSMContext):
    await state.clear()
    msg = await m.answer("⏳ Рассылаю...")
    async with get_db() as db:
        users = await (await db.execute("SELECT user_id FROM users WHERE is_banned=0")).fetchall()
    
    cnt = 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            cnt += 1
            await asyncio.sleep(0.05)
        except: pass
    await msg.edit_text(f"✅ Ушло {cnt} юзерам.")

@router.callback_query(F.data == "adm_queue")
async def adm_queue_list(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT id, phone, tariff_name FROM numbers WHERE status='queue'")).fetchall()
    if not rows: return await c.answer("Пусто", show_alert=True)
    txt = "\n".join([f"{r['id']}. {r['phone']} ({r['tariff_name']})" for r in rows])
    await c.message.answer(f"📦 Очередь:\n{txt}"[:4000])
    await c.answer()

# --- ПОДДЕРЖКА ---
@router.callback_query(F.data == "ask_supp")
async def ask_supp(c: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text("📝 Вопрос:", reply_markup=kb.as_markup())
    await state.set_state(UserState.waiting_support)

@router.message(UserState.waiting_support)
async def send_supp(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="Ответить", callback_data=f"reply_{m.from_user.id}")
    try:
        await bot.send_message(ADMIN_ID, f"🆘 <b>Вопрос от {m.from_user.id}:</b>\n{m.text}", reply_markup=kb.as_markup(), parse_mode="HTML")
        await m.answer("✅ Отправлено.")
    except: await m.answer("❌ Ошибка.")

@router.callback_query(F.data.startswith("reply_"))
async def adm_reply(c: CallbackQuery, state: FSMContext):
    await state.update_data(ruid=c.data.split("_")[1])
    await state.set_state(AdminState.support_reply)
    await c.message.answer("✍️ Ответ:")

@router.message(AdminState.support_reply)
async def send_reply(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    try:
        await bot.send_message(d['ruid'], f"👨‍💻 <b>Поддержка:</b>\n{m.text}", parse_mode="HTML")
        await m.answer("✅")
    except: await m.answer("❌ Блок")
    await state.clear()

@router.callback_query(F.data == "back_main")
async def back_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Меню", reply_markup=main_kb(c.from_user.id))

@router.callback_query(F.data.startswith("acc_"))
async def acc_dec(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        if act == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await bot.send_message(uid, "✅ <b>Доступ открыт!</b>", parse_mode="HTML")
            await c.message.edit_text(f"✅ {uid} принят")
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            await c.message.edit_text(f"🚫 {uid} забанен")
        await db.commit()

# --- СИСТЕМНЫЕ КОМАНДЫ ДЛЯ ВОРКЕРА ---
@router.message(Command("startwork"))
async def sys_start(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    await m.answer("Привязать тариф:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def sys_bind(c: CallbackQuery):
    t = c.data.split("_")[1]
    cid, tid = c.message.chat.id, (c.message.message_thread_id if c.message.is_topic_message else 0)
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", t))
        await db.commit()
    
    txt = (f"✅ <b>Чат привязан!</b> Тариф: {t}\n\n"
             "👨‍💻 <b>Гайд по использованию:</b>\n"
             "Пиши <code>/num</code> -> Получишь номер.\n"
             "Вбей номер в WhatsApp Web.\n"
             "Если просят QR: Сфоткай QR с экрана.\n"
             "Скинь фото сюда и подпиши: <code>/sms +77... Сканируй</code>\n"
             "Если просят Код (по номеру): Сфоткай код с экрана.\n"
             "Скинь фото сюда и подпиши: <code>/sms +77... Вводи этот код</code>\n"
             "Когда зашел -> жми <b>✅ Встал</b>.\n"
             "Когда номер слетел -> жми <b>📉 Слет</b>.\n\n"
             "<b>ДЛЯ MAX:</b> Используй <code>/code +7...</code>")
    await c.message.edit_text(txt, parse_mode="HTML")

@router.message(Command("stopwork"))
async def sys_stop(m: Message):
    if m.from_user.id != ADMIN_ID: return
    cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (f"topic_{cid}_{tid}",))
        await db.commit()
    await m.reply("🛑 Отвязано")

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запуск фонового AFK монитора
    asyncio.create_task(afk_monitor(bot))
    
    logger.info("🚀 BOT STARTED v60.0")
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
