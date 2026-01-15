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
    from aiogram.exceptions import TelegramBadRequest
except ImportError:
    sys.exit("❌ pip install aiogram aiosqlite")

# ==========================================
# ⚙️ КОНФИГУРАЦИЯ
# ==========================================
# Замените токен и ID на свои, если не используете .env
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "fast_team_v35_final.db" 

# Таймеры (минуты)
AFK_CHECK_MINUTES = 8   # Через сколько спросить "Вы тут?"
AFK_KICK_MINUTES = 3    # Сколько ждать ответа перед киком
CODE_WAIT_MINUTES = 4   # Тайм-аут ожидания кода

SEP = "━━━━━━━━━━━━━━━━━━━━"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("FAST_TEAM")
router = Router()

# ==========================================
# 🗄 БАЗА ДАННЫХ
# ==========================================
@asynccontextmanager
async def get_db():
    conn = await aiosqlite.connect(DB_NAME)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    try: yield conn
    finally: await conn.close()

async def init_db():
    async with get_db() as db:
        # Пользователи
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
                is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Номера
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
        # Тарифы
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY, price TEXT, hold_time TEXT, work_time TEXT
            )
        """)
        # Конфигурация (привязка топиков)
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '20 мин', '10:00-22:00')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '1 час', '24/7')")
        
        await db.commit()
    logger.info(f"✅ Database {DB_NAME} initialized")

# ==========================================
# 🛠 УТИЛИТЫ
# ==========================================
def clean_phone(phone):
    if not phone: return None
    clean = re.sub(r'[^\d]', '', str(phone))
    if len(clean) < 10 or len(clean) > 15: return None
    # Нормализация для СНГ
    if clean.startswith('77') and len(clean) == 11: return '+' + clean
    if clean.startswith('8') and len(clean) == 11: clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    return '+' + clean if clean.isdigit() else None

def mask_phone(phone, user_id):
    if user_id == ADMIN_ID: return phone
    try:
        if len(phone) < 9: return phone
        return f"{phone[:5]}***{phone[-4:]}"
    except: return phone

def get_now_iso(): 
    return datetime.now(timezone.utc).isoformat()

def format_dt(iso_str):
    """Превращает ISO в YYYY-MM-DD HH:MM"""
    try:
        if not iso_str: return "-"
        dt = datetime.fromisoformat(str(iso_str))
        # Корректировка часового пояса (например +3 для МСК/+5 Актау, ставим серверное или UTC)
        # Здесь оставляем UTC или добавляем смещение. Для примера +3
        local_dt = dt + timedelta(hours=3) 
        return local_dt.strftime("%Y-%m-%d %H:%M")
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
# 🚦 СОСТОЯНИЯ (FSM)
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_help_msg = State()

class AdminState(StatesGroup):
    edit_price = State()
    edit_time = State()
    reply_to_user = State()
    report_hours = State()
    bind_tariff = State()

# ==========================================
# ⌨️ КЛАВИАТУРЫ
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="help_menu")
    if user_id == ADMIN_ID:
        kb.button(text="⚡ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 1)
    return kb.as_markup()

def help_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🆘 Поддержка", callback_data="ask_support")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def worker_kb_wa(nid):
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
# 👋 START И АВТОРИЗАЦИЯ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    uid = m.from_user.id
    username = f"@{m.from_user.username}" if m.from_user.username else "NoUsername"
    
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        
        if not res:
            # Новый юзер
            await db.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                             (uid, username, m.from_user.first_name))
            await db.commit()
            
            # Уведомление админу
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"),
                    InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                ]])
                try:
                    await bot.send_message(ADMIN_ID, f"👤 <b>Запрос доступа:</b>\nID: <code>{uid}</code>\nUser: {username}", 
                                           reply_markup=kb, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"Failed to notify admin: {e}")
                    
            return await m.answer("🔒 <b>Доступ ограничен.</b>\nОжидайте одобрения администратора.", parse_mode="HTML")
        
        # Проверка статуса
        if res['is_banned']:
            return await m.answer("🚫 Вы заблокированы.")
        
        if not res['is_approved']:
            return await m.answer("⏳ Заявка на рассмотрении.")
            
        await m.answer(f"👋 Привет, {m.from_user.first_name}!\n{SEP}", reply_markup=main_kb(uid))

@router.callback_query(F.data == "back_main")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Главное меню\n{SEP}", reply_markup=main_kb(c.from_user.id))
    await c.answer()

# ==========================================
# 👮‍♂️ АДМИН: ОДОБРЕНИЕ ДОСТУПА
# ==========================================
@router.callback_query(F.data.startswith("acc_"))
async def cb_account_decision(c: CallbackQuery, bot: Bot):
    # Проверка прав (на всякий случай, хотя сообщение у админа)
    if c.from_user.id != ADMIN_ID:
        return await c.answer("🚫")

    parts = c.data.split("_")
    action = parts[1] # ok / no
    target_uid = int(parts[2])
    
    async with get_db() as db:
        if action == "ok":
            await db.execute("UPDATE users SET is_approved=1, is_banned=0 WHERE user_id=?", (target_uid,))
            adm_text = f"✅ Доступ выдан ID: {target_uid}"
            user_text = "✅ <b>Вам одобрен доступ!</b>\nЖмите /start для начала работы."
        else:
            await db.execute("UPDATE users SET is_banned=1, is_approved=0 WHERE user_id=?", (target_uid,))
            adm_text = f"🚫 Пользователь забанен ID: {target_uid}"
            user_text = "🚫 Вам отказано в доступе."
        await db.commit()
    
    # Редактируем сообщение у админа
    await c.message.edit_text(adm_text)
    # Шлем юзеру
    try: await bot.send_message(target_uid, user_text, parse_mode="HTML")
    except: pass
    await c.answer()

# ==========================================
# 👷‍♂️ ВОРКЕР: ЛОГИКА /STARTWORK /NUM
# ==========================================
@router.message(Command("startwork"))
async def cmd_startwork(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    
    async with get_db() as db:
        tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in tariffs:
        kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    kb.adjust(1)
    
    await m.answer("🛠 Выберите тариф для привязки к этому чату/топику:", reply_markup=kb.as_markup())
    await state.set_state(AdminState.bind_tariff)

@router.callback_query(AdminState.bind_tariff, F.data.startswith("bind_"))
async def cb_bind_save(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", 
                         (f"topic_{cid}_{tid}", t_name))
        await db.commit()
    
    guide_text = (
        f"✅ <b>Чат привязан к тарифу: {t_name}!</b>\n\n"
        f"👨‍💻 <b>Гайд по использованию:</b>\n\n"
        f"1️⃣ Пиши /num -> Получишь номер.\n\n"
        f"2️⃣ Вбей номер в WhatsApp Web / Эмулятор.\n\n"
        f"3️⃣ <b>Если просят QR:</b> Сфоткай QR с экрана.\n"
        f"   Скинь фото сюда и подпиши: <code>/sms +7... Сканируй</code>\n\n"
        f"4️⃣ <b>Если просят Код:</b> Сфоткай код с экрана.\n"
        f"   Скинь фото сюда и подпиши: <code>/sms +7... Вводи этот код</code>\n\n"
        f"5️⃣ Когда зашел -> жми <b>✅ Встал</b>.\n"
        f"6️⃣ Когда номер слетел -> жми <b>📉 Слет</b>."
    )
    
    await c.message.edit_text(guide_text, parse_mode="HTML")
    await state.clear()

@router.message(Command("stopwork"))
async def cmd_stopwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (f"topic_{cid}_{tid}",))
        await db.commit()
    await m.reply("❌ Топик отвязан. Бот здесь больше не работает.")

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    
    async with get_db() as db:
        # 1. Проверяем привязку
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return # Игнорим если не привязан
        
        t_name = conf['value']
        
        # 2. Ищем номер
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (t_name,))).fetchone()
        if not row:
            return await m.reply("📭 <b>Очередь пуста</b>", parse_mode="HTML")
        
        # 3. Бронируем
        now = get_now_iso()
        await db.execute("""
            UPDATE numbers 
            SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? 
            WHERE id=?
        """, (m.from_user.id, cid, tid, now, row['id']))
        await db.commit()

    # 4. Сообщение Воркеру
    is_max = "MAX" in t_name.upper()
    
    txt_worker = (
        f"🚀 <b>Вы взяли номер.</b>\n{SEP}\n"
        f"📱 <code>{row['phone']}</code>\n"
        f"💰 {row['tariff_price']} | ⏳ {row['tariff_hold']}\n{SEP}\n"
    )
    
    if is_max:
        txt_worker += f"Код: <code>/code {row['phone']}</code>"
        kb = worker_kb_max(row['id'])
    else:
        txt_worker += f"Код: <code>/sms {row['phone']} текст</code>"
        kb = worker_kb_wa(row['id'])

    await m.answer(txt_worker, reply_markup=kb, parse_mode="HTML")

    # 5. Сообщение Юзеру
    txt_user = (
        f"⚡ <b>Ваш номер взяли!</b>\n"
        f"📱 {mask_phone(row['phone'], 0)}\n"
        f"Ожидайте код."
    )
    try: await bot.send_message(row['user_id'], txt_user, parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def cmd_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args: 
        return await m.reply("⚠️ Формат: <code>/code +7999...</code>", parse_mode="HTML")
    
    ph = clean_phone(command.args.split()[0])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Не ваш номер или неверный статус.")

    # Обновляем метку ожидания
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now_iso(), row['id']))
        await db.commit()

    try:
        await bot.send_message(row['user_id'], 
                               "🔔 <b>Офис запросил номер</b>\nОтветьте ниже сообщением, чтобы дать код.", 
                               parse_mode="HTML")
        await m.reply("✅ Запрос отправлен юзеру.")
    except:
        await m.reply("❌ Не удалось отправить (юзер заблочил бота?)")

# ==========================================
# 🏗 ВОРКЕР: КНОПКИ ДЕЙСТВИЙ
# ==========================================
@router.callback_query(F.data.startswith("w_"))
async def cb_worker_actions(c: CallbackQuery, bot: Bot):
    parts = c.data.split("_")
    action = parts[1] # act, skip, err, drop
    nid = parts[2]
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        
        if not row: return await c.answer("Номер не найден в БД")
        
        # ЗАЩИТА: Только текущий воркер
        if row['worker_id'] != c.from_user.id:
            return await c.answer("🔒 Не ты брал этот номер!", show_alert=True)

        user_msg = ""
        adm_msg = ""
        new_kb = None
        now = get_now_iso()

        if action == "act": # Встал
            await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
            adm_msg = f"✅ Номер встал\n📱 {row['phone']}"
            user_msg = "✅ Номер встал и все"
            new_kb = worker_active_kb(nid)
            
        elif action == "err": # Ошибка (WA)
            await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (now, nid))
            adm_msg = "❌ Ошибка"
            user_msg = "❌ Ошибка"
            
        elif action == "skip": # Пропуск (MAX) - сброс или отмена? 
            # По просьбе: "Офис пропустил ваш номер" -> это больше похоже на отказ.
            # Ставим статус dead (или можно вернуть в queue, но текст юзеру звучит как отказ)
            # Реализуем как отмену текущей сессии.
            await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (now, nid))
            adm_msg = "⏭ Пропуск"
            user_msg = "⚠️ Офис пропустил ваш номер."
            
        elif action == "drop": # Слет
            await db.execute("UPDATE numbers SET status='finished', end_time=? WHERE id=?", (now, nid))
            dur = calc_duration(row['start_time'], now)
            adm_msg = f"📉 Слет ({dur})"
            user_msg = f"📉 Ваш номер слетел\nВремя работы: {dur}"
        
        await db.commit()
    
    # Обновляем сообщение воркера
    await c.message.edit_text(adm_msg, reply_markup=new_kb)
    
    # Шлем юзеру
    if user_msg:
        try: await bot.send_message(row['user_id'], user_msg, parse_mode="HTML")
        except: pass
        
    await c.answer()

# ==========================================
# 👤 ЮЗЕР: МЕНЮ И ЗАГРУЗКА
# ==========================================
@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db: 
        tariffs = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in tariffs: 
        kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"pick_{t['name']}")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text(f"📂 Выберите тариф для сдачи:\n{SEP}", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick_tariff(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    async with get_db() as db: 
        t = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (tn,))).fetchone()
    
    if not t: return await c.answer("Тариф не найден", show_alert=True)

    await state.update_data(tariff=tn, price=t['price'], hold=t['hold_time'], work_time=t['work_time'])
    await state.set_state(UserState.waiting_numbers)
    
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    
    msg = (
        f"💎 Тариф: <b>{tn}</b>\n"
        f"💰 Прайс: {t['price']}\n"
        f"⏳ Холд: {t['hold_time']}\n{SEP}\n"
        f"📱 <b>Отправьте номера списком</b> (каждый с новой строки или через запятую)."
    )
    await c.message.edit_text(msg, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.message(UserState.waiting_numbers)
async def fsm_receive_numbers(m: Message, state: FSMContext):
    data = await state.get_data()
    raw_lines = re.split(r'[;,\n]', m.text)
    
    valid_nums = []
    bad_count = 0
    
    for x in raw_lines:
        ph = clean_phone(x.strip())
        if ph: valid_nums.append(ph)
        elif x.strip(): bad_count += 1
    
    if not valid_nums:
        return await m.answer("❌ Нет валидных номеров.\nФормат: +79991234567")
    
    report = f"✅ <b>Принято в очередь: {len(valid_nums)}</b>\n{SEP}\n"
    
    async with get_db() as db:
        for ph in valid_nums:
            cur = await db.execute(
                """INSERT INTO numbers 
                   (user_id, phone, tariff_name, tariff_price, tariff_hold, work_time, last_ping) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (m.from_user.id, ph, data['tariff'], data['price'], data['hold'], data['work_time'], get_now_iso())
            )
            nid = cur.lastrowid
            # Позиция
            pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id <= ?", (nid,))).fetchone())[0]
            report += f"📱 {mask_phone(ph, m.from_user.id)} — <b>{pos}#</b>\n"
        await db.commit()
    
    if bad_count > 0:
        report += f"\n⚠️ Не прошло проверку: {bad_count} шт."
    
    await state.clear()
    await m.answer(report, reply_markup=main_kb(m.from_user.id), parse_mode="HTML")

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        stats = await (await db.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN status='queue' THEN 1 ELSE 0 END) as queue
            FROM numbers WHERE user_id=?
        """, (uid,))).fetchone()
    
    msg = (
        f"👤 <b>Личный кабинет</b>\n{SEP}\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📦 Загружено: {stats['total']}\n"
        f"🔥 В работе: {stats['active']}\n"
        f"⏳ В очереди: {stats['queue']}"
    )
    await c.message.edit_text(msg, reply_markup=back_kb(), parse_mode="HTML")

# ==========================================
# ℹ️ ПОМОЩЬ И ПОДДЕРЖКА
# ==========================================
@router.callback_query(F.data == "help_menu")
async def cb_help_menu(c: CallbackQuery):
    text = (
        f"📲 <b>Что делает бот</b>\n"
        f"Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
        f"📦 <b>Требования к номерам</b>\n"
        f"✔️ Активный и чистый номер\n"
        f"✔️ Доступ к SMS\n"
        f"❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
        f"⏳ <b>Холд и выплаты</b>\n"
        f"Холд — время проверки номера.\n"
        f"💰 Выплата производится после успешного завершения холда.\n\n"
        f"⚠️ <i>Отправляя номер, вы подтверждаете, что ознакомились с правилами.</i>"
    )
    await c.message.edit_text(text, reply_markup=help_kb(), parse_mode="HTML")

@router.callback_query(F.data == "ask_support")
async def cb_ask_support(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_help_msg)
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text("✍️ Напишите ваш вопрос ниже:", reply_markup=kb.as_markup())

@router.message(UserState.waiting_help_msg)
async def fsm_send_ticket(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    
    # Кнопка для админа
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Ответить", callback_data=f"reply_{m.from_user.id}")
    
    admin_msg = f"🆘 <b>Новый запрос</b>\nОт: {m.from_user.id} (@{m.from_user.username})\n\n{m.text}"
    
    try:
        await bot.send_message(ADMIN_ID, admin_msg, reply_markup=kb.as_markup(), parse_mode="HTML")
        await m.answer("✅ Сообщение отправлено администрации.", reply_markup=main_kb(m.from_user.id))
    except:
        await m.answer("❌ Ошибка отправки.")

# ==========================================
# ⚡ АДМИН ПАНЕЛЬ
# ==========================================
@router.callback_query(F.data == "admin_main")
async def cb_admin_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Отчеты", callback_data="adm_reports")
    kb.button(text="📝 Изменить тарифы", callback_data="adm_edit_tariffs")
    kb.button(text="🔙 Выход", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("⚡ Админ панель", reply_markup=kb.as_markup())

# --- ОТВЕТ ПОЛЬЗОВАТЕЛЮ ---
@router.callback_query(F.data.startswith("reply_"))
async def cb_adm_reply_start(c: CallbackQuery, state: FSMContext):
    uid = c.data.split("_")[1]
    await state.update_data(reply_uid=uid)
    await state.set_state(AdminState.reply_to_user)
    await c.message.answer(f"✍️ Введите ответ для ID {uid}:")
    await c.answer()

@router.message(AdminState.reply_to_user)
async def fsm_adm_reply_send(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    uid = data.get('reply_uid')
    await state.clear()
    
    try:
        await bot.send_message(uid, f"👨‍💻 <b>Поддержка:</b>\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ отправлен.")
    except:
        await m.answer("❌ Не доставлено (юзер заблокировал бота).")

# --- ОТЧЕТЫ ---
@router.callback_query(F.data == "adm_reports")
async def cb_adm_reports(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="24 часа", callback_data="rep_24")
    kb.button(text="48 часов", callback_data="rep_48")
    kb.button(text="120 часов", callback_data="rep_120")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(3, 1)
    
    await c.message.edit_text("📊 Выберите период отчета (до 120ч):", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("rep_"))
async def cb_gen_report(c: CallbackQuery):
    hours = int(c.data.split("_")[1])
    dt_start = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT * FROM numbers WHERE created_at >= ? ORDER BY id DESC
        """, (dt_start,))).fetchall()
        
    if not rows:
        return await c.answer("📂 Записей не найдено.", show_alert=True)
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'User', 'Phone', 'Status', 'Tariff', 'Created', 'Start', 'End'])
    
    for r in rows:
        writer.writerow([
            r['id'], r['user_id'], r['phone'], r['status'], r['tariff_name'],
            format_dt(r['created_at']), format_dt(r['start_time']), format_dt(r['end_time'])
        ])
    
    output.seek(0)
    doc = BufferedInputFile(output.getvalue().encode(), filename=f"report_{hours}h.csv")
    await c.message.answer_document(doc, caption=f"📊 Отчет за последние {hours}ч")
    await c.answer()

# --- ТАРИФЫ ---
@router.callback_query(F.data == "adm_edit_tariffs")
async def cb_adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in ts:
        kb.button(text=f"✏️ {t['name']}", callback_data=f"edt_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    
    await c.message.edit_text("🛠 Выберите тариф для изменения:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("edt_"))
async def cb_edit_t_price(c: CallbackQuery, state: FSMContext):
    target = c.data.split("_")[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_price)
    await c.message.edit_text(f"1️⃣ Введите новую **ЦЕНУ** для {target}:", parse_mode="Markdown")

@router.message(AdminState.edit_price)
async def fsm_t_price(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_time)
    await m.answer("2️⃣ Введите новое **ВРЕМЯ РАБОТЫ** (например 10:00-22:00):")

@router.message(AdminState.edit_time)
async def fsm_t_time(m: Message, state: FSMContext):
    data = await state.get_data()
    
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, work_time=? WHERE name=?", 
                         (data['price'], m.text, data['target']))
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ Тариф {data['target']} обновлен!\nЦена: {data['price']}\nВремя: {m.text}")

# ==========================================
# 📨 ПЕРЕСЫЛКА СООБЩЕНИЙ (BRIDGE)
# ==========================================
@router.message(F.text | F.photo)
async def bridge_handler(m: Message, bot: Bot):
    # Если это админ или команда - игнор
    if m.text and m.text.startswith('/'): return
    if m.from_user.id == ADMIN_ID: return
    
    # 1. Если это ВОРКЕР присылает /sms (или фото с caption /sms)
    # Это обрабатывается как команда или текст?
    # Если воркер просто пишет в топик -> это пересылается юзеру? Нет, бот должен пересылать ТОЛЬКО ответы на запросы юзера?
    # В ТЗ: "Скинь фото сюда и подпиши: /sms..." -> это команда.
    
    # 2. Если это ЮЗЕР пишет боту
    async with get_db() as db:
        # Ищем активный номер юзера
        row = await (await db.execute("""
            SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')
        """, (m.from_user.id,))).fetchone()
        
    if row and row['worker_chat_id']:
        # Сброс таймера ожидания кода
        if row['wait_code_start']:
            async with get_db() as db:
                await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
                await db.commit()
        
        # Формируем сообщение в топик воркера
        txt = f"📩 <b>Ответ юзера ({row['phone']})</b>:\n{m.text if m.text else '[Вложение]'}"
        try:
            if m.photo:
                await bot.send_photo(row['worker_chat_id'], m.photo[-1].file_id, 
                                     caption=txt, message_thread_id=row['worker_thread_id'], parse_mode="HTML")
            else:
                await bot.send_message(row['worker_chat_id'], txt, 
                                       message_thread_id=row['worker_thread_id'], parse_mode="HTML")
            await m.react([ReactionTypeEmoji(emoji="⚡")])
        except Exception as e:
            logger.error(f"Bridge error: {e}")

# Команда /sms для Воркеров (пересылка юзеру)
@router.message(Command("sms"))
async def cmd_sms(m: Message, command: CommandObject, bot: Bot):
    # Парсинг: /sms +7999... Текст
    args = m.text.split(maxsplit=2) # /sms, phone, text
    if len(args) < 3 and not (m.caption and "/sms" in m.caption):
        return await m.reply("⚠️ Пример: <code>/sms +7999... Текст</code>\nИли фото с подписью.", parse_mode="HTML")
    
    # Извлекаем номер
    raw_ph = args[1] if len(args) >= 2 else (m.caption.split()[1] if m.caption else "")
    ph = clean_phone(raw_ph)
    
    # Извлекаем текст
    text_to_send = args[2] if len(args) >= 3 else ""
    if m.caption:
        # Если фото, текст берем из caption, убирая команду и номер
        # Простой вариант: все после номера
        parts = m.caption.split(maxsplit=2)
        if len(parts) > 2: text_to_send = parts[2]
        else: text_to_send = "См. фото"

    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id:
        return await m.reply("❌ Номер не ваш или не активен.")
    
    try:
        if m.photo:
            await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=text_to_send)
        else:
            await bot.send_message(row['user_id'], text_to_send)
        await m.reply("✅ Отправлено юзеру.")
    except:
        await m.reply("❌ Не доставлено.")

# ==========================================
# 🔄 МОНИТОРИНГ ФОНОВЫЙ
# ==========================================
async def monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now_dt = datetime.now(timezone.utc)
            now_iso = get_now_iso()
            
            async with get_db() as db:
                # 1. Тайм-аут ожидания кода
                waiters = await (await db.execute("SELECT * FROM numbers WHERE wait_code_start IS NOT NULL")).fetchall()
                for w in waiters:
                    st = datetime.fromisoformat(w['wait_code_start'])
                    if (now_dt - st).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead', end_time=? WHERE id=?", (now_iso, w['id']))
                        try: await bot.send_message(w['user_id'], f"⏰ Время ожидания вышло. Номер {w['phone']} отменен.")
                        except: pass
                
                # 2. AFK проверка очереди
                queue = await (await db.execute("SELECT * FROM numbers WHERE status='queue'")).fetchall()
                for r in queue:
                    last_ping = r['last_ping'] if r['last_ping'] else r['created_at']
                    
                    if "PENDING" in str(last_ping): # Уже спросили
                        pt_str = last_ping.split("_")[1]
                        pt = datetime.fromisoformat(pt_str)
                        if (now_dt - pt).total_seconds() / 60 >= AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                            # Можно уведомить юзера о кике
                    else:
                        la = datetime.fromisoformat(last_ping)
                        if (now_dt - la).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            kb = InlineKeyboardBuilder().button(text="👋 Я тут", callback_data=f"afk_alive_{r['id']}").as_markup()
                            try:
                                await bot.send_message(r['user_id'], "⚠️ <b>Проверка активности!</b>\nНажмите кнопку, чтобы остаться в очереди.", reply_markup=kb, parse_mode="HTML")
                                await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{now_iso}", r['id']))
                            except:
                                await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],)) # Юзер заблочил бота

                await db.commit()
        except Exception as e:
            logger.error(f"Monitor Loop Error: {e}")

@router.callback_query(F.data.startswith("afk_alive_"))
async def cb_afk_alive(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now_iso(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("✅ Спасибо, вы в очереди!")

# ==========================================
# 🚀 MAIN LOOP
# ==========================================
async def main():
    await init_db()
    
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    # Удаляем вебхуки для поллинга
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запуск фоновых задач
    asyncio.create_task(monitor(bot))
    
    logger.info(f"🚀 BOT STARTED (Admin: {ADMIN_ID})")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
