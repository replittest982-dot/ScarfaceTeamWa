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
    sys.exit("❌ Ошибка: Не установлены библиотеки. Выполните: pip install aiogram aiosqlite")

# ==========================================
# 1. КОНФИГУРАЦИЯ И НАСТРОЙКИ
# ==========================================
# Замените на свои значения или используйте .env
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "fast_team_v21.db"

# Настройки времени (в минутах)
AFK_CHECK_MINUTES = 8   # Через сколько проверять активность
AFK_KICK_MINUTES = 3    # Сколько ждать ответа на кнопку
SEP = "━━━━━━━━━━━━━━━━━━━━"

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

router = Router()

# Проверка токена
if not TOKEN or "YOUR_TOKEN" in TOKEN:
    sys.exit("❌ FATAL: BOT_TOKEN не указан!")

# ==========================================
# 2. БАЗА ДАННЫХ
# ==========================================
@asynccontextmanager
async def get_db():
    conn = await aiosqlite.connect(DB_NAME, timeout=30)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
    finally:
        await conn.close()

async def init_db():
    async with get_db() as db:
        # Пользователи
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_approved INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                last_afk_check TEXT,
                reg_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Номера (Заявки)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                phone TEXT,
                tariff_name TEXT,
                tariff_price TEXT,
                status TEXT DEFAULT 'queue',
                worker_id INTEGER DEFAULT 0,
                worker_chat_id INTEGER DEFAULT 0,
                worker_thread_id INTEGER DEFAULT 0,
                start_time TEXT,
                end_time TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Тарифы и Конфигурация (привязка топиков)
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (name TEXT PRIMARY KEY, price TEXT, work_time TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('WhatsApp','50₽','10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('MAX','10$','24/7')")
        await db.commit()
    logger.info("✅ База данных инициализирована (v21.0)")

# ==========================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================
def clean_phone(phone):
    """Очистка и форматирование номера"""
    if not phone: return None
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean) == 11:
        return '+' + clean
    if clean.startswith('8') and len(clean) == 11:
        clean = '7' + clean[1:]
    elif len(clean) == 10:
        clean = '7' + clean
    return '+' + clean if re.match(r'^7\d{10}$', clean) else None

def mask_phone(phone, user_id):
    """Скрытие номера для всех, кроме админа"""
    if user_id == ADMIN_ID: return phone
    try:
        if len(phone) < 9: return phone
        return f"{phone[:5]}***{phone[-4:]}"
    except:
        return phone

def get_now():
    """Текущее время в UTC ISO"""
    return datetime.now(timezone.utc).isoformat()

def format_report_dt(iso_str):
    """Красивая дата для отчетов"""
    try:
        dt = datetime.fromisoformat(iso_str)
        # Добавляем +3 часа для МСК (или настройте под себя)
        dt = dt + timedelta(hours=3) 
        return dt.strftime("%Y-%m-%d %H:%M")
    except:
        return iso_str

def calc_duration(start_iso, end_iso):
    """Расчет времени работы"""
    try:
        if not start_iso or not end_iso: return "0 мин"
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins // 60}ч {mins % 60}мин" if mins >= 60 else f"{mins} мин"
    except:
        return "0 мин"

# Состояния FSM
class UserState(StatesGroup):
    waiting_number = State() # Ожидание ввода номера
    waiting_question = State() # Ожидание вопроса в поддержку

class AdminState(StatesGroup):
    replying_to = State() # Админ отвечает юзеру
    waiting_tariff_price = State() # Изменение цены
    waiting_tariff_time = State() # Изменение времени

# ==========================================
# 4. КЛАВИАТУРЫ
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="🆘 Помощь", callback_data="ask_help")
    if user_id == ADMIN_ID:
        kb.button(text="⚙️ Админ панель", callback_data="admin_main")
    kb.adjust(1, 2, 1)
    return kb.as_markup()

def admin_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="📄 Отчеты", callback_data="adm_reports")
    kb.button(text="💰 Тарифы", callback_data="adm_tariffs")
    kb.button(text="🔙 Выход", callback_data="back_main")
    kb.adjust(1, 2, 1)
    return kb.as_markup()

# ==========================================
# 5. ХЕНДЛЕРЫ: СТАРТ И МЕНЮ
# ==========================================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        
        # Регистрация нового юзера
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name, last_afk_check) VALUES (?, ?, ?, ?)", 
                             (uid, m.from_user.username, m.from_user.first_name, get_now()))
            await db.commit()
            
            # Уведомление админу
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"acc_ok_{uid}"),
                    InlineKeyboardButton(text="🚫 Бан", callback_data=f"acc_no_{uid}")
                ]])
                try:
                    await m.bot.send_message(ADMIN_ID, f"👤 <b>Запрос доступа:</b>\nID: {uid}\nUser: @{m.from_user.username}", reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer("🔒 Доступ ограничен.\nОжидайте одобрения администратора.")
        
        if res['is_banned']:
            return await m.answer("🚫 Вы заблокированы.")
            
        if res['is_approved']:
            await m.answer(f"👋 Привет, {m.from_user.first_name}!\n{SEP}", reply_markup=main_kb(uid))
        else:
            await m.answer("⏳ Ваша заявка всё ещё на рассмотрении.")

@router.callback_query(F.data == "back_main")
async def cb_back_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 Главное меню\n{SEP}", reply_markup=main_kb(c.from_user.id))
    await c.answer()

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    async with get_db() as db:
        active = await (await db.execute("SELECT count(*) FROM numbers WHERE user_id=? AND status IN ('work','active')", (c.from_user.id,))).fetchone()
        finished = await (await db.execute("SELECT count(*) FROM numbers WHERE user_id=? AND status='finished'", (c.from_user.id,))).fetchone()
        
    txt = (f"👤 <b>Профиль</b>\n{SEP}\n"
           f"🆔 ID: <code>{c.from_user.id}</code>\n"
           f"⚡ Активных номеров: {active[0]}\n"
           f"✅ Сдано всего: {finished[0]}")
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]]), parse_mode="HTML")
    await c.answer()

# ==========================================
# 6. СИСТЕМА ПОДДЕРЖКИ (НОВОЕ)
# ==========================================
@router.callback_query(F.data == "ask_help")
async def cb_ask_help(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_question)
    await c.message.edit_text(f"🆘 <b>Поддержка</b>\n{SEP}\nНапишите ваш вопрос или проблему одним сообщением:", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="back_main")]]), 
                              parse_mode="HTML")
    await c.answer()

# ==========================================
# 7. ЮЗЕР: СДАЧА НОМЕРА
# ==========================================
@router.callback_query(F.data == "sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()
        
    kb = InlineKeyboardBuilder()
    for t in rows:
        kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"add_num_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    
    await c.message.edit_text("📥 Выберите сервис для сдачи номера:", reply_markup=kb.as_markup())
    await c.answer()

@router.callback_query(F.data.startswith("add_num_"))
async def cb_add_num_request(c: CallbackQuery, state: FSMContext):
    tariff = c.data.split("_")[2]
    await state.update_data(tariff=tariff)
    await state.set_state(UserState.waiting_number)
    
    await c.message.edit_text(
        f"📞 Выбран сервис: <b>{tariff}</b>\n{SEP}\nВведите номер телефона (например: +79001234567):", 
        parse_mode="HTML", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="back_main")]])
    )
    await c.answer()

# ==========================================
# 8. ОБРАБОТЧИК ВСЕХ ТЕКСТОВЫХ СООБЩЕНИЙ
# ==========================================
@router.message(F.text & ~F.text.startswith("/"))
async def handle_text_all(m: Message, state: FSMContext, bot: Bot):
    st = await state.get_state()
    
    # --- 1. Ввод номера ---
    if st == UserState.waiting_number:
        data = await state.get_data()
        tariff = data.get("tariff", "WhatsApp")
        clean = clean_phone(m.text)
        
        if not clean:
            return await m.reply("❌ Некорректный формат номера. Попробуйте снова (+7...).")
            
        async with get_db() as db:
            # Проверка дублей
            exists = await (await db.execute("SELECT id FROM numbers WHERE phone=? AND status IN ('queue','work','active')", (clean,))).fetchone()
            if exists:
                return await m.reply("❌ Этот номер уже находится в работе или очереди.")
            
            # Получаем цену тарифа
            t_row = await (await db.execute("SELECT price FROM tariffs WHERE name=?", (tariff,))).fetchone()
            price = t_row['price'] if t_row else "0"
            
            await db.execute("""
                INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, created_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (m.from_user.id, clean, tariff, price, get_now()))
            await db.commit()
            
        await state.clear()
        await m.answer(f"✅ Номер <b>{clean}</b> добавлен в очередь!\nТариф: {tariff}\nОжидайте воркера.", parse_mode="HTML")
        return

    # --- 2. Вопрос в поддержку ---
    if st == UserState.waiting_question:
        if not ADMIN_ID: return await m.reply("❌ Админ не настроен.")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✉️ Ответить", callback_data=f"adm_reply_{m.from_user.id}")
        ]])
        try:
            await bot.send_message(ADMIN_ID, f"🆘 <b>Новый запрос</b>\nОт: {m.from_user.first_name} (ID: {m.from_user.id})\n\n{m.text}", reply_markup=kb, parse_mode="HTML")
            await m.answer("✅ Сообщение отправлено. Ждите ответа.")
        except:
            await m.answer("❌ Ошибка отправки.")
        await state.clear()
        return

    # --- 3. Ответ админа пользователю ---
    if st == AdminState.replying_to:
        data = await state.get_data()
        target_id = data.get("target_id")
        try:
            await bot.send_message(target_id, f"📨 <b>Ответ поддержки:</b>\n{SEP}\n{m.text}", parse_mode="HTML")
            await m.answer("✅ Ответ отправлен.")
        except Exception as e:
            await m.answer(f"❌ Не удалось отправить: {e}")
        await state.clear()
        return

    # --- 4. Ответ юзера на запрос кода (MAX) ---
    # Если юзер просто пишет текст, проверяем, ждут ли от него код
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status='active'", (m.from_user.id,))).fetchone()
        
    if row and row['worker_chat_id']:
        try:
            msg = f"📩 <b>КОД от юзера:</b> <code>{m.text}</code>\nНомер: {mask_phone(row['phone'], 0)}"
            await bot.send_message(row['worker_chat_id'], msg, message_thread_id=row['worker_thread_id'], parse_mode="HTML")
            await m.answer("✅ Код передан воркеру!")
        except:
            pass # Игнорим ошибки

# ==========================================
# 9. ФУНКЦИОНАЛ ВОРКЕРА
# ==========================================
@router.message(Command("startwork"))
async def cmd_startwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        tariffs = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in tariffs:
        kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    kb.adjust(1)
    await m.answer("⚙️ <b>Настройка топика</b>\nВыберите тариф для привязки к этому чату:", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.message(Command("stopwork"))
async def cmd_stopwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    tid = m.message_thread_id if m.is_topic_message else 0
    key = f"topic_{m.chat.id}_{tid}"
    async with get_db() as db:
        await db.execute("DELETE FROM config WHERE key=?", (key,))
        await db.commit()
    await m.answer("🛑 Топик отвязан. Работа остановлена.")

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind_confirm(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: 
        await c.answer("Только админ!", show_alert=True)
        return
        
    tn = c.data.split("_")[1]
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_{c.message.chat.id}_{tid}"
    
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, tn))
        await db.commit()
        
    info = ""
    if tn == "MAX":
        info = (
            "👨‍💻 <b>Гайд по MAX:</b>\n"
            "1. <code>/num</code> -> Берешь номер.\n"
            "2. <code>/code +7...</code> -> Юзеру летит запрос.\n"
            "3. Юзер отвечает -> Приходит сюда."
        )
    else:
        info = (
            "👨‍💻 <b>Гайд по WhatsApp:</b>\n"
            "1. <code>/num</code> -> Берешь номер.\n"
            "2. Вбиваешь в WA.\n"
            "3. Скидываешь QR сюда (с подписью /sms +7...).\n"
        )
        
    await c.message.edit_text(f"✅ <b>Чат привязан!</b>\nТариф: {tn}\n\n{info}", parse_mode="HTML")
    await c.answer()

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    tid = m.message_thread_id if m.is_topic_message else 0
    key = f"topic_{m.chat.id}_{tid}"
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (key,))).fetchone()
        if not conf:
            return await m.reply("❌ Топик не настроен. Используйте /startwork")
        
        tariff_name = conf['value']
        
        # Поиск номера в очереди
        row = await (await db.execute("""
            SELECT * FROM numbers 
            WHERE status='queue' AND tariff_name=? 
            ORDER BY id ASC LIMIT 1
        """, (tariff_name,))).fetchone()
        
        if not row:
            return await m.reply("📭 Очередь пуста")
        
        # Обновление статуса
        await db.execute("""
            UPDATE numbers 
            SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? 
            WHERE id=?
        """, (m.from_user.id, m.chat.id, tid, get_now(), row['id']))
        
        # Сброс таймера AFK, чтобы не кикнуло во время работы
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), row['user_id']))
        await db.commit()
    
    # Сборка клавиатуры в зависимости от тарифа
    kb = InlineKeyboardBuilder()
    cmd_hint = ""
    
    if "MAX" in tariff_name.upper():
        # Логика MAX: Встал (успех) или Пропуск
        kb.button(text="✅ Встал", callback_data=f"w_suc_{row['id']}")
        kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{row['id']}")
        cmd_hint = f"Запросить код: <code>/code {row['phone']}</code>"
    else:
        # Логика WA: Встал (успех), Слет (ошибка позже), Ошибка (сразу)
        kb.button(text="✅ Встал", callback_data=f"w_suc_{row['id']}")
        kb.button(text="📉 Слет", callback_data=f"w_drop_{row['id']}")
        cmd_hint = f"QR/Код: <code>/sms {row['phone']} текст</code>"

    kb.button(text="❌ Ошибка", callback_data=f"w_err_{row['id']}")
    kb.adjust(2, 1)

    await m.answer(
        f"🚀 <b>В РАБОТЕ</b>\n{SEP}\n"
        f"📱 <code>{row['phone']}</code>\n"
        f"💰 {row['tariff_price']}\n"
        f"{SEP}\n{cmd_hint}", 
        reply_markup=kb.as_markup(), 
        parse_mode="HTML"
    )

    # Уведомление юзеру
    try:
        user_msg = f"⚡ Ваш номер <b>{mask_phone(row['phone'], 0)}</b> взят в работу!"
        if "MAX" not in tariff_name.upper():
            user_msg += "\nОжидайте код или QR."
        await bot.send_message(row['user_id'], user_msg, parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def cmd_code_request(m: Message, bot: Bot):
    """Команда для тарифа MAX: запрашивает код у юзера"""
    args = m.text.split()
    if len(args) < 2:
        return await m.reply("⚠️ Формат: `/code +7...`")
    
    ph = clean_phone(args[1])
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status='work'", (ph,))).fetchone()
        
        if not row:
            return await m.reply("❌ Номер не найден или не в работе.")
        if row['worker_id'] != m.from_user.id:
            return await m.reply("🚫 Это не ваш номер.")
        
        # Меняем статус на 'active' (ждем код)
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (row['id'],))
        await db.commit()
    
    try:
        await bot.send_message(
            row['user_id'], 
            f"🔔 <b>Офис запросил код!</b>\nДля номера: {mask_phone(ph, 0)}\n\n👇 <b>Напишите код ответом на это сообщение:</b>", 
            parse_mode="HTML"
        )
        await m.answer("✅ Запрос кода отправлен пользователю.")
    except Exception as e:
        await m.reply(f"❌ Ошибка доставки: {e}")

# ==========================================
# 10. ФОТО-МОСТ (BRIDGE)
# ==========================================
@router.message(F.photo)
async def handle_photo(m: Message, bot: Bot):
    # А. Юзер шлет фото боту (например скрин ошибки или QR с экрана другого устройства)
    if m.chat.type == "private":
        async with get_db() as db:
            row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active') LIMIT 1", (m.from_user.id,))).fetchone()
        
        if row and row['worker_chat_id']:
            caption = f"📩 <b>ФОТО от юзера</b> {mask_phone(row['phone'], 0)}"
            if m.caption: caption += f"\n{m.caption}"
            try:
                await bot.send_photo(row['worker_chat_id'], m.photo[-1].file_id, caption=caption, message_thread_id=row['worker_thread_id'], parse_mode="HTML")
                await m.answer("✅ Фото передано воркеру.")
            except:
                await m.answer("❌ Ошибка передачи.")
        return

    # Б. Воркер шлет фото в топик (QR код для юзера)
    if not m.caption: return 
    
    # Проверяем наличие команд
    if "/sms" in m.caption or "/code" in m.caption:
        parts = m.caption.split(maxsplit=2) # /sms phone text
        if len(parts) < 2: return 
        
        ph = clean_phone(parts[1])
        async with get_db() as db:
            row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
            
        if not row: return await m.reply("❌ Номер не найден.")
        if row['worker_id'] != m.from_user.id: return await m.reply("🚫 Не твой номер!")
        
        txt = parts[2] if len(parts) > 2 else "Вам пришло фото."
        try:
            await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"📩 <b>Сообщение от сервиса:</b>\n{txt}", parse_mode="HTML")
            await m.react([ReactionTypeEmoji(emoji="🔥")])
        except:
            await m.reply("❌ Не удалось доставить юзеру.")

# ==========================================
# 11. ЛОГИКА КНОПОК ВОРКЕРА
# ==========================================
async def check_worker(c: CallbackQuery, nid: int):
    """Проверка, что кнопку жмет тот, кто взял номер"""
    async with get_db() as db:
        row = await (await db.execute("SELECT worker_id FROM numbers WHERE id=?", (nid,))).fetchone()
    if not row: return False
    if row['worker_id'] != c.from_user.id:
        await c.answer("🚫 Не твой номер!", show_alert=True)
        return False
    return True

@router.callback_query(F.data.startswith("w_"))
async def cb_worker_action(c: CallbackQuery, bot: Bot):
    action, nid = c.data.split("_")[1], int(c.data.split("_")[2])
    
    # Защита от чужих нажатий
    if not await check_worker(c, nid): return

    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: 
            await c.answer("Номер уже не актуален")
            return

        new_status = ""
        log_msg = ""
        user_msg = ""
        
        # --- ЛОГИКА СТАТУСОВ ---
        if action == "suc": # Встал
            new_status = "finished"
            log_msg = f"✅ <b>НОМЕР ВСТАЛ</b>"
            user_msg = "✅ <b>Номер успешно принят!</b>\nОплата зачислена."
        
        elif action == "drop": # Слет (для WA)
            new_status = "dead"
            dur = calc_duration(row['start_time'], get_now())
            log_msg = f"📉 <b>СЛЕТ</b> | Время: {dur}"
            user_msg = f"📉 <b>Номер слетел.</b>\nВремя работы: {dur}"
            
        elif action == "skip": # Пропуск (для MAX)
            new_status = "dead"
            log_msg = "⏭ <b>ПРОПУСК</b>"
            user_msg = "⚠️ <b>Офис пропустил ваш номер.</b>"
            
        elif action == "err": # Ошибка
            new_status = "dead"
            log_msg = "❌ <b>ОШИБКА</b>"
            user_msg = "❌ <b>Отмена заявки.</b>"

        # Обновляем БД
        if new_status:
            await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (new_status, get_now(), nid))
            await db.commit()
            
            # Меняем сообщение воркера
            await c.message.edit_text(
                f"{log_msg}\n📱 <code>{row['phone']}</code>\n💰 {row['tariff_price']}",
                parse_mode="HTML", reply_markup=None
            )
            
            # Шлем юзеру
            try:
                await bot.send_message(row['user_id'], user_msg, parse_mode="HTML")
            except: pass
            
    await c.answer()

# ==========================================
# 12. АДМИН ПАНЕЛЬ И ОТЧЕТЫ
# ==========================================
@router.callback_query(F.data == "admin_main")
async def cb_admin_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("⚙️ <b>Админ панель</b>", reply_markup=admin_kb(), parse_mode="HTML")
    await c.answer()

# --- ОТЧЕТЫ ---
@router.callback_query(F.data == "adm_reports")
async def cb_adm_reports(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for h in [24, 48, 72, 120]:
        kb.button(text=f"🕒 {h} часов", callback_data=f"get_rep_{h}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(2, 2, 1)
    await c.message.edit_text("📄 Выберите период отчета:", reply_markup=kb.as_markup())
    await c.answer()

@router.callback_query(F.data.startswith("get_rep_"))
async def cb_get_report(c: CallbackQuery):
    hours = int(c.data.split("_")[2])
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT id, phone, tariff_name, status, created_at 
            FROM numbers WHERE created_at >= ? ORDER BY id DESC
        """, (cutoff,))).fetchall()
        
    if not rows:
        await c.answer("Нет данных за этот период", show_alert=True)
        return

    # Генерация CSV в памяти
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Phone", "Tariff", "Status", "Date (MSK)"])
    for r in rows:
        writer.writerow([r['id'], r['phone'], r['tariff_name'], r['status'], format_report_dt(r['created_at'])])
    
    output.seek(0)
    # Отправка как файл
    doc = BufferedInputFile(output.getvalue().encode(), filename=f"report_{hours}h.csv")
    await c.message.answer_document(doc, caption=f"📊 Отчет за последние {hours}ч")
    await c.answer()

# --- ТАРИФЫ ---
@router.callback_query(F.data == "adm_tariffs")
async def cb_adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    
    kb = InlineKeyboardBuilder()
    for t in rows:
        kb.button(text=f"✏️ {t['name']}", callback_data=f"edit_trf_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("💰 Управление тарифами (Пока только просмотр):", reply_markup=kb.as_markup())
    await c.answer()

# --- ОТВЕТ ПОДДЕРЖКИ (СТАРТ) ---
@router.callback_query(F.data.startswith("adm_reply_"))
async def cb_adm_reply(c: CallbackQuery, state: FSMContext):
    uid = int(c.data.split("_")[2])
    await state.set_state(AdminState.replying_to)
    await state.update_data(target_id=uid)
    await c.message.answer(f"✍️ Введите ответ для пользователя {uid}:")
    await c.answer()

# --- АППРУВ ЮЗЕРОВ ---
@router.callback_query(F.data.startswith("acc_"))
async def cb_acc_user(c: CallbackQuery, bot: Bot):
    action, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        if action == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            msg = "✅ Доступ разрешен! Нажмите /start"
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            msg = "🚫 Вам отказано в доступе."
        await db.commit()
    
    await c.message.edit_text(f"Обработано: {action} для {uid}")
    try: await bot.send_message(uid, msg)
    except: pass
    await c.answer()

# ==========================================
# 13. МОНИТОРИНГ И AFK СИСТЕМА (FIXED)
# ==========================================
@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk_confirm(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    if c.from_user.id != uid: return await c.answer("Не для тебя!")
    
    async with get_db() as db:
        # Просто обновляем таймер на текущее время (убираем PENDING)
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
        await db.commit()
        
    try: await c.message.delete()
    except: pass
    await c.answer("✅ Спасибо!")

async def monitor(bot: Bot):
    logger.info("👀 Мониторинг запущен")
    while True:
        try:
            await asyncio.sleep(60) # Проверка раз в минуту
            now = datetime.now(timezone.utc)
            
            async with get_db() as db:
                # Берем только юзеров, у которых ЕСТЬ номера в очереди
                users = await (await db.execute("""
                    SELECT u.user_id, u.last_afk_check 
                    FROM users u 
                    JOIN numbers n ON u.user_id = n.user_id 
                    WHERE n.status='queue'
                    GROUP BY u.user_id
                """)).fetchall()
                
                for u in users:
                    uid = u['user_id']
                    l_check = u['last_afk_check']
                    
                    if not l_check:
                        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
                        continue
                        
                    # СЦЕНАРИЙ 1: Юзер уже помечен как PENDING (ему отправили кнопку)
                    if "PENDING" in l_check:
                        try:
                            p_time = datetime.fromisoformat(l_check.split("_")[1])
                            # Если прошло больше 3 мин с момента вопроса -> КИК
                            if (now - p_time).total_seconds() / 60 >= AFK_KICK_MINUTES:
                                await db.execute("DELETE FROM numbers WHERE user_id=? AND status='queue'", (uid,))
                                await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
                                await db.commit()
                                try: await bot.send_message(uid, "💤 Вы исключены из очереди за неактивность.")
                                except: pass
                        except Exception as e:
                            logger.error(f"AFK pending error: {e}")
                            
                    # СЦЕНАРИЙ 2: Прошло много времени, надо спросить "Ты тут?"
                    else:
                        last_active = datetime.fromisoformat(l_check)
                        if (now - last_active).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            # Шлем кнопку
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👋 Я тут!", callback_data=f"afk_ok_{uid}")]])
                            try:
                                await bot.send_message(uid, "❓ <b>Вы тут?</b>\nПодтвердите активность, или заявка удалится.", reply_markup=kb, parse_mode="HTML")
                                # Сразу ставим PENDING, чтобы не слать повторно
                                await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (f"PENDING_{get_now()}", uid))
                                await db.commit()
                            except TelegramForbiddenError:
                                # Юзер заблочил бота - удаляем все сразу
                                await db.execute("DELETE FROM numbers WHERE user_id=?", (uid,))
                                await db.commit()
                            except Exception as e:
                                logger.error(f"AFK send error: {e}")

        except Exception as e:
            logger.exception(f"Global Monitor Error: {e}")
            await asyncio.sleep(5)

# ==========================================
# 14. ЗАПУСК
# ==========================================
async def main():
    await init_db()
    
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    # Удаляем вебхуки, чтобы бот не получал старые апдейты
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запускаем фоновую задачу
    asyncio.create_task(monitor(bot))
    
    logger.info("🚀 BOT v21.0 STARTED (FIXED)")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
