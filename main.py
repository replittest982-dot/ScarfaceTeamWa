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
DB_NAME = "bot_v84_groups.db" 

# Таймеры (в минутах)
AFK_CHECK_MINUTES = 8      # Проверка AFK
AFK_KICK_MINUTES = 3       # Кик после проверки
CODE_WAIT_MINUTES = 4      # Ожидание кода

# Дизайн
SEP = "━━━━━━━━━━━━━━"

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
        
        # Номера
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
        
        # Тарифы - ОБНОВЛЕННАЯ СТРУКТУРА
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            name TEXT PRIMARY KEY, 
            price TEXT, 
            hold_time TEXT DEFAULT '20 мин')""")
        
        # Группы (Офисы)
        await db.execute("""CREATE TABLE IF NOT EXISTS groups (
            group_num INTEGER PRIMARY KEY,
            chat_id INTEGER,
            title TEXT
        )""")
        
        # Конфиг (привязка топиков к тарифам)
        await db.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
        
        # Дефолтные тарифы
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '20 мин')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '1 час')")
        
        await db.commit()
    logger.info("✅ DB Loaded v84.1 (FIXED)")

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
# 4. FSM И КЛАВИАТУРЫ
# ==========================================
class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_support = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_hold = State()
    edit_price = State()
    support_reply = State()

def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="ℹ️ Помощь", callback_data="guide")
    kb.button(text="🆘 Поддержка", callback_data="ask_supp")
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
                        f"👤 <b>Заявка:</b>\nID: <code>{uid}</code>\n@{m.from_user.username}", 
                        reply_markup=kb, parse_mode="HTML")
                except: pass
            return await m.answer(f"🔒 <b>Доступ ограничен.</b>\n{SEP}\nОжидайте одобрения.", parse_mode="HTML")
        
        if res['is_banned']: 
            return await m.answer(f"🚫 <b>Вы заблокированы.</b>", parse_mode="HTML")
        if res['is_approved']: 
            await m.answer(f"👋 <b>Привет, {m.from_user.first_name}!</b>\n{SEP}", reply_markup=main_kb(uid), parse_mode="HTML")
        else: 
            await m.answer(f"⏳ <b>Заявка на рассмотрении.</b>", parse_mode="HTML")

@router.callback_query(F.data == "guide")
async def show_guide(c: CallbackQuery):
    txt = (f"ℹ️ <b>FAQ / Информация</b>\n{SEP}\n"
           "📲 <b>Что делает бот?</b>\n"
           "Принимаем номера WhatsApp / MAX. Выплаты после проверки.\n\n"
           "📦 <b>Требования:</b>\n"
           "• Чистый, активный номер\n"
           "• Доступ к приему SMS\n"
           "• Виртуальные номера запрещены ❌\n\n"
           "⏳ <b>Холд и Выплаты:</b>\n"
           "Деньги начисляются после завершения холда.\n\n"
           f"{SEP}")
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
        
    txt = (f"👤 <b>Личный кабинет</b>\n{SEP}\n"
           f"🆔 ID: <code>{uid}</code>\n"
           f"📦 Всего сдано: <b>{total}</b>\n"
           f"🔥 В работе: <b>{active}</b>\n"
           f"{SEP}\n"
           f"🕒 <b>Очередь:</b> {q_pos}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 История номеров", callback_data="my_nums")
    kb.button(text="🔙 Меню", callback_data="back_main")
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
    txt = f"📝 <b>Последние 10 номеров:</b>\n{SEP}\n"
    if not rows: txt += "📭 История пуста."
    
    for r in rows:
        icon = "🟡" if r['status']=='queue' else "🟢" if r['status']=='active' else "✅" if r['status']=='finished' else "❌"
        txt += f"{icon} <code>{mask_phone(r['phone'], uid)}</code> | {r['tariff_price']}\n"
        if r['status'] == 'queue':
             kb.button(text=f"🗑 Удалить {mask_phone(r['phone'], uid)}", callback_data=f"del_{r['id']}")

    kb.button(text="🔙 Назад", callback_data="profile")
    kb.adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("del_"))
async def delete_num(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        row = await (await db.execute("SELECT status FROM numbers WHERE id=? AND user_id=?", (nid, c.from_user.id))).fetchone()
        if row and row['status'] == 'queue':
            await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
            await db.commit()
            await c.answer("✅ Номер удален")
            await my_nums(c)
        else:
            await c.answer("❌ Номер уже в работе!", show_alert=True)

# --- СДАЧА НОМЕРА ---
@router.callback_query(F.data == "sel_tariff")
async def sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()

    if not rows:
        await c.message.edit_text("❌ Тарифы не настроены!", reply_markup=main_kb(c.from_user.id), parse_mode="HTML")
        return

    kb = InlineKeyboardBuilder()
    for r in rows: 
        hold_time = r['hold_time'] or '-' 
        kb.button(text=f"{r['name']} | {r['price']} (Hold: {hold_time})", callback_data=f"pick_{r['name']}")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(f"📂 Выберите тариф:\n{SEP}", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def pick_t(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split("_")[1]
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM tariffs WHERE name=?", (t_name,))).fetchone()
    
    await state.update_data(tariff=t_name, price=res['price'])
    
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text(
        f"💎 Тариф: <b>{t_name}</b>\n"
        f"💰 Прайс: <b>{res['price']}</b>\n"
        f"⏳ Холд: <b>{res['hold_time']}</b>\n{SEP}\n"
        f"📱 <b>Отправьте номера списком или по одному (+7...):</b>", 
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
        return await m.reply("❌ <b>Не найдено валидных номеров.</b>", parse_mode="HTML")
    
    async with get_db() as db:
        for ph in valid:
            await db.execute(
                "INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, last_ping) VALUES (?, ?, ?, ?, ?)",
                (m.from_user.id, ph, data['tariff'], data['price'], get_now())
            )
        await db.commit()
    
    await state.clear()
    await m.answer(f"✅ <b>Принято: {len(valid)} шт.</b>\n{SEP}\nНомера добавлены в очередь.", reply_markup=main_kb(m.from_user.id), parse_mode="HTML")

# ==========================================
# 6. ВОРКЕР: СИСТЕМА + ГРУППЫ
# ==========================================
@router.message(Command("bindgroup"))
async def cmd_bindgroup(m: Message, command: CommandObject):
    """Привязка текущего чата к номеру группы (1, 2, 3)"""
    if m.from_user.id != ADMIN_ID: return
    
    if not command.args:
        return await m.reply("❌ <b>Ошибка!</b>\nИспользуйте: <code>/bindgroup 1</code> (или 2, 3)", parse_mode="HTML")
    
    try:
        group_num = int(command.args.strip())
        if group_num not in [1, 2, 3]: raise ValueError
    except:
        return await m.reply("❌ Номер группы должен быть 1, 2 или 3.")

    chat_id = m.chat.id
    title = m.chat.title or f"Chat {chat_id}"

    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO groups (group_num, chat_id, title) VALUES (?, ?, ?)", 
                         (group_num, chat_id, title))
        await db.commit()
    
    await m.answer(f"✅ <b>Группа {group_num} привязана!</b>\n{SEP}\n📍 Чат: {title}\n🆔 ID: {chat_id}", parse_mode="HTML")

@router.message(Command("startwork"))
async def sys_start(m: Message):
    if m.from_user.id != ADMIN_ID: return 
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    await m.answer(f"⚙️ <b>Настройка воркера</b>\n{SEP}\nВыберите тариф для привязки:", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("bind_"))
async def sys_bind(c: CallbackQuery):
    t = c.data.split("_")[1]
    cid = c.message.chat.id
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"topic_{cid}_{tid}", t))
        await db.commit()
    await c.message.edit_text(f"✅ <b>Топик привязан!</b> Тариф: {t}\n{SEP}\nПиши /num чтобы взять номер.", parse_mode="HTML")

@router.message(Command("stopwork"))
async def sys_stop(m: Message, bot: Bot):
    if m.from_user.id != ADMIN_ID: return
    
    chat_id = m.chat.id
    
    async with get_db() as db:
        group = await (await db.execute("SELECT * FROM groups WHERE chat_id=?", (chat_id,))).fetchone()
        
        if group:
            group_num = group['group_num']
            title = group['title']
            stop_time_utc = get_now()
            stop_time_msk = format_time(stop_time_utc)
            
            nums = await (await db.execute("""
                SELECT id, user_id, phone, start_time FROM numbers 
                WHERE status IN ('work', 'active') AND worker_chat_id=?
            """, (chat_id,))).fetchall()
            
            stopped_count = 0
            for row in nums:
                await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", 
                                 (f"finished_group_{group_num}", stop_time_utc, row['id']))
                stopped_count += 1
                
                duration = calc_duration(row['start_time'], stop_time_utc)
                try:
                    await bot.send_message(
                        row['user_id'], 
                        f"🛑 <b>{title} остановлен!</b>\n{SEP}\n"
                        f"📱 {mask_phone(row['phone'], row['user_id'])}\n"
                        f"⏰ Стоп: {stop_time_msk}\n"
                        f"⏱ Работа: {duration}", 
                        parse_mode="HTML"
                    )
                except: pass
            
            await db.commit()
            await m.answer(f"🛑 <b>СТОП ВОРК!</b>\n{SEP}\n🏢 Офис: {title}\n📦 Остановили номеров: {stopped_count}", parse_mode="HTML")
            
        else:
            cid, tid = m.chat.id, (m.message_thread_id if m.is_topic_message else 0)
            await db.execute("DELETE FROM config WHERE key=?", (f"topic_{cid}_{tid}",))
            await db.commit()
            await m.reply("🛑 <b>Топик отключен (конфиг удален).</b>", parse_mode="HTML")

@router.message(Command("num"))
async def worker_get_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply(f"❌ Топик не настроен.")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 <b>Очередь пуста.</b>", parse_mode="HTML")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?", 
            (m.from_user.id, cid, tid, get_now(), row['id']))
        await db.commit()
    
    await m.answer(f"🚀 <b>В работе:</b>\n{SEP}\n📱 <code>{row['phone']}</code>\n💎 {row['tariff_name']}", reply_markup=worker_kb(row['id'], row['tariff_name']), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"⚡ <b>Ваш номер {mask_phone(row['phone'], row['user_id'])} взят в работу!</b>\n{SEP}\nОжидайте QR или SMS код.", parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def worker_code_req(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Пример: <code>/code +7999...</code>", parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер.")
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?", (get_now(), row['id']))
        await db.commit()

    try:
        await bot.send_message(row['user_id'], f"🔔 <b>ЗАПРОС КОДА</b>\n{SEP}\n📱 Номер: <code>{mask_phone(row['phone'], row['user_id'])}</code>\n👇 <b>Напишите код в чат.</b>", parse_mode="HTML")
        await m.reply(f"✅ <b>Запрос отправлен.</b>", parse_mode="HTML")
    except: await m.reply("❌ Ошибка доставки.")

@router.message(F.photo & F.caption)
async def worker_photo_sms(m: Message, bot: Bot):
    if "/sms" not in m.caption.lower(): return
    ph = clean_phone(m.caption.split()[1]) if len(m.caption.split()) > 1 else None
    if not ph: return await m.reply("⚠️ Формат: /sms +7... текст")
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
    if not row or row['worker_id'] != m.from_user.id: return await m.reply("❌ Не ваш номер.")
    
    try:
        await bot.send_photo(row['user_id'], m.photo[-1].file_id, caption=f"🔔 <b>СООБЩЕНИЕ ОТ ОФИСА</b>\n{SEP}", parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="🔥")])
    except: await m.reply("❌ Не доставлено.")

# --- ОТВЕТ ЮЗЕРА ---
@router.message(F.chat.type == "private")
async def user_any_msg(m: Message, bot: Bot, state: FSMContext):
    if m.text and m.text.startswith('/'): return
    if m.from_user.id == ADMIN_ID: return
    
    # ПРОВЕРЯЕМ СОСТОЯНИЕ FSM
    current_state = await state.get_state()
    if current_state: return  # Если юзер в состоянии - не обрабатываем
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')", (m.from_user.id,))).fetchone()
    
    if row and row['worker_chat_id']:
        async with get_db() as db:
            await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?", (row['id'],))
            await db.commit()
        try:
            target_chat = row['worker_chat_id']
            target_thread = row['worker_thread_id'] if row['worker_thread_id'] else None
            header = f"📩 <b>ОТВЕТ ЮЗЕРА</b>\n📱 <code>{row['phone']}</code>\n{SEP}\n"
            
            if m.text: await bot.send_message(target_chat, message_thread_id=target_thread, text=f"{header}💬 {m.text}", parse_mode="HTML")
            elif m.photo: await bot.send_photo(target_chat, message_thread_id=target_thread, photo=m.photo[-1].file_id, caption=f"{header}📸 [Фото]", parse_mode="HTML")
            await m.answer("✅ <b>Отправлено.</b>", parse_mode="HTML")
        except: await m.answer("❌ Ошибка отправки.")

@router.callback_query(F.data.startswith("w_act_"))
async def w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("🚫 Не ты брал!", show_alert=True)
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        await db.commit()
    
    await c.message.edit_text(f"✅ <b>Встал:</b> {row['phone']}", reply_markup=worker_active_kb(nid), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"✅ <b>Номер успешно активирован!</b>\n{SEP}\nОжидайте выплату.", parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def w_skip(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("🚫 Не ты брал!", show_alert=True)
        await db.execute("UPDATE numbers SET status='queue', worker_id=0, worker_chat_id=0 WHERE id=?", (nid,))
        await db.commit()
    await c.message.edit_text("⏭ <b>Пропуск</b> (вернулся в очередь)", parse_mode="HTML")

@router.callback_query(F.data.startswith(("w_drop_", "w_err_")))
async def w_finish(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row or row['worker_id'] != c.from_user.id: return await c.answer("🚫 Не ты брал!", show_alert=True)
        status = "finished" if is_drop else "dead"
        dur = calc_duration(row['start_time'], get_now())
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (status, get_now(), nid))
        await db.commit()
    
    msg = f"📉 <b>Слет.</b> Работа: {dur}" if is_drop else "❌ <b>Ошибка/Отмена.</b>"
    await c.message.edit_text(msg, parse_mode="HTML")
    try: await bot.send_message(row['user_id'], msg, parse_mode="HTML")
    except: pass

# ==========================================
# 7. АДМИНКА
# ==========================================
@router.callback_query(F.data == "admin_main")
async def adm_main(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы", callback_data="adm_tariffs")
    kb.button(text="📄 Отчеты", callback_data="adm_reports")
    kb.button(text="📢 Рассылка", callback_data="adm_cast")
    kb.button(text="🏢 Группы", callback_data="manage_groups")
    kb.button(text="🔙 Меню", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(f"⚡ <b>Админ панель</b>\n{SEP}", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "manage_groups")
async def manage_groups(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    
    async with get_db() as db:
        groups = await (await db.execute("SELECT * FROM groups ORDER BY group_num")).fetchall()
    
    kb = InlineKeyboardBuilder()
    
    for i in range(1, 4):
        g_name = "Нет данных"
        for g in groups:
            if g['group_num'] == i:
                g_name = g['title']
                break
        
        kb.button(text=f"🛑 Стоп: {g_name}", callback_data=f"stop_group_{i}")
    
    kb.button(text="📋 Статус групп", callback_data="groups_status")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("🏢 <b>Управление группами</b>\n{SEP}\nВыберите группу для остановки:", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("stop_group_"))
async def stop_group(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    group_num = int(c.data.split("_")[-1])
    
    stop_time_utc = get_now()
    stop_time_msk = format_time(stop_time_utc)
    
    async with get_db() as db:
        group = await (await db.execute("SELECT * FROM groups WHERE group_num=?", (group_num,))).fetchone()
        
        if not group:
            return await c.answer(f"❌ Группа {group_num} не привязана!", show_alert=True)
        
        chat_id = group['chat_id']
        title = group['title']
        
        nums = await (await db.execute("""
            SELECT id, user_id, phone, start_time 
            FROM numbers 
            WHERE status IN ('work','active') AND worker_chat_id=?
        """, (chat_id,))).fetchall()
        
        stopped = 0
        for num in nums:
            await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", 
                             (f"finished_group_{group_num}", stop_time_utc, num['id']))
            stopped += 1
            
            duration = calc_duration(num['start_time'], stop_time_utc)
            try:
                await bot.send_message(
                    num['user_id'], 
                    f"🛑 <b>{title} остановлен!</b>\n"
                    f"📱 {mask_phone(num['phone'], num['user_id'])}\n"
                    f"⏰ Стоп: {stop_time_msk}\n"
                    f"⏱ Работа: {duration}",
                    parse_mode="HTML"
                )
            except: pass
        
        await db.commit()
    
    await c.message.edit_text(
        f"🛑 <b>Группа {group_num} остановлена!</b>\n"
        f"🏢 {title}\n"
        f"⏰ {stop_time_msk}\n"
        f"📦 Обработано: {stopped} номеров",
        parse_mode="HTML"
    )

@router.callback_query(F.data == "groups_status")
async def groups_status(c: CallbackQuery):
    async with get_db() as db:
        stats = {}
        for i in range(1, 4):
            count = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status=?", (f"finished_group_{i}",))).fetchone())[0]
            stats[f"Group {i}"] = count
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status IN ('work','active')")).fetchone())[0]
        queue = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue'")).fetchone())[0]
    
    txt = f"📊 <b>СТАТУС</b>\n{SEP}\n"
    for g, cnt in stats.items():
        txt += f"🏁 {g}: {cnt} финиш\n"
    txt += f"\n🔥 Актив: {active}\n🟡 Очередь: {queue}"
    
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="manage_groups").adjust(1)
    await c.message.edit_text(txt, reply_markup=kb.as_markup(), parse_mode="HTML")

# --- ТАРИФЫ (ИСПРАВЛЕНО) ---
@router.callback_query(F.data == "adm_tariffs")
async def adm_tariffs(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}", callback_data=f"ed_{t['name']}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("🛠 <b>Выберите тариф:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("ed_"))
async def ed_t1(c: CallbackQuery, state: FSMContext):
    target = c.data.split("_")[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_price)
    await c.message.edit_text(f"1️⃣ Введите <b>ЦЕНУ</b> для {target}:\n(Пример: 50₽, 10$, 2$)", parse_mode="HTML")

@router.message(AdminState.edit_price)
async def ed_t2(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_hold)
    await m.answer(f"2️⃣ Введите <b>ВРЕМЯ ХОЛДА</b>:\n(Пример: 20 мин, 30мин, 1 час)", parse_mode="HTML")

@router.message(AdminState.edit_hold)
async def ed_t_fin(m: Message, state: FSMContext):
    d = await state.get_data()
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?, hold_time=? WHERE name=?", (d['price'], m.text, d['target']))
        await db.commit()
    await state.clear()
    await m.answer(f"✅ <b>Тариф обновлен!</b>\n{SEP}\n💰 {d['price']}\n⏳ {m.text}", parse_mode="HTML")

# --- ОТЧЕТЫ ---
@router.callback_query(F.data == "adm_reports")
async def adm_reports(c: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for h in [1, 24, 48]: kb.button(text=f"За {h}ч", callback_data=f"rep_{h}")
    kb.button(text="🔙 Назад", callback_data="admin_main")
    kb.adjust(3, 1)
    await c.message.edit_text("📅 <b>Выберите период:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("rep_"))
async def adm_get_rep(c: CallbackQuery):
    h = int(c.data.split("_")[1])
    cut_time = (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat()
    
    async with get_db() as db:
        rows = await (await db.execute("""
            SELECT n.*, g.title as group_name 
            FROM numbers n 
            LEFT JOIN groups g ON n.worker_chat_id = g.chat_id
            WHERE n.created_at >= ? 
            ORDER BY n.id DESC
        """, (cut_time,))).fetchall()
    
    if not rows: return await c.answer("📂 Пусто.")
    
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID', 'User ID', 'Phone', 'Status', 'Group', 'Tariff', 'Created', 'Start', 'End', 'Duration'])
    
    for r in rows:
        dur = calc_duration(r['start_time'], r['end_time'])
        group_name = r['group_name'] if r['group_name'] else "-"
        w.writerow([
            r['id'], r['user_id'], r['phone'], r['status'], group_name,
            r['tariff_name'], format_time(r['created_at']), 
            format_time(r['start_time']), format_time(r['end_time']), dur
        ])
        
    out.seek(0)
    file_data = BufferedInputFile(out.getvalue().encode(), filename=f"report_{h}h.csv")
    await c.message.answer_document(file_data, caption=f"📊 Отчет за {h}ч")

# --- ПОДДЕРЖКА (ИСПРАВЛЕНО) ---
@router.callback_query(F.data == "ask_supp")
async def ask_supp(c: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data="back_main")
    await c.message.edit_text(f"🆘 <b>Техническая поддержка</b>\n{SEP}\nНапишите ваш вопрос одним сообщением:", reply_markup=kb.as_markup(), parse_mode="HTML")
    await state.set_state(UserState.waiting_support)

@router.message(UserState.waiting_support)
async def send_supp(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    
    kb = InlineKeyboardBuilder().button(text="💬 Ответить", callback_data=f"reply_{m.from_user.id}")
    try:
        await bot.send_message(ADMIN_ID, 
            f"🆘 <b>Вопрос от {m.from_user.id}</b> (@{m.from_user.username})\n{SEP}\n{m.text}", 
            reply_markup=kb.as_markup(), parse_mode="HTML")
        await m.answer(f"✅ <b>Отправлено.</b>\nАдминистратор ответит вам.", 
                       reply_markup=main_kb(m.from_user.id), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Supp Error: {e}")
        await m.answer("❌ Ошибка отправки.")

@router.callback_query(F.data.startswith("reply_"))
async def adm_reply(c: CallbackQuery, state: FSMContext):
    uid = c.data.split("_")[1]
    await state.update_data(ruid=uid)
    await state.set_state(AdminState.support_reply)
    await c.message.answer(f"✍️ <b>Введите ответ для {uid}:</b>", parse_mode="HTML")

@router.message(AdminState.support_reply)
async def send_reply(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    try:
        await bot.send_message(d['ruid'], f"👨‍💻 <b>Ответ поддержки:</b>\n{SEP}\n{m.text}", parse_mode="HTML")
        await m.answer("✅ Ответ доставлен.")
    except: await m.answer("❌ Не доставлено.")
    await state.clear()

@router.callback_query(F.data == "back_main")
async def back_to_main(c: CallbackQuery, state: FSMContext):
    await state.clear()  # ЧИСТИМ СОСТОЯНИЕ
    await c.message.edit_text(f"👋 <b>Главное меню</b>\n{SEP}", reply_markup=main_kb(c.from_user.id), parse_mode="HTML")

@router.callback_query(F.data.startswith("acc_"))
async def acc_dec(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        if act == "ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,))
            await db.commit()
            await c.message.edit_text(f"✅ Юзер {uid} принят.")
            try: await bot.send_message(uid, f"✅ <b>Доступ открыт!</b>\nЖмите /start", parse_mode="HTML")
            except: pass
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            await db.commit()
            await c.message.edit_text(f"🚫 Юзер {uid} забанен.")

# --- РАССЫЛКА (ИСПРАВЛЕНО) ---
@router.callback_query(F.data == "adm_cast")
async def adm_cast(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 <b>Пришлите пост для рассылки:</b>", parse_mode="HTML")

@router.message(AdminState.waiting_broadcast)
async def proc_cast(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    
    msg = await m.answer("⏳ <b>Рассылка запущена...</b>", parse_mode="HTML")
    
    async with get_db() as db:
        users = await (await db.execute("SELECT user_id FROM users WHERE is_approved=1")).fetchall()
    
    success, fail = 0, 0
    for u in users:
        try:
            await m.copy_to(u['user_id'])
            success += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            fail += 1
        except Exception:
            fail += 1
            
    await msg.edit_text(
        f"📢 <b>Рассылка завершена!</b>\n{SEP}\n"
        f"✅ Доставлено: <b>{success}</b>\n"
        f"❌ Ошибок: <b>{fail}</b>\n"
        f"📊 Всего: <b>{len(users)}</b>",
        parse_mode="HTML"
    )

# ==========================================
# 9. МОНИТОРИНГ
# ==========================================
async def global_monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60) 
            now = datetime.now(timezone.utc)
            async with get_db() as db:
                # 1. Таймаут кода
                waiters = await (await db.execute("SELECT id, user_id, phone, worker_chat_id, worker_thread_id, wait_code_start FROM numbers WHERE status='active' AND wait_code_start IS NOT NULL")).fetchall()
                for w in waiters:
                    start_time = datetime.fromisoformat(w['wait_code_start'])
                    if (now - start_time).total_seconds() / 60 >= CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead', end_time=?, wait_code_start=NULL WHERE id=?", (get_now(), w['id']))
                        try:
                            await bot.send_message(w['user_id'], f"⏳ <b>Время вышло.</b> Номер {w['phone']} отменен.", parse_mode="HTML")
                            if w['worker_chat_id']:
                                await bot.send_message(chat_id=w['worker_chat_id'], message_thread_id=w['worker_thread_id'] if w['worker_thread_id'] else None, text="⚠️ <b>Таймаут кода!</b>", parse_mode="HTML")
                        except: pass

                # 2. AFK и Удаление
                queue_rows = await (await db.execute("SELECT id, user_id, created_at, last_ping FROM numbers WHERE status='queue'")).fetchall()
                for r in queue_rows:
                    last_act_str = r['last_ping'] if r['last_ping'] else r['created_at']
                    if str(last_act_str).startswith("PENDING_"):
                        ping_time = datetime.fromisoformat(last_act_str.split("_")[1])
                        if (now - ping_time).total_seconds() / 60 >= AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                            try: await bot.send_message(r['user_id'], f"❌ <b>Номер удален из очереди (AFK).</b>", parse_mode="HTML")
                            except: pass
                    else:
                        last_act = datetime.fromisoformat(last_act_str)
                        if (now - last_act).total_seconds() / 60 >= AFK_CHECK_MINUTES:
                            kb = InlineKeyboardBuilder().button(text="👋 Я тут!", callback_data=f"afk_ok_{r['id']}").as_markup()
                            try:
                                await bot.send_message(r['user_id'], f"⚠️ <b>Проверка активности!</b>\n{SEP}\nНажмите кнопку.", reply_markup=kb, parse_mode="HTML")
                                await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (f"PENDING_{get_now()}", r['id']))
                            except: await db.execute("DELETE FROM numbers WHERE id=?", (r['id'],))
                await db.commit()
        except: await asyncio.sleep(5)

@router.callback_query(F.data.startswith("afk_ok_"))
async def afk_confirm(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?", (get_now(), nid))
        await db.commit()
    await c.message.delete()
    await c.answer("✅ Вы в очереди!")

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
    logger.info("🚀 BOT v84.1 STARTED (FIXED: SUPPORT + BROADCAST + TARIFFS)")
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
