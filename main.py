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
    from aiogram.filters import Command, CommandStart
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
    sys.exit("❌ Ошибка: Выполни pip install aiogram aiosqlite")

# ==========================================
# 1. КОНФИГУРАЦИЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_NAME = "fast_team_v27.db"

# Таймеры
AFK_CHECK_MINUTES = 8  # Через сколько спросить "Ты тут?"
AFK_KICK_MINUTES = 3   # Сколько ждать ответа перед киком
SEP = "━━━━━━━━━━━━━━━━━━━━"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

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
                tariff_name TEXT, tariff_price TEXT, status TEXT DEFAULT 'queue', 
                worker_id INTEGER DEFAULT 0, worker_chat_id INTEGER DEFAULT 0, 
                worker_thread_id INTEGER DEFAULT 0, start_time TEXT, end_time TEXT, 
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (name TEXT PRIMARY KEY, price TEXT, work_time TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('WhatsApp','50₽','10:00-22:00 МСК')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES('MAX','10$','24/7')")
        await db.commit()
    logger.info("✅ Database initialized (v27.0 Ultimate)")

# ==========================================
# 3. УТИЛИТЫ
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

def get_now(): return datetime.now(timezone.utc).isoformat()

def format_report_dt(iso_str):
    try: return (datetime.fromisoformat(iso_str) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
    except: return iso_str

def calc_duration(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        mins = int((e - s).total_seconds() / 60)
        return f"{mins // 60}ч {mins % 60}мин" if mins >= 60 else f"{mins} мин"
    except: return "0 мин"

# FSM
class UserState(StatesGroup):
    waiting_number = State()
    waiting_question = State()

class AdminState(StatesGroup):
    replying_to = State()

# ==========================================
# 4. ЮЗЕР -> ОФИС (BRIDGE)
# ==========================================

# Единый обработчик для ТЕКСТА и ФОТО от юзера в ЛС
@router.message(F.chat.type == "private")
async def handle_user_message(m: Message, bot: Bot, state: FSMContext):
    user_id = m.from_user.id
    st = await state.get_state()
    
    # --- ВВОД НОМЕРА ---
    if st == UserState.waiting_number:
        if not m.text: return await m.reply("❌ Пришлите номер текстом.")
        clean = clean_phone(m.text)
        if not clean: return await m.reply("❌ Некорректный формат.")
        
        data = await state.get_data()
        tariff = data.get("tariff", "WhatsApp")
        
        async with get_db() as db:
            exists = await (await db.execute("SELECT id FROM numbers WHERE phone=? AND status IN ('queue','work','active')", (clean,))).fetchone()
            if exists: return await m.reply("❌ Этот номер уже в системе.")
            
            p_row = await (await db.execute("SELECT price FROM tariffs WHERE name=?", (tariff,))).fetchone()
            price = p_row['price'] if p_row else "0"
            
            # Вставляем номер и обновляем таймер активности
            await db.execute("INSERT INTO numbers (user_id, phone, tariff_name, tariff_price, created_at) VALUES (?, ?, ?, ?, ?)", 
                             (user_id, clean, tariff, price, get_now()))
            await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), user_id))
            await db.commit()
            
        await state.clear()
        await m.answer(f"✅ Номер {clean} добавлен в очередь!\nТариф: {tariff}")
        return

    # --- ПОДДЕРЖКА (ВОПРОС) ---
    if st == UserState.waiting_question:
        if not ADMIN_ID: return
        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✉️ Ответить", callback_data=f"adm_reply_{user_id}")]])
            await bot.send_message(ADMIN_ID, f"🆘 <b>Вопрос от {user_id}:</b>\n{m.text or '[Вложение]'}", reply_markup=kb, parse_mode="HTML")
            await m.answer("✅ Отправлено админу.")
        except: pass
        await state.clear()
        return

    # --- ПЕРЕСЫЛКА В ТОПИК ---
    async with get_db() as db:
        row = await (await db.execute("""
            SELECT * FROM numbers 
            WHERE user_id=? AND status IN ('work', 'active') 
            ORDER BY id DESC LIMIT 1
        """, (user_id,))).fetchone()
    
    if not row: return 
    if not row['worker_chat_id']: return await m.reply("⏳ Ожидайте специалиста.")

    topic_msg = f"📩 <b>ОТВЕТ ЮЗЕРА</b>\n📱 {mask_phone(row['phone'], 0)}\n{SEP}"
    if m.text: topic_msg += f"\n{m.text}"
    if m.caption: topic_msg += f"\n{m.caption}"

    try:
        thread_id = int(row['worker_thread_id']) if row['worker_thread_id'] else None
        
        if m.photo:
            await bot.send_photo(chat_id=row['worker_chat_id'], message_thread_id=thread_id, photo=m.photo[-1].file_id, caption=topic_msg, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=row['worker_chat_id'], message_thread_id=thread_id, text=topic_msg, parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji="⚡")])
    except TelegramBadRequest:
        await m.reply("❌ Ошибка связи с офисом.")
    except Exception as e:
        logger.error(f"Bridge Error: {e}")

# ==========================================
# 5. ОФИС -> ЮЗЕР (ФОТО/SMS)
# ==========================================
@router.message(F.photo & F.caption)
async def handle_worker_photo(m: Message, bot: Bot):
    if m.chat.type == "private": return 

    # Парсинг /sms +7... текст
    match = re.search(r'(/sms|/code)\s+([+\d]+)\s*(.*)', m.caption, flags=re.DOTALL)
    
    if match:
        raw_phone = match.group(2)
        text_for_user = match.group(3).strip()
        ph = clean_phone(raw_phone)
        
        if not ph: return await m.reply("❌ Кривой номер.")
        if not text_for_user: text_for_user = "Вам пришло фото от сервиса."

        async with get_db() as db:
            row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,))).fetchone()
        
        if not row: return await m.reply("❌ Номер не в работе.")
        if row['worker_id'] != m.from_user.id: return await m.reply("🚫 Не твой номер.")

        try:
            await bot.send_photo(chat_id=row['user_id'], photo=m.photo[-1].file_id, caption=f"📩 <b>Сообщение от сервиса:</b>\n{SEP}\n{text_for_user}", parse_mode="HTML")
            await m.react([ReactionTypeEmoji(emoji="👌")])
        except Exception as e:
            await m.reply(f"❌ Не доставлено: {e}")

# ==========================================
# 6. КОМАНДЫ ВОРКЕРА
# ==========================================
@router.message(Command("startwork"))
async def cmd_startwork(m: Message):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db: tariffs = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in tariffs: kb.button(text=t['name'], callback_data=f"bind_{t['name']}")
    kb.adjust(1)
    await m.answer("⚙️ Выбери тариф для этого топика:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    tn = c.data.split("_")[1]
    tid = c.message.message_thread_id if c.message.is_topic_message else 0
    key = f"topic_{c.message.chat.id}_{tid}"
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, tn))
        await db.commit()
    await c.message.edit_text(f"✅ Топик привязан к <b>{tn}</b> (Thread ID: {tid})", parse_mode="HTML")

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    tid = m.message_thread_id if m.is_topic_message else 0
    key = f"topic_{m.chat.id}_{tid}"
    
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?", (key,))).fetchone()
        if not conf: return await m.reply(f"❌ Топик не настроен (TID: {tid}). Юзай /startwork")
        
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1", (conf['value'],))).fetchone()
        if not row: return await m.reply("📭 Очередь пуста")
        
        await db.execute("""
            UPDATE numbers SET status='work', worker_id=?, worker_chat_id=?, worker_thread_id=?, start_time=? WHERE id=?
        """, (m.from_user.id, m.chat.id, tid, get_now(), row['id']))
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), row['user_id']))
        await db.commit()
    
    kb = InlineKeyboardBuilder()
    if conf['value'] == "MAX":
        kb.button(text="✅ Встал", callback_data=f"w_suc_{row['id']}")
        kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{row['id']}")
        hint = f"Запрос кода: `/code {row['phone']}`"
    else:
        kb.button(text="✅ Встал", callback_data=f"w_suc_{row['id']}")
        kb.button(text="📉 Слет", callback_data=f"w_drop_{row['id']}")
        hint = f"Фото юзеру: `/sms {row['phone']} Текст` (прикрепи фото)"
        
    # Добавил кнопку ошибки
    kb.button(text="❌ Ошибка", callback_data=f"w_err_{row['id']}")
    kb.adjust(2, 1)
    
    await m.answer(f"🚀 <b>В РАБОТЕ</b>\n📱 <code>{row['phone']}</code>\n💰 {row['tariff_price']}\n\n{hint}", reply_markup=kb.as_markup(), parse_mode="HTML")
    try: await bot.send_message(row['user_id'], f"⚡ Ваш номер <b>{mask_phone(row['phone'], 0)}</b> взят в работу!", parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def cmd_code(m: Message, bot: Bot):
    args = m.text.split()
    if len(args) < 2: return await m.reply("⚠️ Формат: `/code +7...`")
    ph = clean_phone(args[1])
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status='work'", (ph,))).fetchone()
        if not row: return await m.reply("❌ Номер не в работе.")
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (row['id'],))
        await db.commit()
    try:
        await bot.send_message(row['user_id'], f"🔔 <b>ОФИС ЗАПРОСИЛ КОД!</b>\nДля номера: {mask_phone(ph, 0)}\n\n👇 <b>Напишите код в ответ на это сообщение:</b>", parse_mode="HTML")
        await m.answer("✅ Запрос отправлен юзеру.")
    except: await m.reply("❌ Не удалось доставить запрос.")

# ==========================================
# 7. МЕНЮ, ПРОФИЛЬ, CALLBACKS
# ==========================================
def main_kb(user_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Сдать номер", callback_data="sel_tariff")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="🆘 Помощь", callback_data="ask_help")
    if user_id == ADMIN_ID: kb.button(text="⚙️ Админ", callback_data="admin_main")
    kb.adjust(1, 2, 1)
    return kb.as_markup()

@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name, last_afk_check) VALUES (?, ?, ?, ?)", (uid, m.from_user.username, m.from_user.first_name, get_now()))
            await db.commit()
            if ADMIN_ID:
                try: await m.bot.send_message(ADMIN_ID, f"👤 Новый: {uid}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅", callback_data=f"acc_ok_{uid}"), InlineKeyboardButton(text="🚫", callback_data=f"acc_no_{uid}")]]))
                except: pass
            return await m.answer("🔒 Ждите одобрения.")
        if res['is_approved']: await m.answer(f"👋 Привет!", reply_markup=main_kb(uid))
        else: await m.answer("⏳ Заявка на рассмотрении.")

@router.callback_query(F.data == "back_main")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Главное меню", reply_markup=main_kb(c.from_user.id))
    await c.answer()

# --- ПРОФИЛЬ (НОВОЕ) ---
@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?", (uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status IN ('work','active')", (uid,))).fetchone())[0]
    
    await c.message.edit_text(
        f"👤 <b>Профиль</b>\n{SEP}\n🆔 ID: {uid}\n📦 Всего сдано: {total}\n🔥 Активных: {active}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back_main")]]),
        parse_mode="HTML"
    )
    await c.answer()

# --- ПОМОЩЬ (НОВОЕ) ---
@router.callback_query(F.data == "ask_help")
async def cb_help(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_question)
    await c.message.edit_text(
        f"🆘 <b>Помощь</b>\n{SEP}\nНапишите ваш вопрос одним сообщением:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back_main")]]),
        parse_mode="HTML"
    )
    await c.answer()

# --- СДАЧА НОМЕРА ---
@router.callback_query(F.data == "sel_tariff")
async def cb_sel(c: CallbackQuery):
    async with get_db() as db: rows = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in rows: kb.button(text=f"{t['name']} | {t['price']}", callback_data=f"add_num_{t['name']}")
    kb.button(text="🔙", callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text("Выберите сервис:", reply_markup=kb.as_markup())
    await c.answer()

@router.callback_query(F.data.startswith("add_num_"))
async def cb_add(c: CallbackQuery, state: FSMContext):
    tariff = c.data.split("_")[2]
    await state.update_data(tariff=tariff)
    await state.set_state(UserState.waiting_number)
    await c.message.edit_text(f"📞 Тариф: {tariff}\nПиши номер (+7...):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back_main")]]))
    await c.answer()

# --- КНОПКИ ВОРКЕРА (ОБНОВЛЕНО) ---
@router.callback_query(F.data.startswith("w_"))
async def cb_worker(c: CallbackQuery, bot: Bot):
    act, nid = c.data.split("_")[1], int(c.data.split("_")[2])
    
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?", (nid,))).fetchone()
        if not row: return await c.answer("Не найдено")
        if row['worker_id'] != c.from_user.id: return await c.answer("Не твой!", show_alert=True)
        
        st, msg, user_msg = "dead", "❌ Отмена", "❌ Ошибка"
        
        if act == "suc": 
            st, msg, user_msg = "finished", "✅ Успех", "✅ Номер успешно проверен!"
        elif act == "drop": 
            st, msg, user_msg = "dead", "📉 Слет", "📉 Номер слетел."
        elif act == "skip": 
            st, msg, user_msg = "dead", "⚠️ Пропуск", "⚠️ Офис пропустил номер."
        elif act == "err": 
            st, msg, user_msg = "dead", "❌ Ошибка", "❌ Техническая ошибка / Отмена"
        
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (st, get_now(), nid))
        await db.commit()
    
    await c.message.edit_text(f"{msg}\n{row['phone']}", reply_markup=None)
    try: await bot.send_message(row['user_id'], user_msg)
    except: pass
    await c.answer()

# --- AFK OK BUTTON (НОВОЕ) ---
@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk_ok(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    if c.from_user.id != uid:
        return await c.answer("🚫 Не для вас!", show_alert=True)
    
    async with get_db() as db:
        await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
        await db.commit()
    
    await c.message.delete()
    await c.answer("✅ Вы в очереди!")

# --- MONITORING (ОБНОВЛЕНО) ---
async def monitor(bot: Bot):
    logger.info("👀 Monitor started (Aggressive Mode)")
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        
        try:
            async with get_db() as db:
                # Берем уникальных юзеров у которых есть номера в очереди
                users = await (await db.execute("""
                    SELECT DISTINCT u.user_id, u.last_afk_check 
                    FROM users u 
                    JOIN numbers n ON u.user_id = n.user_id 
                    WHERE n.status = 'queue'
                """)).fetchall()
                
                for u in users:
                    uid = u['user_id']
                    last = u['last_afk_check']
                    
                    # Если last_afk_check пустой или прошло AFK_CHECK_MINUTES
                    if not last or (not last.startswith("PENDING") and (now - datetime.fromisoformat(last)).total_seconds() / 60 > AFK_CHECK_MINUTES):
                        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👋 Я тут!", callback_data=f"afk_ok_{uid}")]])
                        try:
                            await bot.send_message(uid, f"⚠️ <b>Проверка активности!</b>\n{SEP}\nНажми кнопку, иначе заявка удалится.", reply_markup=kb, parse_mode="HTML")
                            await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (f"PENDING_{get_now()}", uid))
                        except TelegramForbiddenError:
                            await db.execute("DELETE FROM numbers WHERE user_id=? AND status='queue'", (uid,))
                        except Exception as e:
                            logger.error(f"AFK Send Error: {e}")
                    
                    # Если висит статус PENDING и время вышло
                    elif str(last).startswith("PENDING_"):
                        pt = datetime.fromisoformat(last.split("_")[1])
                        if (now - pt).total_seconds() / 60 > AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE user_id=? AND status='queue'", (uid,))
                            # Сбрасываем таймер, чтобы юзер мог заново подать потом без мгновенного кика
                            await db.execute("UPDATE users SET last_afk_check=? WHERE user_id=?", (get_now(), uid))
                            try: await bot.send_message(uid, "❌ Заявки удалены из-за неактивности.")
                            except: pass
                
                await db.commit()
                
        except Exception as e:
            logger.exception(f"Monitor Loop Error: {e}")
            await asyncio.sleep(5)

# --- АДМИНКА ---
@router.callback_query(F.data == "admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id == ADMIN_ID: 
        await c.message.edit_text("Админка", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отчеты", callback_data="adm_reports"), InlineKeyboardButton(text="🔙", callback_data="back_main")]]))
    await c.answer()

@router.callback_query(F.data.startswith("acc_"))
async def cb_acc(c: CallbackQuery, bot: Bot):
    act, uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        await db.execute(f"UPDATE users SET is_{'approved' if act=='ok' else 'banned'}=1 WHERE user_id=?", (uid,))
        await db.commit()
    await c.message.edit_text(f"Done {act}")
    try: await bot.send_message(uid, "✅ Доступ!" if act=="ok" else "🚫 Бан")
    except: pass

# --- START ---
async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(monitor(bot))
    logger.info("🚀 BOT v27.0 STARTED (Ultimate + Fixes)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
