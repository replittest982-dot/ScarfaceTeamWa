import asyncio
import logging
import sys
import os
import re
import io
import csv
from datetime import datetime, timezone, timedelta

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest

# --- CONFIG ---
TOKEN = os.getenv("BOT_TOKEN") 
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR and ADMIN_ID_STR.isdigit() else None
DB_NAME = "fast_team_v28_5.db" 
REF_PERCENT = 0.05 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
router = Router()

# Хранилище для удаления старых AFK сообщений
last_afk_messages = {}

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()
    waiting_for_support_msg = State() # Новый стейт для техподдержки

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    trf_adding_name = State()
    trf_adding_price = State()
    trf_adding_hold = State()

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, 
            is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
            referrer_id INTEGER, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, 
            method TEXT, tariff_name TEXT, tariff_price TEXT, tariff_hold TEXT, 
            status TEXT, worker_id INTEGER, start_time TIMESTAMP,
            end_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, price TEXT, hold_info TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_status', 'on')")
        await db.commit()
        logging.info("🚀 DB v28.5 (SUPPORT EDITION) INITIALIZED")

# --- UTILS ---
def clean_phone(phone: str):
    clean = re.sub(r'[^\d]', '', str(phone))
    if len(clean) == 11 and clean.startswith('8'): clean = '7' + clean[1:]
    elif len(clean) == 10: clean = '7' + clean
    return '+' + clean if (11 <= len(clean) <= 15) else None

def extract_price_float(price_str):
    if not price_str: return 0.0
    try: return float(re.sub(r'[^\d.]', '', str(price_str)))
    except: return 0.0

def calculate_duration(start_iso, end_iso):
    if not start_iso or not end_iso: return "00:00:00"
    try:
        diff = datetime.fromisoformat(end_iso) - datetime.fromisoformat(start_iso)
        h, rem = divmod(int(diff.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02}:{m:02}:{s:02}"
    except: return "00:00:00"

async def check_work_status():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_status'") as cur:
            res = await cur.fetchone()
            return res[0] == 'on' if res else True

async def is_banned(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,)) as cur:
            res = await cur.fetchone()
            return res and res[0] == 1

# --- BACKGROUND TASK: ANTI-AFK ---
async def anti_afk_task(bot: Bot):
    while True:
        await asyncio.sleep(300) # 5 минут
        try:
            active_chats = []
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT key FROM config WHERE key LIKE 'group_cfg_%'") as cur:
                    rows = await cur.fetchall()
            
            for r in rows:
                try:
                    chat_id = int(r[0].replace('group_cfg_', ''))
                    # Удаляем старое
                    if chat_id in last_afk_messages:
                        try: await bot.delete_message(chat_id, last_afk_messages[chat_id])
                        except: pass
                    # Шлем новое, если ворк включен
                    if await check_work_status():
                        msg = await bot.send_message(chat_id, "👋 **Воркеры, вы тут?**\nНе спим, разбираем очередь! 🚀", parse_mode="Markdown")
                        last_afk_messages[chat_id] = msg.message_id
                except: pass
        except Exception as e:
            logging.error(f"AFK LOOP ERROR: {e}")

# --- KEYBOARDS ---
def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="🗂 Мои номера / Удалить", callback_data="my_numbers_menu"), InlineKeyboardButton(text="📊 Очередь", callback_data="public_queue")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="menu_support")] # НОВАЯ КНОПКА
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="⚡️ ADMIN PANEL", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- SUPPORT HANDLERS (NEW) ---
@router.callback_query(F.data == "menu_support")
async def ask_support(c: CallbackQuery, state: FSMContext):
    if await is_banned(c.from_user.id): return await c.answer("Бан.")
    
    await c.message.edit_text(
        "📝 **Напишите ваш вопрос или проблему:**\n"
        "Администратор получит ваше сообщение и ответит в ближайшее время.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_main")]])
    )
    await state.set_state(UserState.waiting_for_support_msg)

@router.message(UserState.waiting_for_support_msg)
async def send_support_msg(message: types.Message, state: FSMContext, bot: Bot):
    if not message.text: return await message.reply("Отправьте текст.")
    
    # Отправка админу
    text_to_admin = (
        f"🆘 **ВОПРОС В ПОДДЕРЖКУ**\n"
        f"👤 От: {message.from_user.first_name} (@{message.from_user.username})\n"
        f"🆔 ID: `{message.from_user.id}`\n\n"
        f"❓ **Вопрос:**\n{message.text}"
    )
    
    try:
        await bot.send_message(ADMIN_ID, text_to_admin, parse_mode="Markdown")
        await message.answer("✅ **Ваше сообщение отправлено!**\nАдмин скоро ответит.", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    except Exception as e:
        await message.answer("❌ Ошибка отправки. Попробуйте позже.")
        logging.error(f"Support send error: {e}")
        
    await state.clear()


# --- START & HELP ---
@router.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    if await is_banned(message.from_user.id): return
    ref_id = int(command.args) if (command.args and command.args.isdigit() and int(command.args) != message.from_user.id) else None

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id = ?", (message.from_user.id,)) as c: res = await c.fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name, is_approved, referrer_id) VALUES (?, ?, ?, 0, ?)", 
                             (message.from_user.id, message.from_user.username, message.from_user.first_name, ref_id))
            await db.commit()
            try: await message.bot.send_message(ADMIN_ID, f"👤 Новый юзер: {message.from_user.id} (@{message.from_user.username})", 
                                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                                    InlineKeyboardButton(text="✅", callback_data=f"acc_ok_{message.from_user.id}"), 
                                                    InlineKeyboardButton(text="🚫", callback_data=f"acc_no_{message.from_user.id}")]]))
            except: pass
            return await message.answer("🔒 Ожидайте подтверждения.")
            
    if res[0] == 1: 
        await message.answer(f"👋 **Привет, {message.from_user.first_name}!**\n🚀 Добро пожаловать в FAST TEAM.", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    else: await message.answer("⏳ Доступ закрыт.")

@router.callback_query(F.data == "menu_guide")
async def show_guide(c: CallbackQuery):
    # НОВЫЙ ТЕКСТ ПОМОЩИ
    text = (
        "ℹ️ **ПОМОЩЬ**\n\n"
        "📲 **Что делает бот**\n"
        "Бот принимает номера WhatsApp / MAX, ставит их в очередь и выплачивает средства после успешной проверки.\n\n"
        "📦 **Требования к номерам**\n"
        "✔️ Активный и чистый номер\n"
        "✔️ Доступ к SMS\n"
        "❌ Виртуальные, заблокированные и использованные номера не принимаются\n\n"
        "⏳ **Холд и выплаты**\n"
        "Холд — время проверки номера\n"
        "💰 Выплата производится после успешного завершения холда\n\n"
        "⚠️ **Отправляя номер, вы подтверждаете, что ознакомились с правилами.**"
    )
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(c.from_user.id))

# --- WORKER HANDLERS ---
@router.message(F.photo)
async def sms_photo_handler(m: types.Message, bot: Bot):
    if not m.caption or not m.caption.strip().startswith("/sms"): return
    try:
        parts = m.caption.strip().split(None, 2)
        if len(parts) < 2: return await m.reply("⚠️ /sms номер текст")
        ph = clean_phone(parts[1])
        if not ph: return await m.reply("❌ Кривой номер")
        tx = parts[2] if len(parts) > 2 else "Вход в аккаунт 👆"
        
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: r = await cur.fetchone()
        if r:
            await bot.send_photo(r[0], m.photo[-1].file_id, caption=f"🔔 **SMS / КОД**\n📱 `{ph}`\n💬 {tx}", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else: await m.reply(f"🚫 Номер {ph} не в работе.")
    except: pass

@router.message(Command("sms"))
async def sms_text_handler(m: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ /sms номер код")
    try:
        args = command.args.split(None, 1)
        ph = clean_phone(args[0])
        if not ph: return await m.reply("❌ Кривой номер")
        tx = args[1] if len(args) > 1 else "Код выше"
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: r = await cur.fetchone()
        if r:
            await bot.send_message(r[0], f"🔔 **SMS / КОД**\n📱 `{ph}`\n💬 {tx}", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else: await m.reply(f"🚫 Номер {ph} не в работе.")
    except: pass

@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    if not rows: return await message.answer("❌ Нет тарифов.")
    kb = [[InlineKeyboardButton(text=f"📌 {r[0]}", callback_data=f"set_group_{r[0]}")] for r in rows]
    await message.answer("⚙️ **Настройка группы**", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("set_group_"))
async def set_group_tariff(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_group_")[1]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (f"group_cfg_{c.message.chat.id}", t_name))
        await db.commit()
    await c.message.delete()
    await c.message.answer(f"✅ **Группа привязана!**\n💎 Тариф: **{t_name}**\n\n📋 **ВОРКЕРАМ:**\n`/num` - взять номер\n`/sms номер код` - отправить код", parse_mode="Markdown")

@router.message(Command("num"))
async def cmd_num(message: types.Message):
    if not await check_work_status(): return await message.reply("⛔️ Ворк на паузе.")
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (f"group_cfg_{message.chat.id}",)) as cur: t_res = await cur.fetchone()
        if not t_res: return 
        async with db.execute("SELECT id, user_id, phone, tariff_price FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY created_at ASC LIMIT 1", (t_res[0],)) as cur:
            row = await cur.fetchone()
        if not row: return await message.reply("📭 Очередь пуста!")
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", 
                         (message.from_user.id, datetime.now(timezone.utc).isoformat(), row[0]))
        await db.commit()
    
    await message.answer(f"🚀 **В РАБОТЕ**\n📱 `{row[2]}`\n💰 Тариф: {t_res[0]}", 
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                             InlineKeyboardButton(text="Встал ✅", callback_data=f"w_act_{row[0]}"), 
                             InlineKeyboardButton(text="Ошибка ❌", callback_data=f"w_err_{row[0]}")]]), parse_mode="Markdown")

@router.callback_query(F.data.startswith("w_act_"))
async def worker_activate(c: CallbackQuery, bot: Bot):
    nid = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (nid,)) as cur: res = await cur.fetchone()
        await db.commit()
    await c.message.edit_text(f"📉 **СЛЕТ**\n📱 `{res[0]}`", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📉 СЛЕТ", callback_data=f"w_drop_{nid}")]]), parse_mode="Markdown")
    try: await bot.send_message(res[1], f"⚙️ **Ваш номер в работе!**\n📱 `{res[0]}`\nВоркер начал действия. Ожидайте.", parse_mode="Markdown")
    except: pass

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def worker_fin(c: CallbackQuery, bot: Bot):
    nid = c.data.split('_')[2]
    st = "drop" if "drop" in c.data else "dead"
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, user_id, start_time, tariff_price FROM numbers WHERE id=?", (nid,)) as cur: res = await cur.fetchone()
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (st, now_iso, nid))
        if st == "drop":
            async with db.execute("SELECT referrer_id FROM users WHERE user_id=?", (res[1],)) as cur: ref_data = await cur.fetchone()
            if ref_data and ref_data[0]:
                reward = round(extract_price_float(res[3]) * REF_PERCENT, 3)
                if reward > 0:
                    try: await bot.send_message(ref_data[0], f"💰 **Реферальный бонус!**\n+ {reward}$", parse_mode="Markdown")
                    except: pass
        await db.commit()
    
    await c.message.edit_text(f"🏁 Финал: {'СЛЕТ (Ожидает выплаты)' if st == 'drop' else 'ОШИБКА'}\n📱 {res[0]}")
    try:
        dur = calculate_duration(res[2], now_iso)
        msg = (f"📉 **НОМЕР СЛЕТЕЛ!**\n📱 `{res[0]}`\n⏱ Время: {dur}\n💰 Статус: **Ожидает выплату**" if st=="drop" else f"❌ **ОШИБКА**\n📱 `{res[0]}`")
        await bot.send_message(res[1], msg, parse_mode="Markdown")
    except: pass

# --- MENUS ---
@router.callback_query(F.data == "menu_profile")
async def show_profile(c: CallbackQuery, bot: Bot):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*), SUM(CAST(tariff_price AS REAL)) FROM numbers WHERE user_id=? AND status='drop'", (uid,)) as cur: stats = await cur.fetchone()
        async with db.execute("SELECT COUNT(*) FROM users WHERE referrer_id=?", (uid,)) as cur: ref_count = (await cur.fetchone())[0]
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (uid,)) as cur: reg = (await cur.fetchone())[0].split('T')[0]
    
    text = (f"👤 **Профиль**\n🆔 `{uid}`\n📅 Рег: {reg}\n\n👥 Рефералов: **{ref_count}**\n\n📊 **Стат:**\n✅ Сдано: **{stats[0] or 0}**\n💰 Баланс: **{stats[1] or 0.0}$**")
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(uid))

@router.callback_query(F.data == "my_numbers_menu")
async def my_numbers_menu(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, phone, tariff_name FROM numbers WHERE user_id=? AND status='queue'", (uid,)) as cur: queue_rows = await cur.fetchall()
        async with db.execute("SELECT phone, status FROM numbers WHERE user_id=? AND status IN ('work','active')", (uid,)) as cur: active_rows = await cur.fetchall()
        async with db.execute("SELECT phone, status, start_time, end_time FROM numbers WHERE user_id=? AND status IN ('drop','dead') ORDER BY id DESC LIMIT 5", (uid,)) as cur: history_rows = await cur.fetchall()

    text = "🗂 **МОИ НОМЕРА**\n\n⏳ **Очередь:**\n" + ("— Пусто —\n" if not queue_rows else "".join([f"• `{r[1]}`\n" for r in queue_rows]))
    text += "\n⚙️ **В работе:**\n" + ("— Пусто —\n" if not active_rows else "".join([f"🔥 `{r[0]}`\n" for r in active_rows]))
    text += "\n📜 **История:**\n" + ("— Пусто —\n" if not history_rows else "".join([f"{'✅' if r[1]=='drop' else '❌'} `{r[0]}` | {calculate_duration(r[2], r[3])}\n" for r in history_rows]))

    kb = []
    if queue_rows: kb.append([InlineKeyboardButton(text="🗑 Удалить номер (Из очереди)", callback_data="cancel_queue_menu")])
    kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="my_numbers_menu")])
    kb.append([InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")])
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data == "cancel_queue_menu")
async def cancel_queue_menu(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, phone FROM numbers WHERE user_id=? AND status='queue'", (uid,)) as cur: rows = await cur.fetchall()
    if not rows: return await c.answer("Нечего удалять.")
    kb = [[InlineKeyboardButton(text=f"❌ {r[1]}", callback_data=f"del_num_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="my_numbers_menu")])
    await c.message.edit_text("🗑 **Удаление:**", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")

@router.callback_query(F.data.startswith("del_num_"))
async def delete_number(c: CallbackQuery):
    nid = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("DELETE FROM numbers WHERE id=? AND user_id=? AND status='queue'", (nid, c.from_user.id)): await db.commit()
    await c.answer("Удалено!")
    await my_numbers_menu(c)

@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    if await is_banned(c.from_user.id): return await c.answer("Бан.")
    if not await check_work_status(): return await c.answer("⛔️ Ворк на паузе.", show_alert=True)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as cur: rows = await cur.fetchall()
    if not rows: return await c.answer("Нет тарифов", show_alert=True)
    kb = [[InlineKeyboardButton(text=r[0], callback_data=f"trf_pick_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")])
    await c.message.edit_text("👇 **Выберите тариф:**", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("trf_pick_"))
async def step_method(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split('trf_pick_')[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as cur: res = await cur.fetchone()
    await state.update_data(tariff_name=t_name, tariff_price=res[0], tariff_hold=res[1])
    kb = [[InlineKeyboardButton(text="✉️ SMS", callback_data="input_sms"), InlineKeyboardButton(text="📸 QR", callback_data="input_qr")],
          [InlineKeyboardButton(text="🔙 Назад", callback_data="select_tariff")]]
    await c.message.edit_text(f"💎 {t_name} | {res[0]}$\n⏳ {res[1]}\n\nСпособ:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == "input_sms" else 'qr')
    await c.message.edit_text("📱 **Введите номера (списком):**\n", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]))
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    if not await check_work_status():
        await state.clear()
        return await message.answer("⛔️ Ворк на паузе.")
    
    d = await state.get_data()
    raw = message.text.replace(',', '\n').split('\n')
    added, errors = 0, 0
    async with aiosqlite.connect(DB_NAME) as db:
        for line in raw:
            p = clean_phone(line.strip())
            if p:
                async with db.execute("SELECT id FROM numbers WHERE phone=? AND status IN ('queue','work','active')", (p,)) as cur: exists = await cur.fetchone()
                if not exists:
                    await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)", 
                                     (message.from_user.id, p, d['method'], d['tariff_name'], d['tariff_price'], d['tariff_hold'], datetime.now(timezone.utc).isoformat()))
                    added += 1
                else: errors += 1
        await db.commit()
    
    msg = f"✅ Принято: **{added}**"
    if errors > 0: msg += f"\n🚫 Отклонено (в работе): {errors}"
    await message.answer(msg, reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery): await show_profile(c, c.message.bot)

@router.callback_query(F.data == "public_queue")
async def public_queue_view(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT tariff_name, COUNT(*) FROM numbers WHERE status='queue' GROUP BY tariff_name") as cur: stats = await cur.fetchall()
    text = "📊 **Общая очередь:**\n\n" + ("📭 Пусто" if not stats else "\n".join([f"🔹 {t}: **{c}** шт." for t, c in stats]))
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]]))

# --- ADMIN ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    is_work = await check_work_status()
    kb = [
        [InlineKeyboardButton(text=f"Ворк: {'🟢' if is_work else '🔴'}", callback_data="adm_toggle_work")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="➕ Тариф", callback_data="adm_trf_add"), InlineKeyboardButton(text="🗑 Тариф", callback_data="adm_trf_del_menu")],
        [InlineKeyboardButton(text="📄 ОТЧЕТЫ (2 ФАЙЛА)", callback_data="adm_report")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")]
    ]
    await c.message.edit_text("⚡️ **ADMIN PANEL**", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data == "adm_toggle_work")
async def adm_toggle_work(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_status'") as cur: res = await cur.fetchone()
        new_status = 'off' if (res and res[0] == 'on') else 'on'
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_status', ?)", (new_status,))
        await db.commit()
    await adm_start(c)

@router.callback_query(F.data == "admin_broadcast")
async def broadcast_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("📢 **Отправьте пост:**", parse_mode="Markdown")
    await state.set_state(AdminState.waiting_for_broadcast)

@router.message(AdminState.waiting_for_broadcast)
async def broadcast_send(message: types.Message, state: FSMContext, bot: Bot):
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cur: users = await cur.fetchall()
    cnt = 0
    await message.answer(f"🚀 Запуск...")
    for u in users:
        try:
            await bot.copy_message(u[0], message.chat.id, message.message_id)
            cnt += 1
            await asyncio.sleep(0.05)
        except: pass
    await message.answer(f"✅ Ушло: {cnt}")

@router.callback_query(F.data == "adm_report")
async def adm_report(c: CallbackQuery):
    async def get_csv(only_paid=False):
        where_clause = "WHERE n.status = 'drop'" if only_paid else ""
        async with aiosqlite.connect(DB_NAME) as db:
            rows = await (await db.execute(f"""SELECT n.phone, n.start_time, n.end_time, u.username, n.tariff_name, n.tariff_price, n.tariff_hold, n.status 
                                               FROM numbers n JOIN users u ON n.user_id=u.user_id {where_clause}""")).fetchall()
        if not rows: return None
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Number", "Duration", "Username", "Rent Group", "Thread", "Start Time", "End Time", "Hold Time", "Hold Price", "Break?", "Stopped?"])
        for r in rows:
            dur = calculate_duration(r[1], r[2])
            writer.writerow([r[0], dur, r[3] or "NoUser", r[4], "Main", r[1] or "-", r[2] or "-", r[6], r[5], "No", "Yes" if r[7] == 'dead' else "No"])
        buffer.seek(0)
        return buffer.getvalue().encode('utf-8-sig')

    file_paid = await get_csv(only_paid=True)
    file_full = await get_csv(only_paid=False)

    if file_paid: await c.message.answer_document(BufferedInputFile(file_paid, filename=f"report_PAID.csv"), caption="✅ Оплаченные")
    if file_full: await c.message.answer_document(BufferedInputFile(file_full, filename=f"report_FULL.csv"), caption="📊 Полный отчет")
    if not file_paid and not file_full: await c.answer("Пусто")

@router.message(Command("ban"))
async def cmd_ban(message: types.Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID or not command.args: return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (int(command.args),))
        await db.commit()
    await message.reply("🚫 BANNED")

@router.message(Command("unban"))
async def cmd_unban(message: types.Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID or not command.args: return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (int(command.args),))
        await db.commit()
    await message.reply("✅ UNBANNED")

# --- TARIFFS ---
@router.callback_query(F.data == "adm_trf_add")
async def adm_trf_add(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📝 Имя:")
    await state.set_state(AdminState.trf_adding_name)
@router.message(AdminState.trf_adding_name)
async def adm_n(m: types.Message, state: FSMContext):
    await state.update_data(name=m.text)
    await m.answer("💰 Цена:")
    await state.set_state(AdminState.trf_adding_price)
@router.message(AdminState.trf_adding_price)
async def adm_p(m: types.Message, state: FSMContext):
    await state.update_data(price=m.text)
    await m.answer("⏳ Холд:")
    await state.set_state(AdminState.trf_adding_hold)
@router.message(AdminState.trf_adding_hold)
async def adm_h(m: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute("INSERT INTO tariffs (name, price, hold_info) VALUES (?, ?, ?)", (d['name'], d['price'], m.text))
            await db.commit()
            await m.answer("✅ Created")
        except: await m.answer("❌ Error")
    await state.clear()

@router.callback_query(F.data == "adm_trf_del_menu")
async def adm_del_menu(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name FROM tariffs") as cur: rows = await cur.fetchall()
    kb = [[InlineKeyboardButton(text=f"❌ {r[1]}", callback_data=f"del_trf_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton(text="🔙", callback_data="admin_panel_start")])
    await c.message.edit_text("🗑 Удалить:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
@router.callback_query(F.data.startswith("del_trf_"))
async def adm_del(c: CallbackQuery):
    tid = int(c.data.split("_")[2])
    async with aiosqlite.connect(DB_NAME) as db: await db.execute("DELETE FROM tariffs WHERE id=?", (tid,)); await db.commit()
    await c.answer("Удалено")
    await adm_del_menu(c)

@router.callback_query(F.data.startswith("acc_"))
async def acc(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split('_')[1], int(c.data.split('_')[2])
    if act == "ok":
        async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,)); await db.commit()
        try: await bot.send_message(uid, "✅ Доступ открыт! /start")
        except: pass
        await c.message.edit_text("✅ OK")
    else: await c.message.edit_text("🚫 NO")

async def main():
    if not TOKEN: print("❌ TOKEN!"); return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    asyncio.create_task(anti_afk_task(bot))
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
