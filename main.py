import asyncio
import logging
import sys
import os
import re
import io
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
DB_NAME = "fast_team_v27_6.db" 
REF_PERCENT = 0.05  # 5% реферальных

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
router = Router()

# --- STATES ---
class UserState(StatesGroup):
    waiting_for_number = State()

class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    trf_adding_name = State()
    trf_adding_price = State()
    trf_adding_hold = State()

# --- DATABASE INIT ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Юзеры
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            username TEXT, 
            first_name TEXT, 
            is_approved INTEGER DEFAULT 0, 
            referrer_id INTEGER, 
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        
        # Номера
        await db.execute("""CREATE TABLE IF NOT EXISTS numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            phone TEXT, 
            method TEXT, 
            tariff_name TEXT, 
            tariff_price TEXT, 
            tariff_hold TEXT, 
            status TEXT, 
            worker_id INTEGER, 
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, price TEXT, hold_info TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        # Устанавливаем статус ворка по умолчанию ON, если его нет
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_status', 'on')")
        
        await db.commit()
        logging.info("🚀 FAST TEAM BOT v27.6 READY")

# --- UTILS ---
def clean_phone(phone: str):
    clean = re.sub(r'[^\d+]', '', str(phone))
    if clean.startswith('8') and len(clean) == 11: clean = '+7' + clean[1:]
    elif clean.startswith('7') and len(clean) == 11: clean = '+' + clean
    elif len(clean) == 10 and clean.isdigit(): clean = '+7' + clean
    return clean if re.match(r'^\+\d{10,15}$', clean) else None

def extract_price_float(price_str):
    if not price_str: return 0.0
    clean = re.sub(r'[^\d.]', '', str(price_str))
    try: return float(clean)
    except: return 0.0

def calculate_duration(start_iso, end_iso):
    if not start_iso or not end_iso: return "-"
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        diff = e - s
        h, rem = divmod(diff.seconds, 3600)
        m, _ = divmod(rem, 60)
        return f"{h}ч {m}м"
    except: return "-"

async def check_work_status():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_status'") as cur:
            res = await cur.fetchone()
            return res[0] == 'on' if res else True

# --- KEYBOARDS ---
def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="🗂 Мои номера", callback_data="my_numbers_menu"), InlineKeyboardButton(text="📊 Очередь", callback_data="public_queue")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="⚡️ ADMIN PANEL", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- WORKER: PHOTO & SMS ---
@router.message(F.photo)
async def sms_photo_handler(m: types.Message, bot: Bot):
    if not m.caption: return
    caption_clean = m.caption.strip()
    if not caption_clean.startswith("/sms"): return

    try:
        parts = caption_clean.split(None, 2)
        if len(parts) < 2: return await m.reply("⚠️ Формат: /sms номер текст")

        ph_raw = parts[1]
        tx = parts[2] if len(parts) > 2 else "Вход в аккаунт 👆"
        
        ph = clean_phone(ph_raw)
        if not ph: return await m.reply(f"❌ Кривой номер: {ph_raw}")

        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur:
                r = await cur.fetchone()
        
        if r:
            await bot.send_photo(chat_id=r[0], photo=m.photo[-1].file_id, caption=f"🔔 **SMS / КОД**\n📱 `{ph}`\n💬 {tx}", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else:
            await m.reply(f"🚫 Номер {ph} не в работе.")
    except Exception as e:
        logging.error(f"Error photo: {e}")
        await m.reply("❌ Ошибка отправки.")

@router.message(Command("sms"))
async def sms_text_handler(m: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Формат: /sms номер код")
    try:
        args = command.args.split(None, 1)
        ph = clean_phone(args[0])
        tx = args[1] if len(args) > 1 else "Код выше"
        
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur:
                r = await cur.fetchone()
        if r:
            await bot.send_message(r[0], f"🔔 **SMS / КОД**\n📱 `{ph}`\n💬 {tx}", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else:
            await m.reply(f"🚫 Номер {ph} не в работе.")
    except: pass

# --- ADMIN: SETUP GROUP & SWITCH ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    
    if not rows: return await message.answer("❌ Нет тарифов.")
    
    # Кнопки выбора тарифа
    kb = [[InlineKeyboardButton(text=f"📌 {r[0]}", callback_data=f"set_group_{r[0]}")] for r in rows]
    await message.answer("⚙️ **Настройка группы**\nВыберите глобальный тариф для этого чата:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")

@router.callback_query(F.data.startswith("set_group_"))
async def set_group_tariff(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_group_")[1]
    # Привязка к ID чата (группы)
    key = f"group_cfg_{c.message.chat.id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name))
        await db.commit()
    
    await c.message.delete()
    await c.message.answer(
        f"✅ **Группа настроена!**\n💎 Тариф: **{t_name}**\n\n"
        f"📋 **ВОРКЕРАМ:**\n`/num` — взять номер\n`/sms номер код` — отправить смс", 
        parse_mode="Markdown"
    )

@router.message(Command("num"))
async def cmd_num(message: types.Message):
    # Проверяем конфиг группы
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (f"group_cfg_{message.chat.id}",)) as cur:
            t_res = await cur.fetchone()
        if not t_res: return # Бот не настроен в этом чате
        
        async with db.execute("SELECT id, user_id, phone, tariff_price FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY created_at ASC LIMIT 1", (t_res[0],)) as cur:
            row = await cur.fetchone()
        
        if not row: return await message.reply("📭 Очередь пуста!")
        
        # Воркер берет номер -> статус WORK
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", 
                         (message.from_user.id, datetime.now(timezone.utc).isoformat(), row[0]))
        await db.commit()
    
    await message.answer(
        f"🚀 **В РАБОТЕ**\n📱 `{row[2]}`\n💰 {t_res[0]}", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Встал ✅", callback_data=f"w_act_{row[0]}"), 
            InlineKeyboardButton(text="Ошибка ❌", callback_data=f"w_err_{row[0]}")
        ]]), 
        parse_mode="Markdown"
    )

@router.callback_query(F.data.startswith("w_act_"))
async def worker_activate(c: CallbackQuery, bot: Bot):
    nid = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id = ?", (nid,)) as cur:
            res = await cur.fetchone()
        await db.commit()
    
    # 1. Меняем сообщение воркера
    await c.message.edit_text(
        f"📉 **СЛЕТ**\n📱 `{res[0]}`", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📉 СЛЕТ", callback_data=f"w_drop_{nid}")]]), 
        parse_mode="Markdown"
    )

    # 2. УВЕДОМЛЕНИЕ ДРОПУ
    try:
        await bot.send_message(res[1], f"⚙️ **Ваш номер в работе!**\n📱 `{res[0]}`\nВоркер начал действия. Ожидайте SMS.", parse_mode="Markdown")
    except: pass

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def worker_fin(c: CallbackQuery, bot: Bot):
    nid = c.data.split('_')[2]
    st = "drop" if "drop" in c.data else "dead"
    now_iso = datetime.now(timezone.utc).isoformat()
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, user_id, start_time, tariff_price FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
            
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (st, now_iso, nid))
        
        # Рефералка
        if st == "drop":
            async with db.execute("SELECT referrer_id FROM users WHERE user_id=?", (res[1],)) as cur:
                ref_data = await cur.fetchone()
            if ref_data and ref_data[0]:
                reward = round(extract_price_float(res[3]) * REF_PERCENT, 3)
                if reward > 0:
                    try: await bot.send_message(ref_data[0], f"💰 **Реферальный бонус!**\n+ {reward}$ (5%) за номер `{res[0]}`", parse_mode="Markdown")
                    except: pass
        await db.commit()
    
    status_text = "СЛЕТ (Ожидает выплаты)" if st == "drop" else "ОШИБКА"
    await c.message.edit_text(f"🏁 Финал: {status_text}\n📱 {res[0]}")
    
    # Уведомление Дропу с итогом
    try:
        dur = calculate_duration(res[2], now_iso)
        msg = (f"📉 **НОМЕР СЛЕТЕЛ!**\n📱 `{res[0]}`\n⏱ Время: {dur}\n💰 Статус: **Ожидает выплату**" 
               if st=="drop" else 
               f"❌ **ОТМЕНА / ОШИБКА**\n📱 `{res[0]}`\nПричина: Нет кода или ошибка входа.")
        await bot.send_message(res[1], msg, parse_mode="Markdown")
    except: pass

# --- USER COMMANDS ---
@router.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    referrer_id = None
    if command.args and command.args.isdigit():
        rid = int(command.args)
        if rid != message.from_user.id: referrer_id = rid

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id = ?", (message.from_user.id,)) as c: res = await c.fetchone()
        
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name, is_approved, referrer_id) VALUES (?, ?, ?, 0, ?)", 
                             (message.from_user.id, message.from_user.username, message.from_user.first_name, referrer_id))
            await db.commit()
            
            try: await message.bot.send_message(ADMIN_ID, f"👤 Запрос: {message.from_user.id} (@{message.from_user.username})\nRef: {referrer_id}", 
                                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                                    InlineKeyboardButton(text="✅", callback_data=f"acc_ok_{message.from_user.id}"), 
                                                    InlineKeyboardButton(text="🚫", callback_data=f"acc_no_{message.from_user.id}")]]))
            except: pass
            return await message.answer("🔒 Ожидайте подтверждения.")
            
    if res[0] == 1: 
        await message.answer(f"👋 **Привет, {message.from_user.first_name}!**\n🚀 Добро пожаловать в FAST TEAM.", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    else: 
        await message.answer("⏳ Доступ закрыт.")

@router.callback_query(F.data == "menu_profile")
async def show_profile(c: CallbackQuery, bot: Bot):
    uid = c.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={uid}"

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*), SUM(CAST(tariff_price AS REAL)) FROM numbers WHERE user_id=? AND status='drop'", (uid,)) as cur:
            stats = await cur.fetchone()
        async with db.execute("SELECT COUNT(*) FROM users WHERE referrer_id=?", (uid,)) as cur:
            ref_count = (await cur.fetchone())[0]
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (uid,)) as cur:
            reg = (await cur.fetchone())[0].split('T')[0]

    text = (f"👤 **Профиль**\n🆔 `{uid}`\n📅 C нами с: {reg}\n\n"
            f"👥 Рефералов: **{ref_count}**\n🔗 `{ref_link}`\n\n"
            f"📊 **Статистика:**\n✅ Сдано: **{stats[0] or 0}**\n💰 Баланс: **{stats[1] or 0.0}$**")
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(uid))

@router.callback_query(F.data == "my_numbers_menu")
async def my_numbers_menu(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        # Очередь (для удаления)
        async with db.execute("SELECT id, phone, tariff_name FROM numbers WHERE user_id=? AND status='queue'", (uid,)) as cur:
            queue_rows = await cur.fetchall()
        # Активные (в работе)
        async with db.execute("SELECT phone, status FROM numbers WHERE user_id=? AND status IN ('work','active')", (uid,)) as cur:
            active_rows = await cur.fetchall()
        # История
        async with db.execute("SELECT phone, status, start_time, end_time FROM numbers WHERE user_id=? AND status IN ('drop','dead') ORDER BY id DESC LIMIT 5", (uid,)) as cur:
            history_rows = await cur.fetchall()

    text = "🗂 **МОИ НОМЕРА**\n\n"
    
    text += "⏳ **Очередь (Ожидание):**\n"
    if not queue_rows: text += "— Пусто —\n"
    else:
        for r in queue_rows: text += f"• `{r[1]}` ({r[2]})\n"

    text += "\n⚙️ **В работе:**\n"
    if not active_rows: text += "— Пусто —\n"
    else:
        for r in active_rows: text += f"🔥 `{r[0]}`\n"
        
    text += "\n📜 **История (последние 5):**\n"
    if not history_rows: text += "— Пусто —\n"
    else:
        for r in history_rows:
            st_icon = "✅" if r[1] == 'drop' else "❌"
            dur = calculate_duration(r[2], r[3])
            text += f"{st_icon} `{r[0]}` | {dur}\n"

    kb = []
    if queue_rows:
        kb.append([InlineKeyboardButton(text="🗑 Отменить (Очередь)", callback_data="cancel_queue_menu")])
    kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="my_numbers_menu")])
    kb.append([InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")])
    
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data == "cancel_queue_menu")
async def cancel_queue_menu(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, phone FROM numbers WHERE user_id=? AND status='queue'", (uid,)) as cur:
            rows = await cur.fetchall()
    
    if not rows:
        return await c.answer("Нечего отменять.", show_alert=True)

    kb = [[InlineKeyboardButton(text=f"❌ {r[1]}", callback_data=f"del_num_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="my_numbers_menu")])
    await c.message.edit_text("🗑 **Выберите номер для удаления:**", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")

@router.callback_query(F.data.startswith("del_num_"))
async def delete_number(c: CallbackQuery):
    nid = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем, что номер все еще в очереди и принадлежит юзеру
        async with db.execute("SELECT phone FROM numbers WHERE id=? AND user_id=? AND status='queue'", (nid, c.from_user.id)) as cur:
            row = await cur.fetchone()
        
        if row:
            await db.execute("DELETE FROM numbers WHERE id=?", (nid,))
            await db.commit()
            await c.answer(f"Номер {row[0]} удален!")
        else:
            await c.answer("Ошибка! Номер уже взят в работу или удален.", show_alert=True)
            
    await my_numbers_menu(c)

# --- TARIFF & ADD NUMBER ---
@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    # ПРОВЕРКА ГЛОБАЛЬНОГО СТАТУСА ВОРКА
    is_work = await check_work_status()
    if not is_work:
        return await c.answer("⛔️ Прием номеров временно ПРИОСТАНОВЛЕН админом.", show_alert=True)

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as cur: rows = await cur.fetchall()
    
    if not rows: return await c.answer("Нет тарифов", show_alert=True)
    
    kb = []
    current_row = []
    for r in rows:
        current_row.append(InlineKeyboardButton(text=r[0], callback_data=f"trf_pick_{r[0]}"))
        if len(current_row) == 2:
            kb.append(current_row)
            current_row = []
    if current_row: kb.append(current_row)
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")])
    await c.message.edit_text("👇 **Выберите тариф:**", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("trf_pick_"))
async def step_method(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split('trf_pick_')[1]
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as cur: res = await cur.fetchone()
    
    if not res: return await c.answer("Тариф удален")
    await state.update_data(tariff_name=t_name, tariff_price=res[0], tariff_hold=res[1])
    
    kb = [[InlineKeyboardButton(text="✉️ SMS", callback_data="input_sms"), InlineKeyboardButton(text="📸 QR", callback_data="input_qr")],
          [InlineKeyboardButton(text="🔙 Назад", callback_data="select_tariff")]]
    await c.message.edit_text(f"💎 {t_name} | {res[0]}$\n⏳ {res[1]}\n\nСпособ:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == "input_sms" else 'qr')
    await c.message.edit_text("📱 **Введите номера (списком):**", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]))
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    # ПОВТОРНАЯ ПРОВЕРКА СТАТУСА ВОРКА (на случай если выключили пока он вводил)
    if not await check_work_status():
        await state.clear()
        return await message.answer("⛔️ **ВОРК ОСТАНОВЛЕН!**\nВаши номера не были добавлены.", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")

    if not message.text: return
    d = await state.get_data()
    raw = message.text.replace(',', '\n').split('\n')
    added = 0
    async with aiosqlite.connect(DB_NAME) as db:
        for line in raw:
            p = clean_phone(line.strip())
            if p:
                await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)", 
                                 (message.from_user.id, p, d['method'], d['tariff_name'], d['tariff_price'], d['tariff_hold'], datetime.now(timezone.utc).isoformat()))
                added += 1
        await db.commit()
    await message.answer(f"✅ Добавлено: **{added}**", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery): await c.message.edit_text("👋 Меню", reply_markup=main_menu_kb(c.from_user.id))

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    
    # Получаем статус ворка
    is_work = await check_work_status()
    status_icon = "🟢 ВКЛЮЧЕН" if is_work else "🔴 ВЫКЛЮЧЕН"
    
    kb = [
        [InlineKeyboardButton(text=f"Ворк: {status_icon}", callback_data="adm_toggle_work")],
        [InlineKeyboardButton(text="➕ Тариф", callback_data="adm_trf_add"), InlineKeyboardButton(text="🗑 Тариф", callback_data="adm_trf_del_menu")],
        [InlineKeyboardButton(text="📄 ОТЧЕТ (.txt)", callback_data="adm_report")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")]
    ]
    await c.message.edit_text("⚡️ **ADMIN PANEL**", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data == "adm_toggle_work")
async def adm_toggle_work(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key='work_status'") as cur:
            res = await cur.fetchone()
            current = res[0] if res else 'on'
        
        new_status = 'off' if current == 'on' else 'on'
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('work_status', ?)", (new_status,))
        await db.commit()
    
    await adm_start(c) # Обновляем меню

@router.callback_query(F.data == "adm_report")
async def adm_report(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT u.username, n.phone, n.status, n.tariff_price, u.referrer_id FROM numbers n JOIN users u ON n.user_id = u.user_id ORDER BY n.id DESC") as cur: rows = await cur.fetchall()
    if not rows: return await c.answer("Пусто")
    total = sum([extract_price_float(r[3]) for r in rows if r[2] == 'drop'])
    lines = [f"{r[0]} | {r[1]} | {r[4] or '-'} | {r[2]} | {r[3]}" for r in rows]
    buf = io.BytesIO(f"REPORT\nTOTAL: {total}$\n\n".encode() + "\n".join(lines).encode())
    await c.message.answer_document(BufferedInputFile(buf.read(), filename="report.txt"), caption=f"К выплате: **{total}$**", parse_mode="Markdown")

# ... (Остальные админские хендлеры создания/удаления тарифов и доступа такие же, добавляем их для целостности) ...
@router.callback_query(F.data == "adm_trf_add")
async def adm_trf_add(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📝 Имя (ex: MAX):")
    await state.set_state(AdminState.trf_adding_name)

@router.message(AdminState.trf_adding_name)
async def adm_n(m: types.Message, state: FSMContext):
    await state.update_data(name=m.text)
    await m.answer("💰 Цена (ex: 4):")
    await state.set_state(AdminState.trf_adding_price)

@router.message(AdminState.trf_adding_price)
async def adm_p(m: types.Message, state: FSMContext):
    await state.update_data(price=m.text)
    await m.answer("⏳ Холд (ex: 1ч):")
    await state.set_state(AdminState.trf_adding_hold)

@router.message(AdminState.trf_adding_hold)
async def adm_h(m: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute("INSERT INTO tariffs (name, price, hold_info) VALUES (?, ?, ?)", (d['name'], d['price'], m.text))
            await db.commit()
            await m.answer("✅ Тариф создан!", reply_markup=main_menu_kb(m.from_user.id))
        except: await m.answer("❌ Имя занято")
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
        try: await bot.send_message(uid, "✅ Доступ!")
        except: pass
        await c.message.edit_text("✅ OK")
    else: await c.message.edit_text("🚫 NO")

async def main():
    if not TOKEN: print("❌ TOKEN!"); return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
