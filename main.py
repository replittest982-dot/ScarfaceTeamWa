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
DB_NAME = "fast_team_v28.db" 
REF_PERCENT = 0.05 

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
        # Юзеры (добавили is_banned)
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            username TEXT, 
            first_name TEXT, 
            is_approved INTEGER DEFAULT 0, 
            is_banned INTEGER DEFAULT 0,
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
        
        # Статус ворка по умолчанию ON
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('work_status', 'on')")
        
        await db.commit()
        logging.info("🚀 FAST TEAM BOT v28.0 (FINAL) STARTED")

# --- UTILS ---
def clean_phone(phone: str):
    # Жесткий фильтр. Оставляем только цифры.
    # Если начинается с 8 -> меняем на +7
    # Если с 7 -> +7
    # Если 9... (10 цифр) -> +79...
    clean = re.sub(r'[^\d]', '', str(phone))
    
    if len(clean) == 11:
        if clean.startswith('8'): clean = '7' + clean[1:]
    elif len(clean) == 10:
        clean = '7' + clean
        
    if len(clean) >= 11 and len(clean) <= 15:
        return '+' + clean
    return None

def extract_price_float(price_str):
    if not price_str: return 0.0
    clean = re.sub(r'[^\d.]', '', str(price_str))
    try: return float(clean)
    except: return 0.0

def calculate_duration(start_iso, end_iso):
    if not start_iso or not end_iso: return "00:00:00"
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        diff = e - s
        total_seconds = int(diff.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"
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
        ph = clean_phone(parts[1])
        if not ph: return await m.reply(f"❌ Кривой номер: {parts[1]}")
        tx = parts[2] if len(parts) > 2 else "Вход в аккаунт 👆"
        
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur: r = await cur.fetchone()
        
        if r:
            await bot.send_photo(chat_id=r[0], photo=m.photo[-1].file_id, caption=f"🔔 **SMS / КОД**\n📱 `{ph}`\n💬 {tx}", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else: await m.reply(f"🚫 Номер {ph} не в работе.")
    except: await m.reply("❌ Ошибка отправки.")

@router.message(Command("sms"))
async def sms_text_handler(m: types.Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply("⚠️ Формат: /sms номер код")
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

# --- ADMIN: SETUP GROUP & SWITCH ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    
    if not rows: return await message.answer("❌ Нет тарифов.")
    
    kb = [[InlineKeyboardButton(text=f"📌 {r[0]}", callback_data=f"set_group_{r[0]}")] for r in rows]
    await message.answer("⚙️ **Настройка группы**\nВыберите глобальный тариф для этого чата:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")

@router.callback_query(F.data.startswith("set_group_"))
async def set_group_tariff(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_group_")[1]
    key = f"group_cfg_{c.message.chat.id}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name))
        await db.commit()
    
    await c.message.delete()
    # ТУТОРИАЛ
    await c.message.answer(
        f"✅ **Группа привязана!**\n💎 Тариф: **{t_name}**\n\n"
        f"📋 **ПАМЯТКА ВОРКЕРА:**\n"
        f"1️⃣ Взять номер: `/num`\n"
        f"2️⃣ Отправить код: `/sms номер код`\n"
        f"3️⃣ Скрин/QR: фото с подписью `/sms номер текст`", 
        parse_mode="Markdown"
    )

@router.message(Command("num"))
async def cmd_num(message: types.Message):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM config WHERE key=?", (f"group_cfg_{message.chat.id}",)) as cur: t_res = await cur.fetchone()
        if not t_res: return 
        
        async with db.execute("SELECT id, user_id, phone, tariff_price FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY created_at ASC LIMIT 1", (t_res[0],)) as cur:
            row = await cur.fetchone()
        
        if not row: return await message.reply("📭 Очередь пуста!")
        
        await db.execute("UPDATE numbers SET status='work', worker_id=?, start_time=? WHERE id=?", 
                         (message.from_user.id, datetime.now(timezone.utc).isoformat(), row[0]))
        await db.commit()
    
    await message.answer(
        f"🚀 **В РАБОТЕ**\n📱 `{row[2]}`\n💰 Тариф: {t_res[0]}", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Встал ✅", callback_data=f"w_act_{row[0]}"), 
            InlineKeyboardButton(text="Ошибка ❌", callback_data=f"w_err_{row[0]}")
        ]]), parse_mode="Markdown"
    )

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
        async with db.execute("SELECT phone, user_id, start_time, tariff_price, tariff_name FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
            
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", (st, now_iso, nid))
        
        if st == "drop":
            async with db.execute("SELECT referrer_id FROM users WHERE user_id=?", (res[1],)) as cur: ref_data = await cur.fetchone()
            if ref_data and ref_data[0]:
                reward = round(extract_price_float(res[3]) * REF_PERCENT, 3)
                if reward > 0:
                    try: await bot.send_message(ref_data[0], f"💰 **Реферальный бонус!**\n+ {reward}$ (5%) за номер `{res[0]}`", parse_mode="Markdown")
                    except: pass
        await db.commit()
    
    status_text = "СЛЕТ (Ожидает выплаты)" if st == "drop" else "ОШИБКА"
    await c.message.edit_text(f"🏁 Финал: {status_text}\n📱 {res[0]}")
    
    try:
        dur = calculate_duration(res[2], now_iso)
        msg = (f"📉 **НОМЕР СЛЕТЕЛ!**\n📱 `{res[0]}`\n⏱ Время: {dur}\n💰 Статус: **Ожидает выплату**" if st=="drop" else f"❌ **ОШИБКА**\n📱 `{res[0]}`\nПричина: Нет кода/ошибка.")
        await bot.send_message(res[1], msg, parse_mode="Markdown")
    except: pass

# --- USER COMMANDS ---
@router.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    if await is_banned(message.from_user.id): return
    
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
        # КРАСИВОЕ ПРИВЕТСТВИЕ
        welcome_text = (
            f"👋 **Привет, {message.from_user.first_name}!**\n\n"
            f"🚀 **FAST TEAM** — лучший сервис скупа.\n"
            f"🛡 Гарантия выплат и скорости.\n\n"
            f"👇 Жми меню для работы!"
        )
        await message.answer(welcome_text, reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    else: await message.answer("⏳ Доступ закрыт.")

@router.callback_query(F.data == "menu_profile")
async def show_profile(c: CallbackQuery, bot: Bot):
    uid = c.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={uid}"

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*), SUM(CAST(tariff_price AS REAL)) FROM numbers WHERE user_id=? AND status='drop'", (uid,)) as cur: stats = await cur.fetchone()
        async with db.execute("SELECT COUNT(*) FROM users WHERE referrer_id=?", (uid,)) as cur: ref_count = (await cur.fetchone())[0]
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (uid,)) as cur: reg = (await cur.fetchone())[0].split('T')[0]

    text = (f"👤 **Профиль**\n🆔 `{uid}`\n📅 Рег: {reg}\n\n"
            f"👥 Рефералов: **{ref_count}**\n🔗 `{ref_link}`\n\n"
            f"📊 **Стат:**\n✅ Сдано: **{stats[0] or 0}**\n💰 Баланс: **{stats[1] or 0.0}$**")
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
    if queue_rows: kb.append([InlineKeyboardButton(text="🗑 Отменить (Очередь)", callback_data="cancel_queue_menu")])
    kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="my_numbers_menu")])
    kb.append([InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")])
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data == "cancel_queue_menu")
async def cancel_queue_menu(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, phone FROM numbers WHERE user_id=? AND status='queue'", (uid,)) as cur: rows = await cur.fetchall()
    if not rows: return await c.answer("Нечего отменять.")
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

# --- TARIFF & ADD NUMBER ---
@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    if await is_banned(c.from_user.id): return await c.answer("🚫 Вы забанены")
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
    await c.message.edit_text("📱 **Введите номера (списком):**\n(Мусор удалится автоматически)", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]]))
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    if not await check_work_status():
        await state.clear()
        return await message.answer("⛔️ Ворк на паузе.")
    
    d = await state.get_data()
    raw = message.text.replace(',', '\n').split('\n')
    added = 0
    async with aiosqlite.connect(DB_NAME) as db:
        for line in raw:
            p = clean_phone(line.strip()) # АВТО-ОЧИСТКА
            if p:
                await db.execute("INSERT INTO numbers (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)", 
                                 (message.from_user.id, p, d['method'], d['tariff_name'], d['tariff_price'], d['tariff_hold'], datetime.now(timezone.utc).isoformat()))
                added += 1
        await db.commit()
    await message.answer(f"✅ Принято: **{added}**", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")
    await state.clear()

@router.callback_query(F.data == "nav_main")
async def nav_main(c: CallbackQuery): await show_profile(c, c.message.bot)

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    is_work = await check_work_status()
    kb = [
        [InlineKeyboardButton(text=f"Ворк: {'🟢' if is_work else '🔴'}", callback_data="adm_toggle_work")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="➕ Тариф", callback_data="adm_trf_add"), InlineKeyboardButton(text="🗑 Тариф", callback_data="adm_trf_del_menu")],
        [InlineKeyboardButton(text="📄 ОТЧЕТ (CSV)", callback_data="adm_report")],
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

# --- BROADCAST ---
@router.callback_query(F.data == "admin_broadcast")
async def broadcast_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("📢 **Отправьте пост для рассылки:**\n(Текст/Фото/Видео)", parse_mode="Markdown")
    await state.set_state(AdminState.waiting_for_broadcast)

@router.message(AdminState.waiting_for_broadcast)
async def broadcast_send(message: types.Message, state: FSMContext, bot: Bot):
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cur: users = await cur.fetchall()
    
    cnt = 0
    await message.answer(f"🚀 Запуск на {len(users)}...")
    for u in users:
        try:
            await bot.copy_message(u[0], message.chat.id, message.message_id)
            cnt += 1
            await asyncio.sleep(0.05)
        except: pass
    await message.answer(f"✅ Ушло: {cnt}")

# --- REPORT (CSV) ---
@router.callback_query(F.data == "adm_report")
async def adm_report(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""SELECT n.phone, n.start_time, n.end_time, u.username, n.tariff_name, n.tariff_price, n.tariff_hold, n.status 
                            FROM numbers n JOIN users u ON n.user_id=u.user_id""")
        rows = await (await db.execute("""SELECT n.phone, n.start_time, n.end_time, u.username, n.tariff_name, n.tariff_price, n.tariff_hold, n.status 
                            FROM numbers n JOIN users u ON n.user_id=u.user_id""")).fetchall()
    
    if not rows: return await c.answer("Пусто")
    
    # Генерация CSV как в примере
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    # Заголовки как в твоем файле
    writer.writerow(["Number", "Duration", "Username", "Rent Group", "Thread", "Start Time", "End Time", "Hold Time", "Hold Price", "Break?", "Stopped?"])
    
    for r in rows:
        dur = calculate_duration(r[1], r[2])
        # Mapping данных в колонки
        writer.writerow([
            r[0], # Number
            dur,  # Duration
            r[3] or "NoUser", # Username
            r[4], # Rent Group (Tariff)
            "Main", # Thread
            r[1] or "-", # Start
            r[2] or "-", # End
            r[6], # Hold info
            r[5], # Price
            "No", # Break
            "Yes" if r[7] == 'dead' else "No" # Stopped
        ])
    
    buffer.seek(0)
    # Превращаем в байты для отправки
    b_data = buffer.getvalue().encode('utf-8-sig') # BOM для Excel
    await c.message.answer_document(BufferedInputFile(b_data, filename=f"report_{datetime.now().strftime('%Y%m%d')}.csv"), caption="📄 CSV Отчет готов")

# --- BAN SYSTEM ---
@router.message(Command("ban"))
async def cmd_ban(message: types.Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID or not command.args: return
    uid = int(command.args)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
        await db.commit()
    await message.reply(f"🚫 {uid} ЗАБАНЕН.")

@router.message(Command("unban"))
async def cmd_unban(message: types.Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID or not command.args: return
    uid = int(command.args)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (uid,))
        await db.commit()
    await message.reply(f"✅ {uid} РАЗБАНЕН.")

# --- TARIFF ADMIN (ADD/DEL) ---
@router.callback_query(F.data == "adm_trf_add")
async def adm_trf_add(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📝 Имя (ex: RU WA):")
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
        try: await bot.send_message(uid, "✅ Доступ открыт! Нажми /start")
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
