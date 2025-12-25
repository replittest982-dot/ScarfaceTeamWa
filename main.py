import asyncio
import logging
import sys
import os
import re
import io
from datetime import datetime, timezone

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
DB_NAME = "fast_team_v27.db" 

# Логирование
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
        # Таблица юзеров
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            username TEXT, 
            first_name TEXT, 
            is_approved INTEGER DEFAULT 0, 
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        
        # Таблица номеров
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
            last_ping TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP)""")
            
        # Таблица тарифов
        await db.execute("""CREATE TABLE IF NOT EXISTS tariffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            name TEXT UNIQUE, 
            price TEXT, 
            hold_info TEXT)""")
            
        # Конфиг (привязки топиков)
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        
        await db.commit()
        logging.info("🚀 FAST TEAM BOT v27.2 HOTFIX STARTED & DB CONNECTED")

# --- UTILS ---
def clean_phone(phone: str):
    clean = re.sub(r'[^\d+]', '', str(phone))
    if clean.startswith('8') and len(clean) == 11: clean = '+7' + clean[1:]
    elif clean.startswith('7') and len(clean) == 11: clean = '+' + clean
    elif len(clean) == 10 and clean.isdigit(): clean = '+7' + clean
    return clean if re.match(r'^\+\d{10,15}$', clean) else None

# --- KEYBOARDS ---
def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton(text="📥 Сдать номер", callback_data="select_tariff")],
        [InlineKeyboardButton(text="📊 Очередь", callback_data="public_queue"), InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="menu_guide")]
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="⚡️ ADMIN PANEL", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def worker_initial_kb(num_id): 
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Встал ✅", callback_data=f"w_act_{num_id}"), 
        InlineKeyboardButton(text="Ошибка ❌", callback_data=f"w_err_{num_id}")
    ]])

def worker_active_kb(num_id):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📉 СЛЕТ", callback_data=f"w_drop_{num_id}")
    ]])

# --- WORKER: PHOTO & SMS HANDLERS ---
@router.message(F.photo)
async def sms_photo_handler(m: types.Message, bot: Bot):
    # Воркер кидает фото с подписью: /sms +79990000000 текст
    if not m.caption: return
    caption_clean = m.caption.strip()
    if not caption_clean.startswith("/sms"): return

    try:
        parts = caption_clean.split(None, 2)
        if len(parts) < 2: return await m.reply("⚠️ Формат: /sms номер текст")

        ph_raw = parts[1]
        tx = parts[2] if len(parts) > 2 else "Вход в аккаунт 👆"
        
        ph = clean_phone(ph_raw)
        if not ph: return await m.reply(f"❌ Неверный номер: {ph_raw}")

        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM numbers WHERE phone=? AND status IN ('work','active')", (ph,)) as cur:
                r = await cur.fetchone()
        
        if r:
            # Отправляем фото юзеру
            await bot.send_photo(chat_id=r[0], photo=m.photo[-1].file_id, caption=f"🔔 **SMS / КОД**\n📱 `{ph}`\n💬 {tx}", parse_mode="Markdown")
            await m.react([types.ReactionTypeEmoji(emoji="👍")])
        else:
            await m.reply(f"🚫 Номер {ph} не найден в работе.")
    except Exception as e:
        logging.error(f"Error photo: {e}")
        await m.reply("❌ Ошибка отправки фото.")

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

# --- WORKER FLOW & SETUP ---
@router.message(Command("startwork"))
async def worker_setup(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as c: rows = await c.fetchall()
    
    if not rows: return await message.answer("❌ Нет тарифов. Создайте их в админке.")
    
    kb = [[InlineKeyboardButton(text=f"📌 {r[0]}", callback_data=f"set_topic_{r[0]}")] for r in rows]
    await message.answer("⚙️ **Настройка привязки**\nВыберите тариф для этого чата:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")

@router.callback_query(F.data.startswith("set_topic_"))
async def set_topic(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t_name = c.data.split("set_topic_")[1]
    key = f"topic_cfg_{c.message.chat.id}_{c.message.message_thread_id if c.message.is_topic_message else 0}"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, t_name))
        await db.commit()
    
    # Удаляем меню выбора
    await c.message.delete()
    
    # Отправляем ТУТОРИАЛ (ИНСТРУКЦИЮ)
    tutorial_text = (
        f"✅ **Топик успешно привязан!**\n"
        f"💎 Тариф: **{t_name}**\n\n"
        f"📋 **ИНСТРУКЦИЯ ДЛЯ ВОРКЕРОВ:**\n"
        f"1️⃣ Взять номер: `/num`\n"
        f"2️⃣ Отправить код: `/sms номер код`\n"
        f"3️⃣ Если просят скрин/QR: кидайте фото с подписью `/sms номер текст`\n"
        f"⚠️ Нажимайте кнопки только на своих заявках!"
    )
    await c.message.answer(tutorial_text, parse_mode="Markdown")

@router.message(Command("stopwork"))
async def stop_work(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    key = f"topic_cfg_{message.chat.id}_{message.message_thread_id if message.is_topic_message else 0}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM config WHERE key=?", (key,))
        await db.commit()
    await message.answer("🛑 Топик отвязан. Работа остановлена.")

@router.message(Command("num"))
async def cmd_num(message: types.Message, bot: Bot):
    cid = message.chat.id
    tid = message.message_thread_id if message.is_topic_message else 0
    
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Проверяем привязку
        async with db.execute("SELECT value FROM config WHERE key=?", (f"topic_cfg_{cid}_{tid}",)) as cur:
            t_res = await cur.fetchone()
        if not t_res: return 
        
        # 2. Берем номер из очереди (самый старый)
        async with db.execute("SELECT id, user_id, phone, tariff_price FROM numbers WHERE status = 'queue' AND tariff_name = ? ORDER BY created_at ASC LIMIT 1", (t_res[0],)) as cur:
            row = await cur.fetchone()
        
        if not row: return await message.reply("📭 Очередь пуста!")
        
        # 3. Обновляем статус
        await db.execute("UPDATE numbers SET status='work', worker_id=? WHERE id=?", (message.from_user.id, row[0]))
        await db.commit()
    
    # 4. Выдаем номер
    await message.answer(
        f"🚀 **В РАБОТЕ**\n📱 `{row[2]}`\n💰 {t_res[0]} | {row[3]}", 
        reply_markup=worker_initial_kb(row[0]), 
        parse_mode="Markdown"
    )

@router.callback_query(F.data.startswith("w_act_"))
async def worker_activate(c: CallbackQuery):
    nid = c.data.split('_')[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE numbers SET status='active' WHERE id=?", (nid,))
        async with db.execute("SELECT phone FROM numbers WHERE id = ?", (nid,)) as cur:
            res = await cur.fetchone()
        await db.commit()
    
    # Заменяем сообщение на "СЛЕТ"
    await c.message.edit_text(f"📉 **СЛЕТ**\n📱 `{res[0]}`", reply_markup=worker_active_kb(nid), parse_mode="Markdown")

@router.callback_query(F.data.startswith("w_drop_") | F.data.startswith("w_err_"))
async def worker_fin(c: CallbackQuery, bot: Bot):
    nid = c.data.split('_')[2]
    # drop = успешно слетел, dead = ошибка
    st = "drop" if "drop" in c.data else "dead" 
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT phone, user_id FROM numbers WHERE id=?", (nid,)) as cur: 
            res = await cur.fetchone()
        await db.execute("UPDATE numbers SET status=?, end_time=? WHERE id=?", 
                         (st, datetime.now(timezone.utc).isoformat(), nid))
        await db.commit()
    
    status_text = "СЛЕТ (Ожидает выплаты)" if st == "drop" else "ОШИБКА"
    await c.message.edit_text(f"🏁 Финал: {status_text}\n📱 {res[0]}")
    
    # Уведомляем юзера
    try: 
        msg = f"📉 Номер {res[0]} успешно слетел! Ожидайте выплату." if st=="drop" else f"❌ Номер {res[0]} - Ошибка/Неверный код."
        await bot.send_message(res[1], msg)
    except: pass

# --- USER COMMANDS & HANDLERS ---
@router.message(CommandStart())
async def cmd_start(message: types.Message):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT is_approved FROM users WHERE user_id = ?", (message.from_user.id,)) as c: 
            res = await c.fetchone()
        
        if not res:
            await db.execute("INSERT INTO users (user_id, username, first_name, is_approved) VALUES (?, ?, ?, 0)", 
                             (message.from_user.id, message.from_user.username, message.from_user.first_name))
            await db.commit()
            try: await message.bot.send_message(ADMIN_ID, f"👤 Новый запрос: {message.from_user.id} (@{message.from_user.username})", 
                                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                                    InlineKeyboardButton(text="✅", callback_data=f"acc_ok_{message.from_user.id}"), 
                                                    InlineKeyboardButton(text="🚫", callback_data=f"acc_no_{message.from_user.id}")
                                                ]]))
            except: pass
            return await message.answer("🔒 Ожидайте подтверждения доступа.")
            
    if res[0] == 1: 
        await message.answer("👋 **Добро пожаловать в FAST TEAM**", parse_mode="Markdown", reply_markup=main_menu_kb(message.from_user.id))
    else: 
        await message.answer("⏳ Доступ закрыт.")

@router.callback_query(F.data == "menu_profile")
async def show_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*), SUM(CAST(tariff_price AS REAL)) FROM numbers WHERE user_id=? AND status='drop'", (uid,)) as cur:
            stats = await cur.fetchone()
            count = stats[0] or 0
            money = stats[1] or 0.0
        async with db.execute("SELECT reg_date FROM users WHERE user_id=?", (uid,)) as cur:
            u_data = await cur.fetchone()
            reg = u_data[0].split('T')[0] if u_data else "Unknown"

    text = (
        f"👤 **Профиль Воркера**\n"
        f"➖➖➖➖➖➖➖➖\n"
        f"🆔 ID: `{uid}`\n"
        f"📅 В команде с: {reg}\n\n"
        f"📊 **Твоя статистика:**\n"
        f"✅ Сдано: **{count}**\n"
        f"💰 Профит: **{money}$**"
    )
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(uid))

@router.callback_query(F.data == "menu_guide")
async def show_guide(c: CallbackQuery):
    text = (
        "ℹ️ **ПОМОЩЬ**\n\n"
        "**Как сдать номер?**\n"
        "1. Нажми кнопку **📥 Сдать номер**\n"
        "2. Выбери сервис (например, RU WA)\n"
        "3. Выбери способ (SMS или QR)\n"
        "4. Отправь номер (можно списком!)\n\n"
        "**Правила:**\n"
        "⚠️ Не кидай использованные номера.\n"
        "⚠️ Следи за статусом заявки."
    )
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(c.from_user.id))

@router.callback_query(F.data == "public_queue")
async def public_queue_view(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT tariff_name, COUNT(*) FROM numbers WHERE status='queue' GROUP BY tariff_name") as cur: 
            stats = await cur.fetchall()
    
    text = "📊 **Текущая очередь:**\n\n"
    if not stats:
        text += "📭 Очередь пуста. Заливай номера!"
    else:
        for t, count in stats:
            text += f"🔹 {t}: **{count}** шт.\n"
            
    kb = [[InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")]]
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# --- TARIFF & ADD NUMBER LOGIC ---
@router.callback_query(F.data == "select_tariff")
async def step_tariff(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM tariffs") as cur: rows = await cur.fetchall()
    
    if not rows: return await c.answer("🚫 Нет доступных тарифов.", show_alert=True)
    
    await c.message.edit_text("👇 **Выберите куда хотите сдать номер:**", parse_mode="Markdown")
    
    kb = []
    current_row = []
    for r in rows:
        current_row.append(InlineKeyboardButton(text=r[0], callback_data=f"trf_pick_{r[0]}"))
        if len(current_row) == 2:
            kb.append(current_row)
            current_row = []
    if current_row: kb.append(current_row)
    
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")])
    await c.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# --- !!! ФИКС ОШИБКИ ЗДЕСЬ !!! ---
@router.callback_query(F.data.startswith("trf_pick_"))
async def step_method(c: CallbackQuery, state: FSMContext):
    t_name = c.data.split('trf_pick_')[1]
    async with aiosqlite.connect(DB_NAME) as db:
        # ЗАМЕНИЛ 'as c' НА 'as cur', ЧТОБЫ НЕ БЫЛО ОШИБКИ
        async with db.execute("SELECT price, hold_info FROM tariffs WHERE name=?", (t_name,)) as cur: 
            res = await cur.fetchone()
            
    if not res: return await c.answer("Тариф удален")
    
    await state.update_data(tariff_name=t_name, tariff_price=res[0], tariff_hold=res[1])
    
    text = (
        f"💎 Тариф: **{t_name}**\n"
        f"💵 Цена: **{res[0]}$**\n"
        f"⏳ Холд: **{res[1]}**\n\n"
        f"👇 **Выберите способ:**"
    )
    
    kb = [
        [InlineKeyboardButton(text="✉️ SMS", callback_data="input_sms"), InlineKeyboardButton(text="📸 QR", callback_data="input_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="select_tariff")]
    ]
    await c.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.in_({"input_sms", "input_qr"}))
async def step_input(c: CallbackQuery, state: FSMContext):
    await state.update_data(method='sms' if c.data == "input_sms" else 'qr')
    await c.message.edit_text(
        "📱 **Введите номера:**\n"
        "Можно отправить один или список (каждый с новой строки).\n\n"
        "Пример:\n`+79991234567`\n`89005553535`", 
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="nav_main")]])
    )
    await state.set_state(UserState.waiting_for_number)

@router.message(UserState.waiting_for_number)
async def receive_number(message: types.Message, state: FSMContext):
    if not message.text: return
    d = await state.get_data()
    
    # Пакетная обработка: разбиваем по переносу строки
    raw_lines = message.text.replace(',', '\n').split('\n')
    added = 0
    
    async with aiosqlite.connect(DB_NAME) as db:
        for line in raw_lines:
            p = clean_phone(line.strip())
            if p:
                await db.execute("""INSERT INTO numbers 
                    (user_id, phone, method, tariff_name, tariff_price, tariff_hold, status, last_ping) 
                    VALUES (?, ?, ?, ?, ?, ?, 'queue', ?)""", 
                    (message.from_user.id, p, d['method'], d['tariff_name'], d['tariff_price'], d['tariff_hold'], datetime.now(timezone.utc).isoformat()))
                added += 1
        await db.commit()
    
    await message.answer(f"✅ Принято номеров: **{added}**", parse_mode="Markdown", reply_markup=main_menu_kb(message.from_user.id))
    await state.clear()

@router.callback_query(F.data == "nav_main")
async def nav_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("👋 **Добро пожаловать в FAST TEAM**", parse_mode="Markdown", reply_markup=main_menu_kb(c.from_user.id))

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_panel_start")
async def adm_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    kb = [
        [InlineKeyboardButton(text="➕ Добавить тариф", callback_data="adm_trf_add"), InlineKeyboardButton(text="🗑 Удалить тариф", callback_data="adm_trf_del_menu")],
        [InlineKeyboardButton(text="📄 ОТЧЕТ (.txt)", callback_data="adm_report")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="nav_main")]
    ]
    await c.message.edit_text("⚡️ **ADMIN PANEL**", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data == "adm_report")
async def adm_report_gen(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT u.username, n.phone, n.end_time, n.status, n.tariff_price 
            FROM numbers n 
            JOIN users u ON n.user_id = u.user_id 
            ORDER BY n.id DESC
        """) as cur:
            rows = await cur.fetchall()
            
    if not rows: return await c.answer("База пуста")
    
    buffer = io.BytesIO()
    total_payout = 0.0
    
    lines = ["USER | PHONE | TIME | STATUS | PRICE"]
    lines.append("-" * 55)
    
    for r in rows:
        uname = r[0] or "NoUser"
        phone = r[1]
        time = r[2].split('T')[0] if r[2] else "N/A"
        status = r[3]
        price = float(r[4]) if r[4] else 0.0
        
        payout_mark = ""
        if status == "drop":
            total_payout += price
            payout_mark = "$"
        else:
            payout_mark = "NO"
            
        lines.append(f"{uname:<15} | {phone:<12} | {time} | {status} | {price}{payout_mark}")
    
    lines.append("-" * 55)
    lines.append(f"TOTAL PAYOUT: {total_payout}$")
    lines.append(f"TOTAL NUMBERS: {len(rows)}")
    
    buffer.write("\n".join(lines).encode('utf-8'))
    buffer.seek(0)
    
    file = BufferedInputFile(buffer.read(), filename=f"report_{datetime.now().strftime('%Y%m%d')}.txt")
    await c.message.answer_document(file, caption=f"📄 Отчет сформирован.\nК выплате: **{total_payout}$**", parse_mode="Markdown")

# --- ADMIN: ADD/DEL TARIFF ---
@router.callback_query(F.data == "adm_trf_add")
async def adm_trf_add_start(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📝 Название (например: RU WA):")
    await state.set_state(AdminState.trf_adding_name)

@router.message(AdminState.trf_adding_name)
async def adm_trf_name(m: types.Message, state: FSMContext):
    await state.update_data(name=m.text)
    await m.answer("💰 Цена (только число, пример: 2):")
    await state.set_state(AdminState.trf_adding_price)

@router.message(AdminState.trf_adding_price)
async def adm_trf_price(m: types.Message, state: FSMContext):
    await state.update_data(price=m.text)
    await m.answer("⏳ Холд (пример: 30 min):")
    await state.set_state(AdminState.trf_adding_hold)

@router.message(AdminState.trf_adding_hold)
async def adm_trf_final(m: types.Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute("INSERT INTO tariffs (name, price, hold_info) VALUES (?, ?, ?)", (d['name'], d['price'], m.text))
            await db.commit()
            await m.answer("✅ Тариф создан!", reply_markup=main_menu_kb(m.from_user.id))
        except:
            await m.answer("❌ Ошибка (возможно имя занято).")
    await state.clear()

@router.callback_query(F.data == "adm_trf_del_menu")
async def adm_trf_del_menu(c: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name FROM tariffs") as cur: rows = await cur.fetchall()
    kb = [[InlineKeyboardButton(text=f"❌ {r[1]}", callback_data=f"del_trf_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel_start")])
    await c.message.edit_text("🗑 Выбери тариф для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("del_trf_"))
async def adm_trf_del(c: CallbackQuery):
    tid = int(c.data.split("_")[2])
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM tariffs WHERE id=?", (tid,))
        await db.commit()
    await c.answer("Тариф удален!")
    await adm_trf_del_menu(c)

# --- ACCESS CONTROL ---
@router.callback_query(F.data.startswith("acc_"))
async def access_control(c: CallbackQuery, bot: Bot):
    if c.from_user.id != ADMIN_ID: return
    act, uid = c.data.split('_')[1], int(c.data.split('_')[2])
    if act == "ok":
        async with aiosqlite.connect(DB_NAME) as db: await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (uid,)); await db.commit()
        try: await bot.send_message(uid, "✅ Доступ открыт! Нажми /start")
        except: pass
        await c.message.edit_text(f"✅ OK {uid}")
    else: await c.message.edit_text(f"🚫 NO {uid}")

# --- MAIN ---
async def main():
    if not TOKEN: 
        print("❌ TOKEN not found")
        return
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
