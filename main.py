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
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, ReactionTypeEmoji, BufferedInputFile
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from aiogram.exceptions import TelegramForbiddenError
except ImportError:
    sys.exit("❌ pip install aiogram aiosqlite")

TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))
DB_NAME = "bot_v85.db"

AFK_CHECK_MINUTES = 8
AFK_KICK_MINUTES = 3
CODE_WAIT_MINUTES = 4

E = {'fire':'🔥','phone':'📱','check':'✅','cross':'❌','clock':'⏰','money':'💰','box':'📦','user':'👤','admin':'⚡','help':'🆘','info':'ℹ️','queue':'🟡','active':'🟢','stop':'🛑','office':'🏢','stats':'📊'}
SEP = "━━━━━━━━━━━━━━━━━━━━"
SEP_M = "─────────────────"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)
router = Router()

@asynccontextmanager
async def get_db():
    conn = await aiosqlite.connect(DB_NAME, timeout=30)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    try: yield conn
    finally: await conn.close()

async def init_db():
    async with get_db() as db:
        await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, is_approved INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0, reg_date TEXT DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS numbers (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, tariff_name TEXT, tariff_price TEXT, status TEXT DEFAULT 'queue', worker_id INTEGER DEFAULT 0, worker_chat_id INTEGER DEFAULT 0, worker_thread_id INTEGER DEFAULT 0, start_time TEXT, end_time TEXT, last_ping TEXT, wait_code_start TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS tariffs (name TEXT PRIMARY KEY, price TEXT, hold_time TEXT DEFAULT '20 мин')")
        await db.execute("CREATE TABLE IF NOT EXISTS groups (group_num INTEGER PRIMARY KEY, chat_id INTEGER, title TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('WhatsApp', '50₽', '20 мин')")
        await db.execute("INSERT OR IGNORE INTO tariffs VALUES ('MAX', '10$', '1 час')")
        await db.commit()
    logger.info("✅ DB v85.0")

def clean_phone(phone):
    clean = re.sub(r'[^\d]', '', str(phone))
    if clean.startswith('77') and len(clean)==11: return '+'+clean
    if clean.startswith('8') and len(clean)==11: clean='7'+clean[1:]
    elif len(clean)==10: clean='7'+clean
    return '+'+clean if re.match(r'^7\d{10}$',clean) else None

def mask_phone(phone, uid):
    if uid==ADMIN_ID: return phone
    try: return f"{phone[:5]}***{phone[-4:]}" if len(phone)>=9 else phone
    except: return phone

def get_now(): return datetime.now(timezone.utc).isoformat()
def format_time(iso):
    try: return (datetime.fromisoformat(iso)+timedelta(hours=3)).strftime("%d.%m %H:%M")
    except: return "-"

def calc_duration(s,e):
    try:
        if not s or not e: return "0 мин"
        return f"{int((datetime.fromisoformat(e)-datetime.fromisoformat(s)).total_seconds()/60)} мин"
    except: return "0 мин"

class UserState(StatesGroup):
    waiting_numbers = State()
    waiting_support = State()

class AdminState(StatesGroup):
    waiting_broadcast = State()
    edit_hold = State()
    edit_price = State()
    support_reply = State()

def main_kb(uid):
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{E['phone']} Сдать номер", callback_data="sel_tariff")
    kb.button(text=f"{E['user']} Профиль", callback_data="profile")
    kb.button(text=f"{E['info']} Помощь", callback_data="guide")
    kb.button(text=f"{E['help']} Поддержка", callback_data="ask_supp")
    if uid==ADMIN_ID: kb.button(text=f"{E['admin']} Админ", callback_data="admin_main")
    kb.adjust(1,2,1,1)
    return kb.as_markup()

def worker_kb(nid, tariff):
    kb = InlineKeyboardBuilder()
    if "MAX" in tariff.upper():
        kb.button(text=f"{E['check']} Встал", callback_data=f"w_act_{nid}")
        kb.button(text="⏭ Пропуск", callback_data=f"w_skip_{nid}")
    else:
        kb.button(text=f"{E['check']} Встал", callback_data=f"w_act_{nid}")
        kb.button(text=f"{E['cross']} Ошибка", callback_data=f"w_err_{nid}")
    return kb.as_markup()

def worker_active_kb(nid):
    return InlineKeyboardBuilder().button(text="📉 Слет", callback_data=f"w_drop_{nid}").as_markup()

@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    async with get_db() as db:
        res = await (await db.execute("SELECT * FROM users WHERE user_id=?",(uid,))).fetchone()
        if not res:
            await db.execute("INSERT INTO users (user_id,username,first_name) VALUES (?,?,?)",(uid,m.from_user.username,m.from_user.first_name))
            await db.commit()
            if ADMIN_ID:
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"{E['check']} Принять",callback_data=f"acc_ok_{uid}"),InlineKeyboardButton(text=f"{E['cross']} Бан",callback_data=f"acc_no_{uid}")]])
                try: await m.bot.send_message(ADMIN_ID,f"👋 <b>Новая заявка</b>\n{SEP_M}\n🆔 <code>{uid}</code>\n👤 @{m.from_user.username}",reply_markup=kb,parse_mode="HTML")
                except: pass
            return await m.answer(f"🔒 <b>Доступ ограничен</b>\n{SEP}\n⏳ Ожидайте одобрения",parse_mode="HTML")
        if res['is_banned']: return await m.answer(f"{E['cross']} <b>Доступ заблокирован</b>",parse_mode="HTML")
        if res['is_approved']: await m.answer(f"👋 <b>Привет, {m.from_user.first_name}!</b>\n{SEP}\nВыберите действие:",reply_markup=main_kb(uid),parse_mode="HTML")
        else: await m.answer(f"{E['clock']} <b>Заявка на рассмотрении</b>",parse_mode="HTML")

@router.message(Command("bindgroup"))
async def cmd_bindgroup(m: Message, command: CommandObject):
    if m.from_user.id!=ADMIN_ID: return
    if not command.args: return await m.reply(f"{E['cross']} <b>Ошибка!</b>\n{SEP_M}\nИспользование: <code>/bindgroup 1</code>",parse_mode="HTML")
    try:
        gn = int(command.args.strip())
        if gn not in [1,2,3]: raise ValueError
    except: return await m.reply(f"{E['cross']} Номер: 1, 2 или 3")
    cid,title = m.chat.id, m.chat.title or f"Chat {m.chat.id}"
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO groups (group_num,chat_id,title) VALUES (?,?,?)",(gn,cid,title))
        await db.commit()
    await m.answer(f"{E['check']} <b>Группа {gn} привязана!</b>\n{SEP}\n{E['office']} {title}\n🆔 <code>{cid}</code>",parse_mode="HTML")

@router.message(Command("startwork"))
async def cmd_startwork(m: Message):
    if m.from_user.id!=ADMIN_ID: return
    async with get_db() as db:
        ts = await (await db.execute("SELECT name FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=t['name'],callback_data=f"bind_{t['name']}")
    kb.adjust(1)
    await m.answer(f"⚙️ <b>Настройка воркера</b>\n{SEP}\nВыберите тариф:",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.message(Command("stopwork"))
async def cmd_stopwork(m: Message, bot: Bot):
    if m.from_user.id!=ADMIN_ID: return
    cid = m.chat.id
    async with get_db() as db:
        group = await (await db.execute("SELECT * FROM groups WHERE chat_id=?",(cid,))).fetchone()
        if group:
            gn,title,st = group['group_num'],group['title'],get_now()
            nums = await (await db.execute("SELECT id,user_id,phone,start_time FROM numbers WHERE status IN ('work','active') AND worker_chat_id=?",(cid,))).fetchall()
            stopped = 0
            for num in nums:
                await db.execute("UPDATE numbers SET status=?,end_time=? WHERE id=?",(f"finished_group_{gn}",st,num['id']))
                stopped+=1
                try: await bot.send_message(num['user_id'],f"{E['stop']} <b>{title} остановлен</b>\n{SEP}\n{E['phone']} {mask_phone(num['phone'],num['user_id'])}\n{E['clock']} {format_time(st)}\n⏱ {calc_duration(num['start_time'],st)}",parse_mode="HTML")
                except: pass
            await db.commit()
            await m.answer(f"{E['stop']} <b>СТОП ВОРК</b>\n{SEP}\n{E['office']} {title}\n{E['box']} Остановлено: {stopped}",parse_mode="HTML")
        else:
            tid = m.message_thread_id if m.is_topic_message else 0
            await db.execute("DELETE FROM config WHERE key=?",(f"topic_{cid}_{tid}",))
            await db.commit()
            await m.reply(f"{E['stop']} <b>Топик отключен</b>",parse_mode="HTML")

@router.message(Command("num"))
async def cmd_num(m: Message, bot: Bot):
    cid = m.chat.id
    tid = m.message_thread_id if m.is_topic_message else 0
    async with get_db() as db:
        conf = await (await db.execute("SELECT value FROM config WHERE key=?",(f"topic_{cid}_{tid}",))).fetchone()
        if not conf: return await m.reply(f"{E['cross']} Топик не настроен")
        row = await (await db.execute("SELECT * FROM numbers WHERE status='queue' AND tariff_name=? ORDER BY id ASC LIMIT 1",(conf['value'],))).fetchone()
        if not row: return await m.reply(f"📭 <b>Очередь пуста</b>",parse_mode="HTML")
        await db.execute("UPDATE numbers SET status='work',worker_id=?,worker_chat_id=?,worker_thread_id=?,start_time=? WHERE id=?",(m.from_user.id,cid,tid,get_now(),row['id']))
        await db.commit()
    await m.answer(f"{E['fire']} <b>В работе</b>\n{SEP}\n{E['phone']} <code>{row['phone']}</code>\n💎 {row['tariff_name']}",reply_markup=worker_kb(row['id'],row['tariff_name']),parse_mode="HTML")
    try: await bot.send_message(row['user_id'],f"⚡ <b>Номер в работе</b>\n{SEP}\n{E['phone']} {mask_phone(row['phone'],row['user_id'])}\n⏳ Ожидайте код",parse_mode="HTML")
    except: pass

@router.message(Command("code"))
async def cmd_code(m: Message, command: CommandObject, bot: Bot):
    if not command.args: return await m.reply(f"⚠️ <b>Пример:</b> <code>/code +7999...</code>",parse_mode="HTML")
    ph = clean_phone(command.args.split()[0])
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')",(ph,))).fetchone()
    if not row or row['worker_id']!=m.from_user.id: return await m.reply(f"{E['cross']} Не ваш номер")
    async with get_db() as db:
        await db.execute("UPDATE numbers SET wait_code_start=? WHERE id=?",(get_now(),row['id']))
        await db.commit()
    try:
        await bot.send_message(row['user_id'],f"🔔 <b>ЗАПРОС КОДА</b>\n{SEP}\n{E['phone']} <code>{mask_phone(row['phone'],row['user_id'])}</code>\n👇 <b>Напишите код</b>",parse_mode="HTML")
        await m.reply(f"{E['check']} <b>Запрос отправлен</b>",parse_mode="HTML")
    except: await m.reply(f"{E['cross']} Ошибка")

@router.callback_query(F.data=="guide")
async def cb_guide(c: CallbackQuery):
    await c.message.edit_text(f"{E['info']} <b>Информация</b>\n{SEP}\n\n📲 <b>Что делает бот?</b>\nПринимаем номера WhatsApp/MAX\nВыплаты после проверки\n\n{E['box']} <b>Требования:</b>\n• Чистый активный номер\n• Доступ к SMS\n• Виртуальные номера {E['cross']}\n\n{E['clock']} <b>Холд и Выплаты:</b>\nДеньги после холда\n\n{SEP}",reply_markup=main_kb(c.from_user.id),parse_mode="HTML")

@router.callback_query(F.data=="profile")
async def cb_profile(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=?",(uid,))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE user_id=? AND status='active'",(uid,))).fetchone())[0]
        my_first = await (await db.execute("SELECT id FROM numbers WHERE user_id=? AND status='queue' ORDER BY id ASC LIMIT 1",(uid,))).fetchone()
        q_pos = 0
        if my_first: q_pos = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue' AND id<?",(my_first[0],))).fetchone())[0]+1
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 История",callback_data="my_nums")
    kb.button(text="🔙 Меню",callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(f"{E['user']} <b>Кабинет</b>\n{SEP}\n🆔 <code>{uid}</code>\n{E['box']} Всего: <b>{total}</b>\n{E['fire']} Активно: <b>{active}</b>\n{SEP_M}\n{E['queue']} Очередь: <b>{q_pos or '-'}</b>",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data=="my_nums")
async def cb_my_nums(c: CallbackQuery):
    uid = c.from_user.id
    async with get_db() as db:
        rows = await (await db.execute("SELECT id,phone,status,tariff_price FROM numbers WHERE user_id=? ORDER BY id DESC LIMIT 10",(uid,))).fetchall()
    kb = InlineKeyboardBuilder()
    txt = f"📝 <b>Последние 10</b>\n{SEP}\n"
    if not rows: txt+="📭 Пусто"
    else:
        for r in rows:
            icon = E['queue'] if r['status']=='queue' else E['active'] if r['status']=='active' else E['check'] if r['status']=='finished' else E['cross']
            txt+=f"{icon} <code>{mask_phone(r['phone'],uid)}</code> | {r['tariff_price']}\n"
            if r['status']=='queue': kb.button(text=f"🗑 {mask_phone(r['phone'],uid)}",callback_data=f"del_{r['id']}")
    kb.button(text="🔙 Назад",callback_data="profile")
    kb.adjust(1)
    await c.message.edit_text(txt,reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data.startswith("del_"))
async def cb_del(c: CallbackQuery):
    nid = c.data.split("_")[1]
    async with get_db() as db:
        row = await (await db.execute("SELECT status FROM numbers WHERE id=? AND user_id=?",(nid,c.from_user.id))).fetchone()
        if row and row['status']=='queue':
            await db.execute("DELETE FROM numbers WHERE id=?",(nid,))
            await db.commit()
            await c.answer(f"{E['check']} Удалено")
            await cb_my_nums(c)
        else: await c.answer(f"{E['cross']} Уже в работе!",show_alert=True)

@router.callback_query(F.data=="sel_tariff")
async def cb_sel_tariff(c: CallbackQuery):
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    if not ts: return await c.message.edit_text(f"{E['cross']} Тарифы не настроены!",reply_markup=main_kb(c.from_user.id),parse_mode="HTML")
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"{t['name']} | {t['price']} (Hold: {t['hold_time']})",callback_data=f"pick_{t['name']}")
    kb.button(text="🔙 Меню",callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(f"📂 <b>Выберите тариф</b>\n{SEP}",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data.startswith("pick_"))
async def cb_pick(c: CallbackQuery, state: FSMContext):
    tn = c.data.split("_")[1]
    async with get_db() as db:
        t = await (await db.execute("SELECT * FROM tariffs WHERE name=?",(tn,))).fetchone()
    await state.update_data(tariff=tn,price=t['price'])
    await state.set_state(UserState.waiting_numbers)
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена",callback_data="back_main")
    await c.message.edit_text(f"💎 <b>{tn}</b>\n{SEP}\n{E['money']} {t['price']}\n{E['clock']} Холд: {t['hold_time']}\n{SEP_M}\n{E['phone']} <b>Отправьте номера</b>",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data=="ask_supp")
async def cb_supp(c: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_support)
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена",callback_data="back_main")
    await c.message.edit_text(f"{E['help']} <b>Поддержка</b>\n{SEP}\nНапишите вопрос:",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data.startswith("bind_"))
async def cb_bind(c: CallbackQuery):
    tn = c.data.split("_")[1]
    cid,tid = c.message.chat.id, c.message.message_thread_id if c.message.is_topic_message else 0
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",(f"topic_{cid}_{tid}",tn))
        await db.commit()
    await c.message.edit_text(f"{E['check']} <b>Топик привязан!</b>\n{SEP}\nТариф: {tn}\nПиши /num",parse_mode="HTML")

@router.callback_query(F.data.startswith("w_act_"))
async def cb_w_act(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?",(nid,))).fetchone()
        if not row or row['worker_id']!=c.from_user.id: return await c.answer(f"{E['cross']} Не ты!",show_alert=True)
        await db.execute("UPDATE numbers SET status='active' WHERE id=?",(nid,))
        await db.commit()
    await c.message.edit_text(f"{E['check']} <b>Встал:</b> {row['phone']}",reply_markup=worker_active_kb(nid),parse_mode="HTML")
    try: await bot.send_message(row['user_id'],f"{E['check']} <b>Активирован!</b>\n{SEP}\nОжидайте выплату",parse_mode="HTML")
    except: pass

@router.callback_query(F.data.startswith("w_skip_"))
async def cb_w_skip(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?",(nid,))).fetchone()
        if not row or row['worker_id']!=c.from_user.id: return await c.answer(f"{E['cross']} Не ты!",show_alert=True)
        await db.execute("UPDATE numbers SET status='queue',worker_id=0,worker_chat_id=0 WHERE id=?",(nid,))
        await db.commit()
    await c.message.edit_text("⏭ <b>Пропуск</b>",parse_mode="HTML")

@router.callback_query(F.data.startswith(("w_drop_","w_err_")))
async def cb_w_finish(c: CallbackQuery, bot: Bot):
    nid = c.data.split("_")[2]
    is_drop = "drop" in c.data
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE id=?",(nid,))).fetchone()
        if not row or row['worker_id']!=c.from_user.id: return await c.answer(f"{E['cross']} Не ты!",show_alert=True)
        status = "finished" if is_drop else "dead"
        dur = calc_duration(row['start_time'],get_now())
        await db.execute("UPDATE numbers SET status=?,end_time=? WHERE id=?",(status,get_now(),nid))
        await db.commit()
    msg = f"📉 <b>Слет</b>\n⏱ {dur}" if is_drop else f"{E['cross']} <b>Ошибка</b>"
    await c.message.edit_text(msg,parse_mode="HTML")
    try: await bot.send_message(row['user_id'],msg,parse_mode="HTML")
    except: pass

@router.callback_query(F.data=="back_main")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text(f"👋 <b>Главное меню</b>\n{SEP}",reply_markup=main_kb(c.from_user.id),parse_mode="HTML")

@router.callback_query(F.data.startswith("acc_"))
async def cb_acc(c: CallbackQuery, bot: Bot):
    if c.from_user.id!=ADMIN_ID: return
    act,uid = c.data.split("_")[1], int(c.data.split("_")[2])
    async with get_db() as db:
        if act=="ok":
            await db.execute("UPDATE users SET is_approved=1 WHERE user_id=?",(uid,))
            await db.commit()
            await c.message.edit_text(f"{E['check']} Юзер {uid} принят")
            try: await bot.send_message(uid,f"{E['check']} <b>Доступ открыт!</b>\n/start",parse_mode="HTML")
            except: pass
        else:
            await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?",(uid,))
            await db.commit()
            await c.message.edit_text(f"{E['cross']} Юзер {uid} забанен")

@router.callback_query(F.data.startswith("afk_ok_"))
async def cb_afk(c: CallbackQuery):
    nid = c.data.split("_")[2]
    async with get_db() as db:
        await db.execute("UPDATE numbers SET last_ping=? WHERE id=?",(get_now(),nid))
        await db.commit()
    await c.message.delete()
    await c.answer(f"{E['check']} В очереди!")

@router.callback_query(F.data=="admin_main")
async def cb_adm(c: CallbackQuery):
    if c.from_user.id!=ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Тарифы",callback_data="adm_tariffs")
    kb.button(text=f"{E['stats']} Отчеты",callback_data="adm_reports")
    kb.button(text="📢 Рассылка",callback_data="adm_cast")
    kb.button(text=f"{E['office']} Группы",callback_data="manage_groups")
    kb.button(text="🔙 Меню",callback_data="back_main")
    kb.adjust(1)
    await c.message.edit_text(f"{E['admin']} <b>Админ панель</b>\n{SEP}",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data=="manage_groups")
async def cb_mgr(c: CallbackQuery):
    if c.from_user.id!=ADMIN_ID: return
    async with get_db() as db:
        groups = await (await db.execute("SELECT * FROM groups ORDER BY group_num")).fetchall()
    kb = InlineKeyboardBuilder()
    for i in range(1,4):
        gn = "Не привязана"
        for g in groups:
            if g['group_num']==i: gn=g['title']; break
        kb.button(text=f"{E['stop']} Стоп: {gn}",callback_data=f"stop_group_{i}")
    kb.button(text=f"{E['stats']} Статус",callback_data="groups_status")
    kb.button(text="🔙 Назад",callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text(f"{E['office']} <b>Управление</b>\n{SEP}\nВыберите группу:",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data.startswith("stop_group_"))
async def cb_stop_g(c: CallbackQuery, bot: Bot):
    if c.from_user.id!=ADMIN_ID: return
    gn = int(c.data.split("_")[-1])
    st = get_now()
    async with get_db() as db:
        g = await (await db.execute("SELECT * FROM groups WHERE group_num=?",(gn,))).fetchone()
        if not g: return await c.answer(f"{E['cross']} Группа {gn} не привязана!",show_alert=True)
        cid,title = g['chat_id'],g['title']
        nums = await (await db.execute("SELECT id,user_id,phone,start_time FROM numbers WHERE status IN ('work','active') AND worker_chat_id=?",(cid,))).fetchall()
        stopped = 0
        for num in nums:
            await db.execute("UPDATE numbers SET status=?,end_time=? WHERE id=?",(f"finished_group_{gn}",st,num['id']))
            stopped+=1
            try: await bot.send_message(num['user_id'],f"{E['stop']} <b>{title} остановлен</b>\n{SEP}\n{E['phone']} {mask_phone(num['phone'],num['user_id'])}\n{E['clock']} {format_time(st)}\n⏱ {calc_duration(num['start_time'],st)}",parse_mode="HTML")
            except: pass
        await db.commit()
    await c.message.edit_text(f"{E['stop']} <b>Группа {gn} остановлена</b>\n{SEP}\n{E['office']} {title}\n{E['clock']} {format_time(st)}\n{E['box']} {stopped}",parse_mode="HTML")

@router.callback_query(F.data=="groups_status")
async def cb_g_stat(c: CallbackQuery):
    async with get_db() as db:
        stats = {}
        for i in range(1,4): stats[f"Группа {i}"] = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status=?",(f"finished_group_{i}",))).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status IN ('work','active')")).fetchone())[0]
        queue = (await (await db.execute("SELECT COUNT(*) FROM numbers WHERE status='queue'")).fetchone())[0]
    txt = f"{E['stats']} <b>СТАТУС</b>\n{SEP}\n"
    for g,cnt in stats.items(): txt+=f"🏁 {g}: {cnt}\n"
    txt+=f"\n{E['fire']} Активно: {active}\n{E['queue']} Очередь: {queue}"
    kb = InlineKeyboardBuilder().button(text="🔙 Назад",callback_data="manage_groups")
    await c.message.edit_text(txt,reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data=="adm_tariffs")
async def cb_adm_t(c: CallbackQuery):
    if c.from_user.id!=ADMIN_ID: return
    async with get_db() as db:
        ts = await (await db.execute("SELECT * FROM tariffs")).fetchall()
    kb = InlineKeyboardBuilder()
    for t in ts: kb.button(text=f"✏️ {t['name']}",callback_data=f"ed_{t['name']}")
    kb.button(text="🔙 Назад",callback_data="admin_main")
    kb.adjust(1)
    await c.message.edit_text("🛠 <b>Выберите тариф:</b>",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data.startswith("ed_"))
async def cb_ed_t(c: CallbackQuery, state: FSMContext):
    if c.from_user.id!=ADMIN_ID: return
    target = c.data.split("_")[1]
    await state.update_data(target=target)
    await state.set_state(AdminState.edit_price)
    await c.message.edit_text(f"1️⃣ <b>ЦЕНА</b> для {target}\n{SEP_M}\nПример: <code>50₽</code>, <code>2$</code>",parse_mode="HTML")

@router.callback_query(F.data=="adm_reports")
async def cb_adm_r(c: CallbackQuery):
    if c.from_user.id!=ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    for h in [1,24,48]: kb.button(text=f"За {h}ч",callback_data=f"rep_{h}")
    kb.button(text="🔙 Назад",callback_data="admin_main")
    kb.adjust(3,1)
    await c.message.edit_text(f"{E['stats']} <b>Период:</b>",reply_markup=kb.as_markup(),parse_mode="HTML")

@router.callback_query(F.data.startswith("rep_"))
async def cb_rep(c: CallbackQuery):
    if c.from_user.id!=ADMIN_ID: return
    h = int(c.data.split("_")[1])
    ct = (datetime.now(timezone.utc)-timedelta(hours=h)).isoformat()
    async with get_db() as db:
        rows = await (await db.execute("SELECT n.*,g.title as group_name FROM numbers n LEFT JOIN groups g ON n.worker_chat_id=g.chat_id WHERE n.created_at>=? ORDER BY n.id DESC",(ct,))).fetchall()
    if not rows: return await c.answer("📂 Пусто")
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['ID','UserID','Phone','Status','Group','Tariff','Created','Start','End','Duration'])
    for r in rows:
        dur = calc_duration(r['start_time'],r['end_time'])
        gn = r['group_name'] if r['group_name'] else "-"
        w.writerow([r['id'],r['user_id'],r['phone'],r['status'],gn,r['tariff_name'],format_time(r['created_at']),format_time(r['start_time']),format_time(r['end_time']),dur])
    out.seek(0)
    await c.message.answer_document(BufferedInputFile(out.getvalue().encode(),filename=f"report_{h}h.csv"),caption=f"{E['stats']} Отчет за {h}ч")

@router.callback_query(F.data=="adm_cast")
async def cb_cast(c: CallbackQuery, state: FSMContext):
    if c.from_user.id!=ADMIN_ID: return
    await state.set_state(AdminState.waiting_broadcast)
    await c.message.edit_text("📢 <b>Пришлите пост:</b>",parse_mode="HTML")

@router.callback_query(F.data.startswith("reply_"))
async def cb_reply(c: CallbackQuery, state: FSMContext):
    if c.from_user.id!=ADMIN_ID: return
    uid = c.data.split("_")[1]
    await state.update_data(ruid=uid)
    await state.set_state(AdminState.support_reply)
    await c.message.answer(f"✍️ <b>Ответ для {uid}:</b>",parse_mode="HTML")

@router.message(UserState.waiting_numbers)
async def fsm_nums(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = re.split(r'[;,\n]',m.text)
    valid = [clean_phone(x.strip()) for x in raw if clean_phone(x.strip())]
    if not valid: return await m.reply(f"{E['cross']} <b>Не найдено номеров</b>",parse_mode="HTML")
    async with get_db() as db:
        for ph in valid: await db.execute("INSERT INTO numbers (user_id,phone,tariff_name,tariff_price,last_ping) VALUES (?,?,?,?,?)",(m.from_user.id,ph,data['tariff'],data['price'],get_now()))
        await db.commit()
    await state.clear()
    await m.answer(f"{E['check']} <b>Принято: {len(valid)}</b>\n{SEP}\nДобавлено в очередь",reply_markup=main_kb(m.from_user.id),parse_mode="HTML")

@router.message(UserState.waiting_support)
async def fsm_supp(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="💬 Ответить",callback_data=f"reply_{m.from_user.id}")
    try:
        await bot.send_message(ADMIN_ID,f"{E['help']} <b>Вопрос</b>\n{SEP}\n🆔 {m.from_user.id} (@{m.from_user.username})\n{SEP_M}\n{m.text}",reply_markup=kb.as_markup(),parse_mode="HTML")
        await m.answer(f"{E['check']} <b>Отправлено</b>\nАдмин ответит",reply_markup=main_kb(m.from_user.id),parse_mode="HTML")
    except Exception as e:
        logger.error(f"Supp: {e}")
        await m.answer(f"{E['cross']} Ошибка")

@router.message(AdminState.support_reply)
async def fsm_reply(m: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    try:
        await bot.send_message(data['ruid'],f"👨‍💻 <b>Ответ:</b>\n{SEP}\n{m.text}",parse_mode="HTML")
        await m.answer(f"{E['check']} Доставлено")
    except: await m.answer(f"{E['cross']} Ошибка")

@router.message(AdminState.waiting_broadcast)
async def fsm_cast(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    msg = await m.answer("⏳ <b>Рассылка...</b>",parse_mode="HTML")
    async with get_db() as db:
        users = await (await db.execute("SELECT user_id FROM users WHERE is_approved=1")).fetchall()
    success,fail = 0,0
    for u in users:
        try: await m.copy_to(u['user_id']); success+=1; await asyncio.sleep(0.05)
        except TelegramForbiddenError: fail+=1
        except: fail+=1
    await msg.edit_text(f"📢 <b>Завершено</b>\n{SEP}\n{E['check']} {success}\n{E['cross']} {fail}\n{E['box']} {len(users)}",parse_mode="HTML")

@router.message(AdminState.edit_price)
async def fsm_ep(m: Message, state: FSMContext):
    await state.update_data(price=m.text)
    await state.set_state(AdminState.edit_hold)
    await m.answer(f"2️⃣ <b>ХОЛД</b>\n{SEP_M}\nПример: <code>20 мин</code>, <code>1 час</code>",parse_mode="HTML")

@router.message(AdminState.edit_hold)
async def fsm_eh(m: Message, state: FSMContext):
    data = await state.get_data()
    async with get_db() as db:
        await db.execute("UPDATE tariffs SET price=?,hold_time=? WHERE name=?",(data['price'],m.text,data['target']))
        await db.commit()
    await state.clear()
    await m.answer(f"{E['check']} <b>Обновлено!</b>\n{SEP}\n{E['money']} {data['price']}\n{E['clock']} {m.text}",parse_mode="HTML")

@router.message(F.photo & F.caption)
async def handle_photo(m: Message, bot: Bot):
    if "/sms" not in m.caption.lower(): return
    ph = clean_phone(m.caption.split()[1]) if len(m.caption.split())>1 else None
    if not ph: return await m.reply("⚠️ /sms +7...")
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE phone=? AND status IN ('work','active')",(ph,))).fetchone()
    if not row or row['worker_id']!=m.from_user.id: return await m.reply(f"{E['cross']} Не ваш")
    try:
        await bot.send_photo(row['user_id'],m.photo[-1].file_id,caption=f"🔔 <b>СООБЩЕНИЕ</b>\n{SEP}",parse_mode="HTML")
        await m.react([ReactionTypeEmoji(emoji=E['fire'])])
    except: await m.reply(f"{E['cross']} Ошибка")

@router.message(F.chat.type=="private")
async def handle_msg(m: Message, bot: Bot, state: FSMContext):
    if m.text and m.text.startswith('/'): return
    if m.from_user.id==ADMIN_ID: return
    cs = await state.get_state()
    if cs:
        logger.info(f"Skip - state: {cs}")
        return
    async with get_db() as db:
        row = await (await db.execute("SELECT * FROM numbers WHERE user_id=? AND status IN ('work','active')",(m.from_user.id,))).fetchone()
    if row and row['worker_chat_id']:
        async with get_db() as db:
            await db.execute("UPDATE numbers SET wait_code_start=NULL WHERE id=?",(row['id'],))
            await db.commit()
        try:
            tc,tt = row['worker_chat_id'], row['worker_thread_id'] if row['worker_thread_id'] else None
            hdr = f"📩 <b>ОТВЕТ</b>\n{E['phone']} <code>{row['phone']}</code>\n{SEP}\n"
            if m.text: await bot.send_message(tc,message_thread_id=tt,text=f"{hdr}💬 {m.text}",parse_mode="HTML")
            elif m.photo: await bot.send_photo(tc,message_thread_id=tt,photo=m.photo[-1].file_id,caption=f"{hdr}📸",parse_mode="HTML")
            await m.answer(f"{E['check']} <b>Отправлено</b>",parse_mode="HTML")
        except: await m.answer(f"{E['cross']} Ошибка")

async def monitor(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            async with get_db() as db:
                waiters = await (await db.execute("SELECT id,user_id,phone,worker_chat_id,worker_thread_id,wait_code_start FROM numbers WHERE status='active' AND wait_code_start IS NOT NULL")).fetchall()
                for w in waiters:
                    st = datetime.fromisoformat(w['wait_code_start'])
                    if (now-st).total_seconds()/60>=CODE_WAIT_MINUTES:
                        await db.execute("UPDATE numbers SET status='dead',end_time=?,wait_code_start=NULL WHERE id=?",(get_now(),w['id']))
                        try:
                            await bot.send_message(w['user_id'],f"{E['clock']} <b>Время вышло</b>\n{w['phone']} отменен",parse_mode="HTML")
                            if w['worker_chat_id']: await bot.send_message(chat_id=w['worker_chat_id'],message_thread_id=w['worker_thread_id'] if w['worker_thread_id'] else None,text="⚠️ <b>Таймаут!</b>",parse_mode="HTML")
                        except: pass
                qrows = await (await db.execute("SELECT id,user_id,created_at,last_ping FROM numbers WHERE status='queue'")).fetchall()
                for r in qrows:
                    las = r['last_ping'] if r['last_ping'] else r['created_at']
                    if str(las).startswith("PENDING_"):
                        pt = datetime.fromisoformat(las.split("_")[1])
                        if (now-pt).total_seconds()/60>=AFK_KICK_MINUTES:
                            await db.execute("DELETE FROM numbers WHERE id=?",(r['id'],))
                            try: await bot.send_message(r['user_id'],f"{E['cross']} <b>Удален (AFK)</b>",parse_mode="HTML")
                            except: pass
                    else:
                        la = datetime.fromisoformat(las)
                        if (now-la).total_seconds()/60>=AFK_CHECK_MINUTES:
                            kb = InlineKeyboardBuilder().button(text="👋 Я тут!",callback_data=f"afk_ok_{r['id']}").as_markup()
                            try:
                                await bot.send_message(r['user_id'],f"⚠️ <b>Проверка!</b>\n{SEP}\nНажмите кнопку",reply_markup=kb,parse_mode="HTML")
                                await db.execute("UPDATE numbers SET last_ping=? WHERE id=?",(f"PENDING_{get_now()}",r['id']))
                            except: await db.execute("DELETE FROM numbers WHERE id=?",(r['id'],))
                await db.commit()
        except Exception as e:
            logger.error(f"Monitor: {e}")
            await asyncio.sleep(5)

async def main():
    await init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(monitor(bot))
    logger.info("🚀 BOT v85 COMPACT")
    try: await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__=="__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): logger.info("Stopped")
