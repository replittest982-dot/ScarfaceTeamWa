import os
import asyncio
import sqlite3
import random
import psutil
from datetime import datetime

# Библиотеки для работы с браузером
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException

# Библиотеки для Telegram
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from faker import Faker

# --- КОНФИГУРАЦИЯ ИНСТАНСА ---
# При запуске на хостинге укажите переменные окружения
INSTANCE_ID = os.getenv("INSTANCE_ID", "1") 
BOT_TOKEN = os.getenv("BOT_TOKEN", "ВАШ_ТОКЕН")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# ОГРАНИЧЕНИЕ: 1 браузер на 1 инстанс для максимальной скорости и экономии RAM
BROWSER_SEMAPHORE = asyncio.Semaphore(1) 
SESSION_DIR = "./sessions"
DB_PATH = "imperator_v16.db"

# Логирование
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(f"Imperator_Inst_{INSTANCE_ID}")
fake = Faker("ru_RU")

if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- FSM ---
class AddAccount(StatesGroup):
    waiting_for_phone = State()
    browser_active = State()

# --- БАЗА ДАННЫХ ---
def db_init():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Таблица аккаунтов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            phone_number TEXT UNIQUE,
            status TEXT DEFAULT 'pending',
            messages_sent INTEGER DEFAULT 0,
            user_agent TEXT,
            last_active DATETIME
        )
    """)
    # Таблица доступа (Whitelist)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS whitelist (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            approved BOOLEAN DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def is_approved(user_id):
    if user_id == ADMIN_ID: return True
    conn = sqlite3.connect(DB_PATH)
    res = conn.execute("SELECT approved FROM whitelist WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return res and res[0] == 1

def add_user_request(user_id, username):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT OR IGNORE INTO whitelist (user_id, username, approved) VALUES (?, ?, 0)", (user_id, username))
    conn.commit()
    conn.close()

def approve_user_db(user_id, status):
    conn = sqlite3.connect(DB_PATH)
    if status:
        conn.execute("UPDATE whitelist SET approved = 1 WHERE user_id = ?", (user_id,))
    else:
        conn.execute("DELETE FROM whitelist WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

# --- SELENIUM CORE (ОПТИМИЗИРОВАННЫЙ + FIX UI) ---
def get_driver(phone):
    options = Options()
    # Изолируем сессии для каждого инстанса
    user_data = os.path.join(os.getcwd(), "sessions", f"inst_{INSTANCE_ID}", phone)
    
    options.add_argument(f"--user-data-dir={user_data}")
    options.add_argument("--headless=new") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    # ВАЖНО: Фиксируем размер окна, чтобы появилась кнопка "Вход по номеру"
    options.add_argument("--window-size=1280,800")
    
    options.add_argument("--blink-settings=imagesEnabled=false") 
    options.page_load_strategy = 'eager' # Быстрая загрузка DOM
    
    driver = webdriver.Chrome(options=options)
    
    # KZ Stealth (Алматы)
    try:
        driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
            "latitude": 43.2389, "longitude": 76.8897, "accuracy": 100
        })
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Intl.DateTimeFormat.prototype.resolvedOptions = function() {
                    return { timeZone: 'Asia/Almaty', locale: 'ru-KZ' };
                };
            """
        })
    except:
        pass
    return driver

# Глобальный словарь активных драйверов для ручного управления
active_drivers = {}

# --- ТЕЛЕГРАМ БОТ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- КЛАВИАТУРЫ ---
def get_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить Аккаунт", callback_data="add_acc")],
        [InlineKeyboardButton(text="📊 Статус Инстанса", callback_data="status")]
    ])

def get_control_kb(phone):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📷 ЧЕК (Скрин)", callback_data=f"check_{phone}")],
        [InlineKeyboardButton(text="🔗 Вход по ссылке", callback_data=f"link_{phone}")],
        [InlineKeyboardButton(text="⌨️ Ввести номер", callback_data=f"type_{phone}")],
        [InlineKeyboardButton(text="✅ ГОТОВО", callback_data=f"ready_{phone}")]
    ])

# --- ЛОГИКА СТАРТА И ДОСТУПА ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "NoUsername"

    if is_approved(user_id):
        await message.answer(f"🚀 **Imperator v16.1 | Inst #{INSTANCE_ID}**\nДоступ разрешен.", reply_markup=get_main_kb())
        return

    add_user_request(user_id, username)
    await message.answer(f"⛔ **Вход заблокирован.**\nВаш ID: `{user_id}`\nЗапрос отправлен владельцу.")
    
    # Уведомление админу
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}")
        ]
    ])
    await bot.send_message(ADMIN_ID, f"👤 **Запрос доступа (Inst {INSTANCE_ID})**\nUser: @{username}\nID: `{user_id}`", reply_markup=kb)

# --- АДМИНСКИЕ КНОПКИ ---
@dp.callback_query(F.data.startswith("approve_"))
async def approve_handler(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    target_id = int(callback.data.split("_")[1])
    approve_user_db(target_id, True)
    await callback.message.edit_text(f"✅ ID {target_id} одобрен!")
    await bot.send_message(target_id, "✅ **Доступ открыт!** Жмите /start")

@dp.callback_query(F.data.startswith("reject_"))
async def reject_handler(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    target_id = int(callback.data.split("_")[1])
    approve_user_db(target_id, False)
    await callback.message.edit_text(f"❌ ID {target_id} отклонен.")

# --- ДОБАВЛЕНИЕ АККАУНТА ---
@dp.callback_query(F.data == "add_acc")
async def start_add(callback: CallbackQuery, state: FSMContext):
    if not is_approved(callback.from_user.id): return
    await callback.message.answer("Введите номер телефона (без +):")
    await state.set_state(AddAccount.waiting_for_phone)

@dp.message(AddAccount.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip().replace("+", "")
    await state.update_data(phone=phone)
    msg = await message.answer(f"⏳ Запуск браузера для {phone} (Inst {INSTANCE_ID})...")
    
    try:
        # Используем семафор, чтобы не нагружать инстанс
        async with BROWSER_SEMAPHORE:
            driver = await asyncio.to_thread(get_driver, phone)
            active_drivers[phone] = driver
            await asyncio.to_thread(driver.get, "https://web.whatsapp.com")
            
        await msg.edit_text(f"✅ Браузер открыт ({phone}).\nЖми 'Вход по ссылке', затем 'Ввести номер'.", reply_markup=get_control_kb(phone))
        await state.set_state(AddAccount.browser_active)
    except Exception as e:
        await msg.edit_text(f"Ошибка запуска: {str(e)[:100]}")

# --- РУЧНОЕ УПРАВЛЕНИЕ (FIXED) ---

@dp.callback_query(F.data.startswith("check_"))
async def do_check(callback: CallbackQuery):
    phone = callback.data.split("_")[1]
    driver = active_drivers.get(phone)
    if not driver: return await callback.answer("Браузер закрыт.")
    
    try:
        screenshot = await asyncio.to_thread(driver.get_screenshot_as_png)
        file = BufferedInputFile(screenshot, filename="screen.png")
        await callback.message.answer_photo(file, caption=f"Статус: {phone}")
        await callback.answer()
    except Exception as e:
        await callback.answer(f"Err: {str(e)[:50]}", show_alert=True)

@dp.callback_query(F.data.startswith("link_"))
async def do_link_click(callback: CallbackQuery):
    phone = callback.data.split("_")[1]
    driver = active_drivers.get(phone)
    if not driver: return
    
    try:
        # ОБНОВЛЕННЫЙ СПИСОК XPATH (Log in / Link / Связать)
        xpaths = [
            "//*[contains(text(), 'Log in with phone number')]",  # Точное совпадение со скрином
            "//*[contains(text(), 'Link with phone number')]",    # Старая версия
            "//*[contains(text(), 'Связать с номером телефона')]", # Русская версия
            "//span[@role='button' and contains(., 'phone')]"      # Поиск по кнопке
        ]
        
        found = False
        for xpath in xpaths:
            try:
                # Ищем элемент
                elements = driver.find_elements(By.XPATH, xpath)
                if elements:
                    el = elements[0]
                    # Пробуем JS клик (самый надежный)
                    driver.execute_script("arguments[0].scrollIntoView(true);", el)
                    await asyncio.sleep(0.5)
                    driver.execute_script("arguments[0].click();", el)
                    found = True
                    break
            except:
                continue
        
        if found:
            await callback.answer("✅ Нажато! Теперь жми 'Ввести номер'", show_alert=True)
        else:
            await callback.answer("❌ Кнопка не найдена. Попробуй обновить страницу (ЧЕК).", show_alert=True)
            
    except Exception as e:
        await callback.answer(f"Error: {str(e)[:100]}", show_alert=True)

@dp.callback_query(F.data.startswith("type_"))
async def do_type_number(callback: CallbackQuery):
    phone = callback.data.split("_")[1]
    driver = active_drivers.get(phone)
    if not driver: return
    
    try:
        # Ищем поле ввода. Оно появляется после нажатия "Log in with phone number"
        # Обычно это <input aria-label="Type your phone number.">
        script = f"""
            var input = document.querySelector('input[aria-label="Type your phone number."]') || document.querySelector('input[type="text"]');
            if (input) {{
                input.focus();
                input.value = "";
                document.execCommand('insertText', false, '{phone}');
                input.dispatchEvent(new Event('change', {{bubbles: true}}));
                return true;
            }} else {{
                return false;
            }}
        """
        success = driver.execute_script(script)
        
        if success:
            await callback.answer("✅ Номер введен! Жми 'Далее' на экране.", show_alert=True)
            # Пытаемся нажать кнопку NEXT
            await asyncio.sleep(1)
            driver.execute_script("""
                var btns = document.querySelectorAll('div[role="button"]');
                for (var i=0; i<btns.length; i++) {
                    if (btns[i].innerText.includes("Next") || btns[i].innerText.includes("Далее")) {
                        btns[i].click();
                        break;
                    }
                }
            """)
        else:
            await callback.answer("❌ Поле ввода не найдено. Сначала нажми 'Вход по ссылке'", show_alert=True)
            
    except Exception as e:
        await callback.answer(f"Err: {str(e)[:100]}", show_alert=True)

@dp.callback_query(F.data.startswith("ready_"))
async def do_ready(callback: CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[1]
    # Сохраняем в базу
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT OR REPLACE INTO accounts (user_id, phone_number, status, last_active) VALUES (?, ?, 'active', ?)",
                 (callback.from_user.id, phone, datetime.now()))
    conn.commit()
    conn.close()
    
    # Закрываем браузер (он перезапустится в фарм-лупе)
    if phone in active_drivers:
        d = active_drivers.pop(phone)
        try:
            d.quit()
        except: pass
        
    await callback.message.answer(f"🎉 Аккаунт {phone} добавлен в базу фарма!")
    await state.clear()

# --- ФАРМ ПРОЦЕССОР (MULTI-INSTANCE) ---
async def farm_loop():
    logger.info(f"FARM LOOP STARTED FOR INSTANCE {INSTANCE_ID}")
    while True:
        await asyncio.sleep(45) # Пауза между проверками
        
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            
            # РАСПРЕДЕЛЕНИЕ: (ID % 3) == (INST_ID - 1)
            # Если у тебя 3 бота, они поделят базу. Если 1 бот, INSTANCE_ID=1 берет всё (id % 1 == 0)
            # Для надежности просто берем случайный, который не активен более 10 мин
            cur.execute("""
                SELECT phone_number FROM accounts 
                WHERE status='active' 
                ORDER BY last_active ASC LIMIT 1
            """)
            target = cur.fetchone()
            conn.close()
            
            if target:
                phone = target[0]
                # Проверка: не занят ли аккаунт ручным добавлением
                if phone in active_drivers:
                    continue
                    
                async with BROWSER_SEMAPHORE:
                    await run_farm_session(phone)
                    
        except Exception as e:
            logger.error(f"Farm Loop Error: {e}")

async def run_farm_session(phone):
    driver = None
    try:
        logger.info(f"Farming: {phone}")
        driver = await asyncio.to_thread(get_driver, phone)
        await asyncio.to_thread(driver.get, "https://web.whatsapp.com")
        
        # Ожидание элемента чата
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[@contenteditable='true'] | //span[@data-icon='chat']"))
            )
            logger.info(f"Loaded: {phone}")
        except TimeoutException:
            logger.warning(f"Timeout: {phone}")
            return # Выходим, если не прогрузилось

        # Имитация активности
        await asyncio.sleep(random.randint(10, 20))
        
        # Обновляем БД
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE accounts SET last_active=? WHERE phone_number=?", (datetime.now(), phone))
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Session Error {phone}: {e}")
    finally:
        if driver:
            try:
                await asyncio.to_thread(driver.quit)
            except: pass

# --- MAIN ---
async def main():
    db_init()
    asyncio.create_task(farm_loop())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
