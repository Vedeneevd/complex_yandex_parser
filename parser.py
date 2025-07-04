import asyncio
import os
import re
from typing import List, Dict
from collections import defaultdict
import pandas as pd
from io import BytesIO
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from dotenv import load_dotenv
from yandex_parser import SiteParser

# Загрузка переменных окружения
load_dotenv()

# Конфигурация бота
BOT_TOKEN = os.getenv("BOT_TOKEN")
MAX_CONCURRENT_REQUESTS = 3  # Максимальное количество одновременных запросов
MAX_URLS_PER_REQUEST = 10  # Максимальное количество URL в одном запросе

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Словари для управления состоянием
user_sessions = {}
active_requests = defaultdict(int)  # Счетчик активных запросов по пользователям
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Глобальный семафор

# Регулярное выражение для поиска URL
URL_PATTERN = re.compile(
    r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
)

# Смайлики для оформления
EMOJIS = {
    "start": "👋",
    "error": "❌",
    "success": "✅",
    "warning": "⚠️",
    "search": "🔍",
    "phone": "📞",
    "inn": "🔢",
    "money": "💰",
    "doc": "📊",
    "time": "⏳",
    "check": "✔️",
    "cancel": "✖️",
    "rocket": "🚀",
    "chart": "📈",
    "tada": "🎉",
    "thinking": "🤔",
    "wait": "⏱️",
    "queue": "📋"
}


def format_revenue(revenue_data: Dict[str, str]) -> str:
    """Форматирует информацию о выручке для вывода"""
    if not revenue_data:
        return f"{EMOJIS['warning']} Информация о выручке не найдена"

    return "\n".join(f"➖ ИНН {inn}: {revenue}" for inn, revenue in revenue_data.items())


def extract_urls(text: str) -> List[str]:
    """Извлекает URL из текста сообщения"""
    return URL_PATTERN.findall(text)


async def create_excel_report(data: List[Dict]) -> BufferedInputFile:
    """Создает Excel-файл с результатами парсинга"""
    rows = []
    for item in data:
        # Обработка всех комбинаций телефонов и ИНН
        for phone in item['phones']:
            for inn in item['inns']:
                rows.append({
                    'URL': item['url'],
                    'Телефон': phone,
                    'ИНН': inn,
                    'Выручка': item['revenues'].get(inn, 'Нет данных'),
                    'Статус': 'Пропущен' if item['skipped'] else 'Обработан'
                })

        # Обработка случаев, когда есть только телефоны или только ИНН
        if item['phones'] and not item['inns']:
            for phone in item['phones']:
                rows.append({
                    'URL': item['url'],
                    'Телефон': phone,
                    'ИНН': 'Не найден',
                    'Выручка': 'Нет данных',
                    'Статус': 'Пропущен' if item['skipped'] else 'Обработан'
                })

        if item['inns'] and not item['phones']:
            for inn in item['inns']:
                rows.append({
                    'URL': item['url'],
                    'Телефон': 'Не найден',
                    'ИНН': inn,
                    'Выручка': item['revenues'].get(inn, 'Нет данных'),
                    'Статус': 'Пропущен' if item['skipped'] else 'Обработан'
                })

    # Создание DataFrame и Excel-файла
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
        worksheet = writer.sheets['Sheet1']
        worksheet.set_column('A:A', 40)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 15)
        worksheet.set_column('D:D', 30)
        worksheet.set_column('E:E', 12)
        worksheet.autofilter(0, 0, 0, 4)

    return BufferedInputFile(output.getvalue(), filename="Результаты_анализа.xlsx")


@dp.message(Command("start"))
async def start_handler(message: Message):
    """Обработчик команды /start"""
    user_sessions[message.from_user.id] = {"state": "waiting_urls"}

    welcome_msg = f"""
{EMOJIS['start']} <b>Привет, {message.from_user.first_name}!</b> {EMOJIS['start']}

Я — твой помощник для анализа конкурентов от <b>Hot Clients</b> {EMOJIS['rocket']}

{EMOJIS['thinking']} <i>Мои возможности:</i>
• Поиск контактных телефонов {EMOJIS['phone']}
• Извлечение ИНН компаний {EMOJIS['inn']}
• Анализ финансовых показателей {EMOJIS['chart']}

{EMOJIS['success']} <b>Просто пришли мне ссылки на сайты</b> (до {MAX_URLS_PER_REQUEST} за раз)
"""
    await message.answer(welcome_msg, parse_mode="HTML")


async def process_urls(message: Message, urls: List[str]):
    """Основная функция обработки URL"""
    processing_msg = await message.answer(
        f"{EMOJIS['time']} <b>Анализирую {len(urls)} сайтов...</b>\n"
        f"{EMOJIS['search']} Это может занять 1-2 минуты...",
        parse_mode="HTML"
    )

    all_results = []
    try:
        with SiteParser() as parser:
            for i, url in enumerate(urls, 1):
                try:
                    contacts = parser.extract_contacts(url)
                    all_results.append(contacts)

                    if contacts['skipped']:
                        await message.answer(
                            f"{EMOJIS['cancel']} <b>Сайт пропущен:</b> {url}\n"
                            "<i>Причина:</i> в черном списке",
                            parse_mode="HTML"
                        )
                        continue

                    # Формируем отчет по сайту
                    site_report = [
                        f"\n{EMOJIS['check']} <b>Сайт #{i}:</b> <code>{url}</code>",
                        f"\n{EMOJIS['phone']} <b>Телефоны:</b>\n" + "\n".join(f"➖ {p}" for p in contacts['phones']) if
                        contacts['phones'] else f"\n{EMOJIS['warning']} Телефоны не найдены",
                        f"\n{EMOJIS['inn']} <b>ИНН:</b>\n" + "\n".join(f"➖ {inn}" for inn in contacts['inns']) if
                        contacts['inns'] else f"\n{EMOJIS['warning']} ИНН не найдены",
                        f"\n{EMOJIS['money']} <b>Финансовые данные:</b>\n" + format_revenue(contacts['revenues']) if
                        contacts['revenues'] else ""
                    ]

                    await message.answer("\n".join(site_report), parse_mode="HTML")
                    await asyncio.sleep(1)

                except Exception as e:
                    await message.answer(
                        f"{EMOJIS['error']} <b>Ошибка при обработке:</b> {url}\n"
                        f"<i>Подробности:</i> {str(e)}",
                        parse_mode="HTML"
                    )
                    continue

            # Отправка итогового отчета
            if all_results:
                excel_file = await create_excel_report(all_results)
                await message.answer_document(
                    excel_file,
                    caption=f"{EMOJIS['doc']} <b>Полный отчет готов!</b>",
                    parse_mode="HTML"
                )

    except Exception as e:
        await message.answer(
            f"{EMOJIS['error']} <b>Критическая ошибка:</b>\n"
            f"<code>{str(e)}</code>",
            parse_mode="HTML"
        )
    finally:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=processing_msg.message_id)
        except:
            pass


@dp.message()
async def message_handler(message: Message):
    """Основной обработчик сообщений"""
    user_id = message.from_user.id

    # Проверка начала диалога
    if user_id not in user_sessions:
        await message.answer(f"{EMOJIS['warning']} Пожалуйста, начните с команды /start")
        return

    # Проверка лимита запросов
    if active_requests[user_id] >= MAX_CONCURRENT_REQUESTS:
        await message.answer(
            f"{EMOJIS['wait']} <b>Достигнут лимит запросов!</b>\n\n"
            f"У меня сейчас {MAX_CONCURRENT_REQUESTS} активных запроса. "
            "Пожалуйста, дождитесь их завершения.",
            parse_mode="HTML"
        )
        return

    # Извлечение URL
    urls = extract_urls(message.text)
    if not urls:
        await message.answer(f"{EMOJIS['error']} Не найдено ссылок в сообщении!")
        return

    # Ограничение количества URL
    if len(urls) > MAX_URLS_PER_REQUEST:
        await message.answer(
            f"{EMOJIS['warning']} Принято первых {MAX_URLS_PER_REQUEST} из {len(urls)} ссылок")
        urls = urls[:MAX_URLS_PER_REQUEST]

    # Учет активного запроса
    active_requests[user_id] += 1

    try:
        async with request_semaphore:
            await process_urls(message, urls)
    finally:
        active_requests[user_id] = max(0, active_requests[user_id] - 1)
        if active_requests[user_id] == 0:
            del active_requests[user_id]


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())