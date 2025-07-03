import asyncio
import os
import re
from typing import List
import pandas as pd
from io import BytesIO

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from dotenv import load_dotenv

from yandex_parser import SiteParser  # Импортируем наш парсер

load_dotenv()

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Словарь для хранения состояния пользователей
user_sessions = {}

# Регулярное выражение для поиска URL
URL_PATTERN = re.compile(
    r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
)


def format_revenue(revenue_data: dict) -> str:
    """Форматирует информацию о выручке для вывода"""
    if not revenue_data:
        return "Информация о выручке не найдена"

    result = []
    for inn, revenue in revenue_data.items():
        result.append(f"- ИНН {inn}: {revenue}")
    return "\n".join(result)


def extract_urls(text: str) -> List[str]:
    """Извлекает URL из текста сообщения"""
    return URL_PATTERN.findall(text)


def create_excel_report(data: list) -> bytes:
    """Создает Excel-файл с результатами парсинга и возвращает bytes"""
    # Подготовка данных для DataFrame
    rows = []
    for item in data:
        for phone in item['phones']:
            for inn in item['inns']:
                rows.append({
                    'URL': item['url'],
                    'Телефон': phone,
                    'ИНН': inn,
                    'Выручка': item['revenues'].get(inn, 'Нет данных'),
                    'Статус': 'Пропущен' if item['skipped'] else 'Обработан'
                })

        # Если есть телефоны, но нет ИНН
        if item['phones'] and not item['inns']:
            for phone in item['phones']:
                rows.append({
                    'URL': item['url'],
                    'Телефон': phone,
                    'ИНН': 'Не найден',
                    'Выручка': 'Нет данных',
                    'Статус': 'Пропущен' if item['skipped'] else 'Обработан'
                })

        # Если есть ИНН, но нет телефонов
        if item['inns'] and not item['phones']:
            for inn in item['inns']:
                rows.append({
                    'URL': item['url'],
                    'Телефон': 'Не найден',
                    'ИНН': inn,
                    'Выручка': item['revenues'].get(inn, 'Нет данных'),
                    'Статус': 'Пропущен' if item['skipped'] else 'Обработан'
                })

    # Создаем DataFrame
    df = pd.DataFrame(rows, columns=['URL', 'Телефон', 'ИНН', 'Выручка', 'Статус'])

    # Создаем Excel-файл в BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Результаты')

        # Получаем объект workbook и worksheet для форматирования
        workbook = writer.book
        worksheet = writer.sheets['Результаты']

        # Устанавливаем ширину столбцов
        worksheet.set_column('A:A', 40)  # URL
        worksheet.set_column('B:B', 20)  # Телефон
        worksheet.set_column('C:C', 15)  # ИНН
        worksheet.set_column('D:D', 30)  # Выручка
        worksheet.set_column('E:E', 12)  # Статус

        # Добавляем фильтры
        worksheet.autofilter(0, 0, 0, 4)

    return output.getvalue()


@dp.message(Command("start"))
async def start_handler(message: Message):
    user_id = message.from_user.id
    user_sessions[user_id] = {"state": "waiting_urls"}

    await message.answer(
        "Привет! На связи Никита, твой помощник в анализе конкурентов для парсинга звонков с компанией Hot Clients.\n\n"
        "Пришли мне ссылки на сайты конкурентов (одну или несколько), и я проанализирую их контактные данные."
    )


@dp.message()
async def any_message_handler(message: Message):
    user_id = message.from_user.id

    # Если пользователь не начал диалог командой /start
    if user_id not in user_sessions:
        await message.answer("Пожалуйста, начните с команды /start")
        return

    # Извлекаем URL из сообщения
    urls = extract_urls(message.text)
    if not urls:
        await message.answer("Не найдено ссылок в вашем сообщении. Пожалуйста, пришлите одну или несколько ссылок.")
        return

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(f"🔍 Начинаю анализ {len(urls)} сайтов... Это может занять 1-2 минуты...")

    all_results = []

    try:
        # Запускаем парсер
        with SiteParser() as parser:
            # Обрабатываем каждый сайт
            for i, url in enumerate(urls[:10], 1):  # Ограничиваем 10 сайтами за раз
                try:
                    contacts = parser.extract_contacts(url)
                    all_results.append(contacts)

                    if contacts['skipped']:
                        await message.answer(f"Сайт {url} пропущен (в черном списке)")
                        continue

                    # Формируем сообщение
                    site_info = f"\n{i}. <b>{url}</b>\n"

                    # Телефоны
                    if contacts['phones']:
                        site_info += "\n📞 <b>Телефоны:</b>\n" + "\n".join(
                            f"- {p}" for p in contacts['phones']) + "\n"
                    else:
                        site_info += "\nТелефоны для этого сайта не найдены\n"

                    # ИНН
                    if contacts['inns']:
                        site_info += "\n🔢 <b>ИНН:</b>\n" + "\n".join(f"- {inn}" for inn in contacts['inns']) + "\n"

                        # Выручка
                        if contacts['revenues']:
                            site_info += "\n💰 <b>Выручка:</b>\n" + format_revenue(contacts['revenues']) + "\n"
                    else:
                        site_info += "\nИНН на этом сайте не найдены\n"

                    # Отправляем сообщение
                    await message.answer(site_info, parse_mode="HTML")

                    # Небольшая задержка между сообщениями
                    await asyncio.sleep(1)

                except Exception as e:
                    print(f"Ошибка при обработке {url}: {str(e)}")
                    await message.answer(f"Ошибка при обработке сайта {url}")
                    continue

            # Создаем и отправляем Excel-файл
            if all_results:
                excel_data = create_excel_report(all_results)
                excel_file = BufferedInputFile(excel_data, filename="Результаты_анализа.xlsx")
                await message.answer_document(
                    excel_file,
                    caption="Вот результаты анализа в удобном формате"
                )

            # Финальное сообщение
            await message.answer(
                "Был рад помочь!\n\n"
                "Если хочешь проанализировать другие сайты, просто пришли мне ссылки на них."
            )

    except Exception as e:
        await message.answer(f"Произошла ошибка при обработке запроса: {str(e)}")

    finally:
        # Удаляем сообщение о обработке
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=processing_msg.message_id)
        except:
            pass


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())