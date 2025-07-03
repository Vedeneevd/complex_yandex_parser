import asyncio
import os
import re
from typing import List

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
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

    try:
        # Запускаем парсер
        with SiteParser() as parser:
            # Обрабатываем каждый сайт
            for i, url in enumerate(urls[:10], 1):  # Ограничиваем 10 сайтами за раз
                try:
                    contacts = parser.extract_contacts(url)

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