import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

from yandex_parser import YandexParser  # Импортируем ваш парсер

load_dotenv()

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Словарь для хранения состояния пользователей
user_sessions = {}


@dp.message(Command("start"))
async def start_handler(message: Message):
    user_id = message.from_user.id
    user_sessions[user_id] = {"state": "waiting_query"}

    await message.answer(
        "Привет! На связи Никита, твой помощник в выборе конкурентов для дальнейшего парсинга звонков с компанией Hot Clients.\n\n"
        "Пришли мне 1 слово или словосочетание по которому твои потенциальные клиенты ищут тебя в Яндексе 👇"
    )


def format_revenue(revenue_data: dict) -> str:
    """Форматирует информацию о выручке для вывода"""
    if not revenue_data:
        return "Информация о выручке не найдена"

    result = []
    for inn, revenue in revenue_data.items():
        result.append(f"- ИНН {inn}: {revenue}")
    return "\n".join(result)


@dp.message()
async def any_message_handler(message: Message):
    user_id = message.from_user.id

    # Если пользователь не начал диалог командой /start
    if user_id not in user_sessions:
        await message.answer("Пожалуйста, начните с команды /start")
        return

    # Если пользователь уже начал диалог, но еще не отправил запрос
    if user_sessions[user_id]["state"] == "waiting_query":
        query = message.text.strip()
        if not query:
            await message.answer("Пожалуйста, введите непустой запрос")
            return

        # Меняем состояние пользователя
        user_sessions[user_id]["state"] = "processing"

        # Отправляем сообщение о начале обработки
        processing_msg = await message.answer(f"🔍 Начинаю поиск по запросу: {query}\nЭто может занять 1-2 минуты...")

        try:
            # Запускаем парсер
            with YandexParser() as parser:
                links = parser.get_search_results(query)

                if not links:
                    await message.answer("По вашему запросу ничего не найдено 😕")
                    return

                # Отправляем общее количество результатов
                await message.answer(f"📊 Найдено {len(links)} результатов по запросу '{query}':")

                # Обрабатываем каждый сайт
                for i, url in enumerate(links[:10], 1):
                    try:
                        contacts = parser.extract_contacts(url)

                        if contacts['skipped']:
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
                        continue

                # Финальное сообщение
                await message.answer(
                    "Был рад помочь!\n\n"
                    "Если хочешь собрать конкурентов по другому запросу пришли мне 1 слово или словосочетание "
                    "по которому твои потенциальные клиенты ищут тебя в Яндексе 👇"
                )

        except Exception as e:
            await message.answer(f"Произошла ошибка при обработке запроса: {str(e)}")

        finally:
            # Удаляем сообщение о обработке
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=processing_msg.message_id)
            except:
                pass

            # Возвращаем пользователя в исходное состояние
            user_sessions[user_id] = {"state": "waiting_query"}


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())