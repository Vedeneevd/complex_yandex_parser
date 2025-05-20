import asyncio
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from yandex_parser import YandexParser  # Импортируем ваш парсер



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


@dp.message()
async def query_handler(message: Message):
    user_id = message.from_user.id

    # Проверяем, ожидаем ли мы запрос от пользователя
    if user_id not in user_sessions or user_sessions[user_id]["state"] != "waiting_query":
        await message.answer("Пожалуйста, начните с команды /start")
        return

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
        parser = YandexParser()
        try:
            links = parser.get_search_results(query)

            if not links:
                await message.answer("По вашему запросу ничего не найдено 😕")
                return

            # Отправляем результаты по частям (из-за ограничения длины в Telegram)
            result_text = f"📊 Найдено {len(links)} результатов по запросу '{query}':\n\n"
            part = []
            current_length = 0

            for i, url in enumerate(links[:10], 1):
                phones, inns = parser.extract_contacts(url)

                site_info = f"{i}. {url}\n"
                if phones:
                    site_info += "📞 Телефоны:\n" + "\n".join(f"- {p}" for p in phones) + "\n"
                else:
                    'Телефоны для этого сайта не найдены'
                if inns:
                    site_info += "🔢 ИНН:\n" + "\n".join(f"- {inn}" for inn in inns) + "\n"
                else:
                    'ИНН на этом сайте не найдены'
                site_info += "\n"



                # Разбиваем на сообщения по ~4000 символов
                if current_length + len(site_info) > 4000:
                    await message.answer("\n".join(part))
                    part = []
                    current_length = 0

                part.append(site_info)
                current_length += len(site_info)

            if part:
                await message.answer("\n".join(part))

            # Финальное сообщение
            await message.answer(
                "Был рад помочь!\n\n"
                "Если хочешь собрать конкурентов по другому запросу пришли мне 1 слово или словосочетание "
                "по которому твои потенциальные клиенты ищут тебя в Яндексе 👇"
            )

        finally:
            parser.close()

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