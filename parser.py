import asyncio
import os
import re
from typing import List, Dict, Set, Optional
from collections import defaultdict
import pandas as pd
from io import BytesIO
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from yandex_parser import SiteParser

# Загрузка переменных окружения
load_dotenv()


class Config:
    """Конфигурация бота и безопасность"""
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS: Set[int] = set(map(int, os.getenv("ADMIN_IDS", "").split(",")))
    ALLOWED_USER_IDS: Set[int] = set(map(int, os.getenv("ALLOWED_USER_IDS", "").split(",")))
    MAX_CONCURRENT_REQUESTS: int = 3
    MAX_URLS_PER_REQUEST: int = 10
    BLACKLISTED_DOMAINS: Set[str] = {"example.com", "test.com"}


class Emojis:
    """Все emoji для оформления сообщений"""
    START = "👋"
    ERROR = "❌"
    SUCCESS = "✅"
    WARNING = "⚠️"
    SEARCH = "🔍"
    PHONE = "📞"
    INN = "🔢"
    MONEY = "💰"
    DOC = "📊"
    TIME = "⏳"
    CHECK = "✔️"
    CANCEL = "✖️"
    ROCKET = "🚀"
    CHART = "📈"
    TADA = "🎉"
    THINKING = "🤔"
    WAIT = "⏱️"
    QUEUE = "📋"
    LOCK = "🔒"
    USER_ADD = "👤➕"
    USER_REMOVE = "👤➖"
    LIST = "📜"
    INFO = "ℹ️"


class UserManager:
    """Управление пользователями и правами доступа"""

    @staticmethod
    async def is_admin(user_id: int) -> bool:
        return user_id in Config.ADMIN_IDS

    @staticmethod
    async def is_allowed(user_id: int) -> bool:
        return user_id in Config.ALLOWED_USER_IDS or await UserManager.is_admin(user_id)

    @staticmethod
    async def add_user(user_id: int) -> bool:
        if user_id not in Config.ALLOWED_USER_IDS:
            Config.ALLOWED_USER_IDS.add(user_id)
            return True
        return False

    @staticmethod
    async def remove_user(user_id: int) -> bool:
        if user_id in Config.ALLOWED_USER_IDS and user_id not in Config.ADMIN_IDS:
            Config.ALLOWED_USER_IDS.remove(user_id)
            return True
        return False

    @staticmethod
    async def get_users_list() -> str:
        users = "\n".join(f"• {uid}" for uid in Config.ALLOWED_USER_IDS)
        admins = "\n".join(f"• {uid} (admin)" for uid in Config.ADMIN_IDS)
        return f"Администраторы:\n{admins}\n\nПользователи:\n{users}"


class ParserTools:
    """Инструменты для парсинга и обработки данных"""
    URL_PATTERN = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )

    @classmethod
    def extract_urls(cls, text: str) -> List[str]:
        return cls.URL_PATTERN.findall(text)

    @staticmethod
    async def format_revenue(revenue_data: Dict[str, str]) -> str:
        if not revenue_data:
            return f"{Emojis.WARNING} Информация о выручке не найдена"
        return "\n".join(f"➖ ИНН {inn}: {revenue}" for inn, revenue in revenue_data.items())

    @staticmethod
    async def create_excel_report(data: List[Dict]) -> BufferedInputFile:
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


class CompetitorAnalyzerBot:
    """Основной класс бота для анализа конкурентов"""

    def __init__(self):
        self.bot = Bot(token=Config.BOT_TOKEN)
        self.dp = Dispatcher()
        self.user_sessions = {}
        self.active_requests = defaultdict(int)
        self.request_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_REQUESTS)

        # Регистрация обработчиков
        self._register_handlers()

    def _register_handlers(self):
        """Регистрация всех обработчиков команд"""
        self.dp.message.register(self._start_handler, Command("start"))
        self.dp.message.register(self._help_handler, Command("help"))
        self.dp.message.register(self._add_user_handler, Command("add_user"))
        self.dp.message.register(self._remove_user_handler, Command("remove_user"))
        self.dp.message.register(self._list_users_handler, Command("list_users"))
        self.dp.message.register(self._main_handler)

    async def _send_message(self, chat_id: int, text: str, **kwargs):
        """Универсальный метод отправки сообщений"""
        await self.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, **kwargs)

    async def _process_urls(self, message: Message, urls: List[str]):
        """Обработка списка URL"""
        processing_msg = await message.answer(
            f"{Emojis.TIME} <b>Анализирую {len(urls)} сайтов...</b>\n"
            f"{Emojis.SEARCH} Это может занять 1-2 минуты...",
            parse_mode=ParseMode.HTML
        )

        all_results = []
        try:
            with SiteParser() as parser:
                for i, url in enumerate(urls, 1):
                    try:
                        domain = re.search(r'https?://([^/]+)', url).group(1)
                        if domain in Config.BLACKLISTED_DOMAINS:
                            await message.answer(
                                f"{Emojis.CANCEL} <b>Сайт в черном списке:</b> {url}",
                                parse_mode=ParseMode.HTML
                            )
                            continue

                        contacts = parser.extract_contacts(url)
                        all_results.append(contacts)

                        if contacts['skipped']:
                            await message.answer(
                                f"{Emojis.CANCEL} <b>Сайт пропущен:</b> {url}\n"
                                "<i>Причина:</i> в черном списке",
                                parse_mode=ParseMode.HTML
                            )
                            continue

                        site_report = [
                            f"\n{Emojis.CHECK} <b>Сайт #{i}:</b> <code>{url}</code>",
                            f"\n{Emojis.PHONE} <b>Телефоны:</b>\n" + "\n".join(f"➖ {p}" for p in contacts['phones']) if
                            contacts['phones'] else f"\n{Emojis.WARNING} Телефоны не найдены",
                            f"\n{Emojis.INN} <b>ИНН:</b>\n" + "\n".join(f"➖ {inn}" for inn in contacts['inns']) if
                            contacts['inns'] else f"\n{Emojis.WARNING} ИНН не найдены",
                            f"\n{Emojis.MONEY} <b>Финансовые данные:</b>\n" +
                            await ParserTools.format_revenue(contacts['revenues']) if
                            contacts['revenues'] else ""
                        ]

                        await message.answer("\n".join(site_report), parse_mode=ParseMode.HTML)
                        await asyncio.sleep(1)

                    except Exception as e:
                        await message.answer(
                            f"{Emojis.ERROR} <b>Ошибка при обработке:</b> {url}\n"
                            f"<i>Подробности:</i> {str(e)}",
                            parse_mode=ParseMode.HTML
                        )
                        continue

                if all_results:
                    excel_file = await ParserTools.create_excel_report(all_results)
                    await message.answer_document(
                        excel_file,
                        caption=f"{Emojis.DOC} <b>Полный отчет готов!</b> {Emojis.TADA}",
                        parse_mode=ParseMode.HTML
                    )

        except Exception as e:
            await message.answer(
                f"{Emojis.ERROR} <b>Критическая ошибка:</b>\n"
                f"<code>{str(e)}</code>",
                parse_mode=ParseMode.HTML
            )
        finally:
            try:
                await self.bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=processing_msg.message_id
                )
            except:
                pass

    async def _start_handler(self, message: Message):
        """Обработчик команды /start"""
        if not await UserManager.is_allowed(message.from_user.id):
            await message.answer(
                f"{Emojis.LOCK} <b>Доступ запрещен!</b>\n"
                "У вас нет прав для использования этого бота.",
                parse_mode=ParseMode.HTML
            )
            return

        self.user_sessions[message.from_user.id] = {"state": "waiting_urls"}

        welcome_msg = f"""
{Emojis.START} <b>Привет, {message.from_user.first_name}!</b> {Emojis.START}

Я — твой помощник для анализа конкурентов от <b>Hot Clients</b> {Emojis.ROCKET}

{Emojis.THINKING} <i>Мои возможности:</i>
• Поиск контактных телефонов {Emojis.PHONE}
• Извлечение ИНН компаний {Emojis.INN}
• Анализ финансовых показателей {Emojis.CHART}

{Emojis.SUCCESS} <b>Просто пришли мне ссылки на сайты</b> (до {Config.MAX_URLS_PER_REQUEST} за раз)

{Emojis.INFO} Для справки используйте /help
"""
        await message.answer(welcome_msg, parse_mode=ParseMode.HTML)

    async def _help_handler(self, message: Message):
        """Обработчик команды /help"""
        help_text = f"""
{Emojis.INFO} <b>Справка по боту</b> {Emojis.INFO}

<b>Основные команды:</b>
/start - Начать работу с ботом
/help - Показать эту справку

<b>Для администраторов:</b>
/add_user [id] - Добавить пользователя
/remove_user [id] - Удалить пользователя
/list_users - Показать список пользователей

<b>Как использовать:</b>
1. Пришлите ссылки на сайты конкурентов
2. Получите контактные данные и финансовую информацию
3. Скачайте полный отчет в Excel
"""
        await message.answer(help_text, parse_mode=ParseMode.HTML)

    async def _add_user_handler(self, message: Message):
        """Добавление пользователя в разрешенные"""
        if not await UserManager.is_admin(message.from_user.id):
            await message.answer(
                f"{Emojis.ERROR} <b>Доступ запрещен!</b>\n"
                "Эта команда только для администраторов.",
                parse_mode=ParseMode.HTML
            )
            return

        try:
            user_id = int(message.text.split()[1])
            if await UserManager.add_user(user_id):
                await message.answer(
                    f"{Emojis.USER_ADD} <b>Пользователь {user_id} добавлен!</b>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.answer(
                    f"{Emojis.WARNING} Пользователь {user_id} уже есть в списке",
                    parse_mode=ParseMode.HTML
                )
        except (IndexError, ValueError):
            await message.answer(
                f"{Emojis.ERROR} <b>Использование:</b> /add_user [user_id]",
                parse_mode=ParseMode.HTML
            )

    async def _remove_user_handler(self, message: Message):
        """Удаление пользователя из разрешенных"""
        if not await UserManager.is_admin(message.from_user.id):
            await message.answer(
                f"{Emojis.ERROR} <b>Доступ запрещен!</b>\n"
                "Эта команда только для администраторов.",
                parse_mode=ParseMode.HTML
            )
            return

        try:
            user_id = int(message.text.split()[1])

            if user_id in Config.ADMIN_IDS:
                await message.answer(
                    f"{Emojis.ERROR} <b>Нельзя удалить администратора!</b>",
                    parse_mode=ParseMode.HTML
                )
                return

            if await UserManager.remove_user(user_id):
                await message.answer(
                    f"{Emojis.USER_REMOVE} <b>Пользователь {user_id} удален!</b>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.answer(
                    f"{Emojis.WARNING} Пользователь {user_id} не найден в списке разрешенных",
                    parse_mode=ParseMode.HTML
                )
        except (IndexError, ValueError):
            await message.answer(
                f"{Emojis.ERROR} <b>Использование:</b> /remove_user [user_id]",
                parse_mode=ParseMode.HTML
            )

    async def _list_users_handler(self, message: Message):
        """Показать список пользователей"""
        if not await UserManager.is_admin(message.from_user.id):
            await message.answer(
                f"{Emojis.ERROR} <b>Доступ запрещен!</b>\n"
                "Эта команда только для администраторов.",
                parse_mode=ParseMode.HTML
            )
            return

        users_list = await UserManager.get_users_list()
        await message.answer(
            f"{Emojis.LIST} <b>Список пользователей:</b>\n\n{users_list}",
            parse_mode=ParseMode.HTML
        )

    async def _main_handler(self, message: Message):
        """Основной обработчик сообщений"""
        if not await UserManager.is_allowed(message.from_user.id):
            return

        user_id = message.from_user.id

        if user_id not in self.user_sessions:
            await message.answer(f"{Emojis.WARNING} Пожалуйста, начните с команды /start")
            return

        if self.active_requests[user_id] >= Config.MAX_CONCURRENT_REQUESTS:
            await message.answer(
                f"{Emojis.WAIT} <b>Достигнут лимит запросов!</b>\n\n"
                f"У меня сейчас {Config.MAX_CONCURRENT_REQUESTS} активных запроса. "
                "Пожалуйста, дождитесь их завершения.",
                parse_mode=ParseMode.HTML
            )
            return

        urls = ParserTools.extract_urls(message.text)
        if not urls:
            await message.answer(f"{Emojis.ERROR} Не найдено ссылок в сообщении!")
            return

        if len(urls) > Config.MAX_URLS_PER_REQUEST:
            await message.answer(
                f"{Emojis.WARNING} Принято первых {Config.MAX_URLS_PER_REQUEST} из {len(urls)} ссылок")
            urls = urls[:Config.MAX_URLS_PER_REQUEST]

        self.active_requests[user_id] += 1

        try:
            async with self.request_semaphore:
                await self._process_urls(message, urls)
        finally:
            self.active_requests[user_id] = max(0, self.active_requests[user_id] - 1)
            if self.active_requests[user_id] == 0:
                del self.active_requests[user_id]

    async def run(self):
        """Запуск бота"""
        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    bot = CompetitorAnalyzerBot()
    asyncio.run(bot.run())