import re
import time
import random
from typing import List, Set, Tuple
from urllib.parse import quote_plus, urlparse, parse_qs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent


class YandexParser:
    def __init__(self):
        self.ua = UserAgent()
        chrome_options = Options()
        chrome_options.add_argument(f"user-agent={self.ua.random}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def human_like_delay(self):
        """Случайная задержка между действиями"""
        time.sleep(random.uniform(1.0, 3.0))

    def solve_captcha(self):
        try:
            captcha = self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '.CheckboxCaptcha, .AdvancedCaptcha')))
            if captcha:
                print("Обнаружена капча. Решите её вручную в открывшемся браузере...")
                input("Нажмите Enter после решения капчи...")
        except:
            pass

    def get_search_results(self, query: str, max_results: int = 10) -> List[str]:
        """Поиск в Яндексе по запросу"""
        url = f"https://yandex.ru/search/?text={quote_plus(query)}&lr=213"
        self.driver.get(url)
        self.human_like_delay()

        if self.solve_captcha():
            self.human_like_delay()

        # Имитация поведения пользователя
        for _ in range(2):
            self.driver.execute_script("window.scrollBy(0, window.innerHeight * 0.7)")
            self.human_like_delay()

        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.serp-item')))
            links = []

            # Несколько стратегий поиска результатов
            selectors = [
                '.serp-item .OrganicTitle-Link',  # Основные результаты
                '.Organic .Link',  # Альтернативный селектор
                'a[href*="yabs.yandex.ru"]'  # Рекламные ссылки
            ]

            for selector in selectors:
                if len(links) >= max_results:
                    break

                items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for item in items:
                    try:
                        href = item.get_attribute('href')
                        clean_url = self.clean_url(href)
                        if clean_url and clean_url not in links:
                            links.append(clean_url)
                            if len(links) >= max_results:
                                break
                    except:
                        continue

            return links[:max_results]

        except Exception as e:
            print(f"Ошибка поиска: {str(e)}")
            return []

    def clean_url(self, url: str) -> str:
        """Очистка URL от параметров отслеживания"""
        try:
            parsed = urlparse(url)
            if 'yabs.yandex.ru' in parsed.netloc:
                qs = parse_qs(parsed.query)
                return qs.get('url', [url])[0]
            return url if url and not url.startswith('https://yandex.ru') else None
        except:
            return None

    @staticmethod
    def normalize_phone(phone: str) -> str:
        """Нормализация телефонного номера"""
        digits = re.sub(r'\D', '', phone)
        if digits.startswith('8'):
            digits = '7' + digits[1:]
        elif not digits.startswith('7'):
            digits = '7' + digits
        return f"+7{digits[1:11]}" if len(digits) == 11 else phone

    def extract_phones(self) -> Set[str]:
        """Поиск телефонных номеров на странице"""
        phone_pattern = r'(?:\+7|8|7|\(\d{3}\)|\d{3})[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}'
        phones = set()

        # Поиск в специальных блоках
        contact_selectors = [
            'footer', 'header',
            '[class*="contact"]',
            '[class*="phone"]',
            '[class*="tel"]'
        ]

        for selector in contact_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for el in elements:
                    phones.update(re.findall(phone_pattern, el.text))
            except:
                continue

        # Поиск в ссылках tel:
        try:
            tel_links = self.driver.find_elements(By.XPATH, '//a[contains(@href, "tel:")]')
            for link in tel_links:
                phone = link.get_attribute('href').replace('tel:', '').strip()
                if phone:
                    phones.add(phone)
        except:
            pass

        # Нормализация и валидация
        normalized_phones = set()
        for phone in phones:
            norm_phone = self.normalize_phone(phone)
            if norm_phone.startswith('+7') and len(norm_phone) == 12:
                normalized_phones.add(norm_phone)

        return normalized_phones

    def extract_inn(self) -> Set[str]:
        """Улучшенный поиск ИНН на странице"""
        inn_patterns = [
            r'(?:ИНН|инн)[\s:]*[\n\r]?\s*(\d{10}|\d{12})',  # ИНН после подписи
            r'(?:ОГРН|ИНН|КПП)[\s:]*(\d+)[\s/]*',  # Реквизиты блоком
            r'\b(\d{10}|\d{12})\b'  # Отдельные числа
        ]

        inns = set()

        # Приоритетные места для поиска
        priority_selectors = [
            'footer',
            '[class*="requisite"]',
            '[class*="inn"]',
            '[class*="legal"]'
        ]

        # Сначала проверяем приоритетные блоки
        for selector in priority_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for el in elements:
                    text = el.text.replace('\n', ' ')
                    for pattern in inn_patterns:
                        matches = re.finditer(pattern, text, re.IGNORECASE)
                        for match in matches:
                            for group in match.groups():
                                if group and len(group) in (10, 12) and group.isdigit():
                                    inns.add(group)
            except:
                continue

        # Затем проверяем всю страницу, если не нашли в приоритетных местах
        if not inns:
            try:
                body_text = self.driver.find_element(By.TAG_NAME, 'body').text.replace('\n', ' ')
                for pattern in inn_patterns:
                    matches = re.finditer(pattern, body_text, re.IGNORECASE)
                    for match in matches:
                        for group in match.groups():
                            if group and len(group) in (10, 12) and group.isdigit():
                                inns.add(group)
            except:
                pass

        return inns

    def extract_contacts(self, url: str) -> Tuple[List[str], List[str]]:
        """Основной метод извлечения контактов"""
        try:
            self.driver.get(url)
            self.human_like_delay()

            # Прокрутка для загрузки всего контента
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            self.human_like_delay()

            phones = self.extract_phones()
            inns = self.extract_inn()

            return sorted(phones), sorted(inns)

        except Exception as e:
            print(f"Ошибка обработки {url}: {str(e)}")
            return [], []

    def close(self):
        """Закрытие драйвера"""
        try:
            self.driver.quit()
        except:
            pass


# Пример использования для Telegram бота
if __name__ == "__main__":
    with YandexParser(headless=False) as parser:
        query = "бурение скважин Москва"
        print(f"Поиск по запросу: {query}")

        links = parser.get_search_results(query)
        print(f"Найдено результатов: {len(links)}")

        for i, url in enumerate(links, 1):
            print(f"\n{i}. Анализ: {url}")
            phones, inns = parser.extract_contacts(url)

            if phones:
                print("Телефоны:")
                for p in phones:
                    print(f"- {p}")
            else:
                print("Телефоны не найдены")

            if inns:
                print("ИНН:")
                for inn in inns:
                    print(f"- {inn}")
            else:
                print("ИНН не найдены")