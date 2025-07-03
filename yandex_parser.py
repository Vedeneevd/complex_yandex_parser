import base64
import os
import re
import time
import random
import requests
from selenium.webdriver import ActionChains
from twocaptcha import TwoCaptcha
from typing import Set, Dict, Optional
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


class SiteParser:
    # Домены, которые нужно пропускать
    SKIP_DOMAINS = {
        'avito.ru',
        'uslugi.yandex.ru',
        'yandex.ru',
        'yandex.com',
        'google.com',
        'facebook.com',
        'vk.com',
        'instagram.com'
    }

    def __init__(self, headless: bool = True):
        self.ua = UserAgent()
        chrome_options = Options()
        chrome_options.add_argument(f"user-agent={self.ua.random}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        if headless:
            chrome_options.add_argument("--headless=new")

        # Автоматическая установка правильной версии ChromeDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.wait = WebDriverWait(self.driver, 20)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        if os.getenv('RUCAPTCHA_API_KEY'):
            self.captcha_solver = TwoCaptcha(os.getenv('RUCAPTCHA_API_KEY'))
        else:
            self.captcha_solver = None

        self.captcha_attempts = 3

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def human_like_delay(self):
        """Случайная задержка между действиями"""
        time.sleep(random.uniform(1.0, 3.0))

    class YandexCaptchaSolver:
        def __init__(self, driver, api_key):
            self.driver = driver
            self.api_key = api_key
            self.wait = WebDriverWait(driver, 20)
            self.base_url = "https://api.rucaptcha.com"

        def get_image_as_base64(self, url):
            """Загружает изображение и возвращает в base64"""
            response = requests.get(url)
            return base64.b64encode(response.content).decode('utf-8')

        def solve_captcha(self):
            """Основной метод решения капчи"""
            try:
                # 1. Получаем изображение капчи
                captcha_wrapper = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.AdvancedCaptcha-ImageWrapper')))
                captcha_img = captcha_wrapper.find_element(By.TAG_NAME, 'img')
                captcha_url = captcha_img.get_attribute('src')

                # 2. Получаем изображение задания
                task_element = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.AdvancedCaptcha-SilhouetteTask')))
                task_img = task_element.find_elements(By.TAG_NAME, 'img')[1]  # Берем второе изображение
                task_url = task_img.get_attribute('src')

                print("Получаем изображения капчи...")
                captcha_base64 = self.get_image_as_base64(captcha_url)
                task_base64 = self.get_image_as_base64(task_url)

                # 3. Отправляем капчу на решение
                print("Отправляем капчу в RuCaptcha...")
                task_id = self.send_to_rucaptcha(captcha_base64, task_base64)

                if not task_id:
                    return False

                # 4. Получаем решение
                print("Ожидаем решение капчи...")
                solution = self.get_solution(task_id)

                if not solution:
                    return False

                # 5. Кликаем по координатам
                print("Выполняем клики по координатам...")
                self.click_coordinates(captcha_img, solution)

                # 6. Отправляем решение
                submit_btn = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '.AdvancedCaptcha-Submit')))
                submit_btn.click()

                print("Капча успешно решена!")
                return True

            except Exception as e:
                print(f"Ошибка при решении капчи: {str(e)}")
                return False

        def send_to_rucaptcha(self, captcha_base64, task_base64):
            """Отправка капчи в RuCaptcha"""
            payload = {
                "clientKey": self.api_key,
                "task": {
                    "type": "CoordinatesTask",
                    "body": captcha_base64,
                    "imginstructions": task_base64,
                    "comment": "Кликните в указанном порядке | Click in the following order"
                }
            }

            try:
                response = requests.post(
                    f"{self.base_url}/createTask",
                    json=payload,
                    timeout=30
                )
                data = response.json()

                if data.get('errorId') != 0:
                    print(f"Ошибка API: {data.get('errorDescription')}")
                    return None

                return data['taskId']

            except Exception as e:
                print(f"Ошибка при отправке капчи: {str(e)}")
                return None

        def get_solution(self, task_id, timeout=120):
            """Получение решения капчи"""
            start_time = time.time()

            while time.time() - start_time < timeout:
                try:
                    response = requests.get(
                        f"{self.base_url}/getTaskResult",
                        params={
                            'clientKey': self.api_key,
                            'taskId': task_id
                        },
                        timeout=10
                    )
                    data = response.json()

                    if data.get('errorId') != 0:
                        print(f"Ошибка API: {data.get('errorDescription')}")
                        return None

                    if data['status'] == 'ready':
                        return data['solution']['coordinates']

                    time.sleep(5)  # Проверяем каждые 5 секунд

                except Exception as e:
                    print(f"Ошибка при проверке решения: {str(e)}")
                    time.sleep(5)

            print("Время ожидания решения истекло")
            return None

        def click_coordinates(self, captcha_element, coordinates):
            """Клики по полученным координатам"""
            for point in coordinates:
                x = int(point['x'])
                y = int(point['y'])

                # Добавляем случайные задержки для имитации человеческого поведения
                delay = random.uniform(0.1, 0.3)
                time.sleep(delay)

                # Плавное перемещение и клик
                action = ActionChains(self.driver)
                action.move_to_element_with_offset(captcha_element, x, y)
                action.pause(delay)
                action.click()
                action.perform()

    def solve_yandex_captcha(self):
        """Комплексное решение Яндекс капчи с использованием RuCaptcha API"""
        try:
            # 1. Проверяем наличие чекбокс-капчи "Я не робот"
            if len(self.driver.find_elements(By.CSS_SELECTOR, '.CheckboxCaptcha')) > 0:
                print("Обнаружена чекбокс-капча")
                checkbox = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '.CheckboxCaptcha-Button')))

                # Человекоподобное взаимодействие
                action = ActionChains(self.driver)
                action.move_to_element(checkbox).pause(random.uniform(0.2, 0.5)).click().perform()
                print("Чекбокс 'Я не робот' отмечен")
                time.sleep(random.uniform(2, 3))  # Важная задержка

            # 2. Проверяем наличие графической капчи
            if len(self.driver.find_elements(By.CSS_SELECTOR, '.AdvancedCaptcha')) > 0:
                print("Обнаружена графическая капча")

                # Получаем элемент с капчей
                captcha_element = self.wait.until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, '.AdvancedCaptcha-Image')))

                # Делаем скриншот элемента капчи
                captcha_path = 'captcha.png'
                captcha_element.screenshot(captcha_path)

                # Подготавливаем задание для RuCaptcha
                try:
                    print("Отправляем капчу в RuCaptcha...")

                    # Читаем изображение как base64
                    with open(captcha_path, 'rb') as f:
                        image_base64 = base64.b64encode(f.read()).decode('utf-8')

                    # Параметры для API RuCaptcha
                    params = {
                        'clientKey': os.getenv('RUCAPTCHA_API_KEY'),
                        'task': {
                            'type': 'CoordinatesTask',
                            'body': image_base64,
                            'comment': 'Пожалуйста, кликните на все объекты, указанные в задании'
                        },
                        'languagePool': 'rn'  # Русский язык
                    }

                    # Отправляем запрос к API RuCaptcha
                    response = requests.post(
                        'https://api.rucaptcha.com/createTask',
                        json=params,
                        timeout=30
                    )
                    response.raise_for_status()
                    task_data = response.json()

                    if task_data.get('errorId', 1) != 0:
                        raise Exception(f"RuCaptcha API error: {task_data.get('errorDescription', 'Unknown error')}")

                    task_id = task_data['taskId']
                    print(f"Задание создано, ID: {task_id}")

                    # Ожидаем решения (максимум 120 секунд)
                    for _ in range(24):
                        time.sleep(5)
                        result_response = requests.post(
                            'https://api.rucaptcha.com/getTaskResult',
                            json={
                                'clientKey': os.getenv('RUCAPTCHA_API_KEY'),
                                'taskId': task_id
                            },
                            timeout=10
                        )
                        result_data = result_response.json()

                        if result_data.get('errorId', 1) != 0:
                            raise Exception(
                                f"RuCaptcha API error: {result_data.get('errorDescription', 'Unknown error')}")

                        if result_data['status'] == 'ready':
                            print("Капча успешно решена")
                            break
                    else:
                        raise Exception("Превышено время ожидания решения капчи")

                    # Получаем координаты кликов
                    solution = result_data['solution']['coordinates']
                    print(f"Получены координаты: {solution}")

                    # Кликаем по всем точкам
                    for point in solution:
                        x = point['x']
                        y = point['y']

                        # Человекоподобное движение и клик
                        action = ActionChains(self.driver)
                        action.move_to_element_with_offset(
                            captcha_element, x, y
                        ).pause(random.uniform(0.1, 0.3)).click().perform()
                        time.sleep(random.uniform(0.2, 0.5))

                    # Отправляем решение
                    submit = self.wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '.AdvancedCaptcha-Submit')))
                    submit.click()

                    # Ждем исчезновения капчи
                    self.wait.until(EC.invisibility_of_element_located(
                        (By.CSS_SELECTOR, '.AdvancedCaptcha')))
                    print("Капча успешно пройдена")

                    return True

                return False

            except Exception as e:
                print(f"Критическая ошибка при решении капчи: {str(e)}")
                self.driver.save_screenshot('captcha_error.png')
                return False

    def should_skip_url(self, url: str) -> bool:
        """Проверяет, нужно ли пропускать URL"""
        try:
            domain = urlparse(url).netloc.lower()
            for skip_domain in self.SKIP_DOMAINS:
                if domain == skip_domain or domain.endswith(f".{skip_domain}"):
                    return True
            return False
        except:
            return False

    def get_search_results(self, query: str, max_results: int = 10) -> List[str]:
        """Поиск в Яндексе по запросу с фильтрацией нежелательных доменов"""
        url = f"https://yandex.ru/search/?text={quote_plus(query)}&lr=213"
        self.driver.get(url)
        self.human_like_delay()

        # Проверяем наличие капчи и решаем ее
        try:
            if self.driver.find_elements(By.CSS_SELECTOR, '.AdvancedCaptcha'):
                if not self.solve_image_captcha():
                    print("Не удалось решить капчу, пробуем продолжить...")
        except:
            pass

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

                        # Пропускаем нежелательные домены
                        if clean_url and not self.should_skip_url(clean_url) and clean_url not in links:
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
        """Очистка URL от параметров отслеживания и проверка на нежелательные домены"""
        try:
            parsed = urlparse(url)

            # Пропускаем нежелательные домены сразу
            domain = parsed.netloc.lower()
            for skip_domain in self.SKIP_DOMAINS:
                if domain == skip_domain or domain.endswith(f".{skip_domain}"):
                    return None

            if 'yabs.yandex.ru' in domain:
                qs = parse_qs(parsed.query)
                clean_url = qs.get('url', [url])[0]
                # Проверяем очищенный URL на нежелательные домены
                return clean_url if not self.should_skip_url(clean_url) else None

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
            r'(?:ИНН|инн)[\s:]*[\n\r]?\s*(\d{10}|\d{12})',
            r'(?:ОГРН|ИНН|КПП)[\s:]*(\d+)[\s/]*',
            r'\b(\d{10}|\d{12})\b'
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

    def get_company_revenue(self, inn: str) -> Optional[str]:
        """Получение выручки компании по ИНН с datanewton.ru"""
        try:
            # 1. Выполняем поиск по ИНН
            search_url = f"https://datanewton.ru/search?query={inn}&type=ul"
            self.driver.get(search_url)
            self.human_like_delay()

            # 2. Кликаем по первой найденной компании
            try:
                # Ждем появления списка компаний
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".list-group.list-group-flush"))
                )

                # Проверяем, есть ли результаты
                no_results = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'ничего не найдено')]")
                if no_results:
                    print(f"Компания с ИНН {inn} не найдена на datanewton.ru")
                    return None

                # Находим и кликаем первую компанию в списке
                first_company = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, ".list-group.list-group-flush a.list-group-item:first-child"))
                )
                first_company.click()
                self.human_like_delay()

                # 3. Ждем загрузки страницы компании и данных о выручке
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'Выручка')]"))
                    )

            # Ожидание загрузки страницы с выручкой
            revenue_element = self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[contains(text(),'Выручка')]/following-sibling::div")
                )
            )

            return revenue_element.text

            except TimeoutException:
                print(f"Не удалось найти компанию с ИНН {inn} в результатах поиска")
                return None

        except Exception as e:
            print(f"Ошибка при получении данных для ИНН {inn}: {str(e)}")
            return None

    def extract_contacts(self, url: str) -> Dict[str, any]:
        """Основной метод извлечения контактов с проверкой на нежелательные домены"""
        if self.should_skip_url(url):
            print(f"Пропускаем URL (в черном списке): {url}")
            return {
                'url': url,
                'phones': [],
                'inns': [],
                'revenues': {},
                'skipped': True
            }

        result = {
            'url': url,
            'phones': [],
            'inns': [],
            'revenues': {},
            'skipped': False
        }

        try:
            self.driver.get(url)
            self.human_like_delay()

            # Прокрутка для загрузки всего контента
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            self.human_like_delay()

            # Проверяем наличие капчи
            if self.driver.find_elements(By.CSS_SELECTOR, '.AdvancedCaptcha'):
                self.solve_yandex_captcha()

            phones = self.extract_phones()
            inns = self.extract_inn()

            result['phones'] = sorted(phones)
            result['inns'] = sorted(inns)

            # Получаем финансовые данные для каждого найденного ИНН
            for inn in inns:
                revenue_data = self.get_company_revenue(inn)
                if revenue_data:
                    result['revenues'][inn] = revenue_data
                else:
                    result['revenues'][inn] = "Финансовые данные не найдены"

            return result

        except Exception as e:
            print(f"Ошибка обработки {url}: {str(e)}")
            return result


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
            contacts = parser.extract_contacts(url)

            if contacts['skipped']:
                print("Сайт пропущен (в черном списке)")
                continue

            if contacts['phones']:
                print("Телефоны:")
                for p in contacts['phones']:
                    print(f"- {p}")
            else:
                print("Телефоны не найдены")

            if contacts['inns']:
                print("ИНН:")
                for inn in contacts['inns']:
                    print(f"- {inn}")
                    if inn in contacts['revenues']:
                        print(f"  Выручка: {contacts['revenues'][inn]}")
            else:
                print("ИНН не найдены")