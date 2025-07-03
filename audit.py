import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Настройка драйвера
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.maximize_window()

try:
    # 1. Выполняем поиск по ИНН
    inn = "5003052454"
    driver.get(f"https://datanewton.ru/search?query={inn}+&type=ul")

    # 2. Кликаем по первой найденной компании
    # Ждем появления списка компаний
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".list-group.list-group-flush"))
    )
    time.sleep(3)
    # Находим первую ссылку в списке
    first_company = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, ".list-group.list-group-flush a.list-group-item:first-child"))
    )
    first_company.click()

    # 3. Ждем загрузки страницы компании и появления данных о выручке
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'Выручка')]"))
    )

    # 4. Извлекаем значение выручки
    revenue_element = driver.find_element(By.XPATH, "//div[contains(text(),'Выручка')]/following-sibling::div")
    revenue = revenue_element.text

    print(f"Размер выручки компании: {revenue}")

finally:
    driver.quit()