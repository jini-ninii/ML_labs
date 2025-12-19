import requests # для отправки запросов к сайту и скачивания HTML-кода страниц
from bs4 import BeautifulSoup # для разбора HTML-кода и поиска нужных данных
import time
import datetime
import random
import uuid # генератор уникальных ID
import sqlite3 # для работы с БД
import urllib3 # отключаем предупреждения
from datetime import timezone
from concurrent.futures import ThreadPoolExecutor, as_completed # многопоточность (качать несколько статей одновременно)
from threading import Lock # для синхронизации записи в БД (очередь для потоков)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

STORAGE_FILE = "articles_nsk.db"
GOAL_TOTAL = 5000
WORKERS_NUM = 30
SITE_ROOT = "http://www.nsktv.ru"
NEWS_LIST_LINK = "http://www.nsktv.ru/news/"

WRITE_LOCK = Lock()

# притворяемся то Хромом, то Файрфоксом чтобы не улететь в бан
BROWSER_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

# создает файл БД и таблицу articles с нужными полями, если их еще нет
# если уже создан, функция при запуске просто убедится, что таблица на месте
def prepare_storage():
    db_conn = sqlite3.connect(STORAGE_FILE) # пытаемся подключиться к файлу, если его нет - создаем
    db_cur = db_conn.cursor() # создает объект курсора, который является посредником для выполнения SQL-запросов к БД

    # запрос к БД (если не существует таблицы articles, создай её с такими-то полями)
    db_cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        guid TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT,
        url TEXT UNIQUE,
        published_at TEXT,
        comments_count INTEGER DEFAULT 0,
        created_at_utc TEXT NOT NULL,
        rating INTEGER DEFAULT 0
    );
    """)
    db_conn.commit()
    db_conn.close()

# принимает данные одной статьи (словарь article_data) и кладет их в файл БД
def store_record(item_data):
    with WRITE_LOCK:
        db_conn = sqlite3.connect(STORAGE_FILE, timeout=5)
        db_cur = db_conn.cursor()
        try:
            # еще один запрос к БД (вставь новую строку или заигнорь, если уже такая есть)
            # используем ? для безопасной вставки
            # это защищает от SQL-инъекций и ошибок, если в тексте встретятся кавычки
            db_cur.execute("""
            INSERT OR IGNORE INTO articles (
                guid, title, description, url, published_at, comments_count, created_at_utc, rating
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()),
                item_data.get('title'),
                item_data.get('description'),
                item_data.get('url'),
                item_data.get('published_at'),
                0,
                datetime.datetime.now(timezone.utc).isoformat(),
                0
            ))
            db_conn.commit()
            return True

        # если что-то пойдет не так, прога не крашнется, а перейдет к следующей статье
        except Exception as err:
            print(f"[Ошибка БД] {err}")
            return False
        finally:
            db_conn.close()

# на случай, если с первого раза не достучались, делаем несколько попыток
def fetch_html_safe(target_link, max_tries=5):
    for try_num in range(max_tries):
        try:
            req_headers = {"User-Agent": random.choice(BROWSER_LIST)}
            time.sleep(random.uniform(0.1, 0.3))

            # если сервер принял звонок, но завис, мы не будем ждать вечно
            # через 15 секунд мы сбросим соединение и попробуем снова
            resp = requests.get(target_link, headers=req_headers, timeout=5, verify=False)

            # если достучались
            # принудительно ставим кодировку utf-8, чтобы русский текст не превратился во что-то непонятное
            # парсим HTML и возвращаем результат
            if resp.status_code == 200:
                resp.encoding = 'utf-8'
                return BeautifulSoup(resp.text, 'html.parser')

            # если страницы не существует
            # даже не пытаемся стучать
            elif resp.status_code == 404:
                return None

            # при других ошибках даем серверу отдышаться
            # и идем на новый круг
            else:
                time.sleep(5)

        # сюда мы попадаем, если интернет отключился совсем или DNS не сработал
        except Exception:
            time.sleep(5 * (try_num + 1))

    # сдаемся, если спустя три попытки ничего не получили
    # переходим к следующей статье
    return None

# получаем ссылку, ходим по ней, вытаскиваем всю полезную информацию и возвращаем готовый результат
def process_single_news(link):

    page_soup = fetch_html_safe(link)
    if not page_soup:
        return None

    # ищем заголовок
    h1_tag = page_soup.find('h1')
    head_text = h1_tag.get_text(strip=True) if h1_tag else "Без заголовка"

    body_text = ""
    # ищем стандартный блок текста
    main_div = page_soup.find('div', class_='detail_text') or page_soup.find('div', class_='news-detail')

    # очистка от мусора
    if main_div:
        for junk in main_div(["script", "style", "iframe", "div.banner", "noindex", "div.news-date-time"]):
            junk.decompose()
        body_text = main_div.get_text(separator="\n", strip=True)

    # если стандартный блок не найден
    # мы берем всю центральную колонку сайта, ищем там все параграфы <p> и склеиваем те, что длиннее 30 символов
    if not body_text:
        center_col = page_soup.find('div', class_='page_center-column')
        if center_col:
            body_text = "\n".join([p.text.strip() for p in center_col.find_all('p') if len(p.text) > 30])

    if not body_text or len(body_text) < 30:
        return None

    date_str = None # если мы не найдем дату, в переменной так и останется None
    date_div = page_soup.find('div', class_='block1__wrap__textb')
    if date_div:
        date_str = date_div.get_text(strip=True)

    # упаковываем всё, что нашли, в словарь и возвращаем его
    return {
        'title': head_text,
        'description': body_text,
        'url': link,
        'published_at': date_str
    }

def start_scraping():
    prepare_storage() # проверяем, что файл базы данных существует

    # узнаем, сколько работы уже сделано
    db_conn = sqlite3.connect(STORAGE_FILE)
    db_cur = db_conn.cursor()
    db_cur.execute("SELECT COUNT(*) FROM articles")
    saved_count = db_cur.fetchone()[0] # получаем число
    db_conn.close()

    print(f"В базе: {saved_count} | Цель: {GOAL_TOTAL}")

    initial_page = max(1, (saved_count // 30))
    current_page = initial_page

    print(f"Начинаем со страницы {current_page}")

    with ThreadPoolExecutor(max_workers=WORKERS_NUM) as pool:

        while saved_count < GOAL_TOTAL:
            if current_page == 1:
                page_url = NEWS_LIST_LINK
            else:
                page_url = f"{NEWS_LIST_LINK}?PAGEN_1={current_page}"

            print(f"\n   Страница {current_page}")

            page_soup = fetch_html_safe(page_url, max_tries=5)

            # если скачать не удалось - идем дальше
            if not page_soup:
                print("   Не удалось открыть страницу, пропуск")
                current_page += 1
                continue

            # собираем ссылки на новости с текущей страницы
            # превращаем их из относительных путей в полные URL-адреса
            # чтобы программа могла по ним перейти
            found_urls = []
            for a_tag in page_soup.find_all('a', class_='news_block'):
                raw_href = a_tag.get('href')
                if raw_href:
                    full_link = SITE_ROOT + raw_href if raw_href.startswith('/') else raw_href
                    found_urls.append(full_link)

            if not found_urls:
                time.sleep(10)
                current_page += 1
                continue

            # проверяем в БД, какие ссылки мы уже скачали
            db_conn = sqlite3.connect(STORAGE_FILE)
            db_cur = db_conn.cursor()
            new_urls = []

            # проверяем каждую ссылку, есть она в базе или нет
            for check_link in found_urls:
                db_cur.execute("SELECT 1 FROM articles WHERE url = ?", (check_link,))
                # если cur.fetchone() вернул пустоту - значит, статьи нет, надо качать
                if not db_cur.fetchone():
                    new_urls.append(check_link)
            db_conn.close()

            print(f"   Новых ссылок: {len(new_urls)}")

            if new_urls:

                # запускаем процесс скачивания сразу для всех ссылок параллельно
                tasks_map = {pool.submit(process_single_news, u): u for u in new_urls}

                # ждем, пока потоки начнут возвращать результаты
                for task in as_completed(tasks_map):
                    result_item = task.result() # получаем словарь статьи или None
                    if result_item:
                        # пытаемся сохранить в базу
                        if store_record(result_item):
                            saved_count += 1
                            # каждые 50 статей отслеживаем прогресс
                            if saved_count % 50 == 0:
                                print(f"   [Всего сохранено: {saved_count}]")

            time.sleep(0.5)
            current_page += 1

    print(f"\nКонец, всего в базе {saved_count} статей")


if __name__ == "__main__":
    start_scraping()

