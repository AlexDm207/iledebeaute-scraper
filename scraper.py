import json
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import cloudscraper
from bs4 import BeautifulSoup

BASE_URL = "https://iledebeaute.ru"
CATALOG_URL = "https://iledebeaute.ru/catalog/tip-has_discount-iz-prom/"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def clean_text(value):
    """Нормализует пробелы и убирает неразрывные пробелы."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).replace("\xa0", " ").strip()


def normalize_name(name):
    """Убирает префикс 'Перейти к товару' из aria-label."""
    name = clean_text(name)
    return re.sub(r"^Перейти к товару\s+", "", name, flags=re.I)


def normalize_url(url):
    """Сравнивает URL без повторов и без мусора в query string."""
    if not url:
        return None
    parsed = urlsplit(url)
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    query_items = sorted(query_items, key=lambda item: item[0])
    query = urlencode(query_items, doseq=True)
    canonical = urlunsplit(parsed._replace(query=query, fragment="")).rstrip("/?")
    return canonical or f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def fetch_html(url):
    """Скачивает страницу и возвращает HTML."""
    session = cloudscraper.create_scraper()
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def parse_catalog_products(html):
    """Находит реальные ссылки на товары в каталоге."""
    soup = BeautifulSoup(html, "html.parser")
    products = []
    seen = set()

    for link in soup.select("a[href*='/product/']"):
        href = link.get("href")
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        name = normalize_name(
            link.get("aria-label")
            or link.get("title")
            or link.get_text(" ", strip=True)
        )

        if not name:
            continue

        products.append({
            "name": name,
            "url": full_url,
            "source": "iledebeaute",
            "source_url": full_url
        })

    return products


def extract_product_block(soup):
    """Берёт блок рядом с h1, чтобы не смешивать цены из других частей страницы."""
    h1 = soup.select_one("h1")
    if h1:
        for parent in (h1.parent, h1.find_parent("div"), h1.find_parent("section"), h1.find_parent("article")):
            if parent is not None:
                return parent
    return soup


def parse_price_pair(page_text):
    """Ищет реальную пару цен: текущая + старая, например 9949¤ 22110¤."""
    match = re.search(r"(\d[\d\s]*\d)\s*¤\s*(\d[\d\s]*\d)\s*¤", page_text)
    if not match:
        return None, None

    current = int(re.sub(r"\D", "", match.group(1)))
    old = int(re.sub(r"\D", "", match.group(2)))
    return current, old


def extract_product_details(item):
    """Разбирает страницу товара и сохраняет только нужные поля."""
    html = fetch_html(item["url"])
    soup = BeautifulSoup(html, "html.parser")

    product_block = extract_product_block(soup)
    product_text = clean_text(product_block.get_text(" ", strip=True))

    title = soup.select_one("h1")
    if title:
        item["name"] = normalize_name(title.get_text(" ", strip=True))

    # Бренды убираем намеренно: вам они не нужны
    item.pop("brand", None)

    current_price, old_price = parse_price_pair(product_text)
    if current_price is not None:
        item["price"] = current_price
    if old_price is not None:
        item["old_price"] = old_price

    lower = product_text.lower()
    if "нет в наличии" in lower:
        item["in_stock"] = False
    elif "в наличии" in lower or "наличие" in lower:
        item["in_stock"] = True
    else:
        item["in_stock"] = None

    rating_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:из\s*5|из 5)", product_text, flags=re.I)
    if rating_match:
        item["rating"] = float(rating_match.group(1).replace(",", "."))

    reviews_match = re.search(r"(\d+)\s*(?:отзыв|отзыва|отзывов)", product_text, flags=re.I)
    if reviews_match:
        item["reviews_count"] = int(reviews_match.group(1))

    volume_match = re.search(r"(\d+\s*(?:мл|ml))", product_text, flags=re.I)
    if volume_match:
        item["volume"] = volume_match.group(1)

    return item


def get_next_page_url(html, current_url=None):
    """Находит следующую страницу каталога, но не даёт зациклиться на текущей."""
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue

        full_href = urljoin(BASE_URL, href)
        text = clean_text(a.get_text(" ", strip=True)).lower()

        if "показать еще" in text or "показать ещё" in text:
            candidates.append(full_href)
            continue

        if "PAGEN_1" in href or "page=" in href:
            candidates.append(full_href)

    current_norm = normalize_url(current_url) if current_url else None
    seen = set()

    for href in candidates:
        norm = normalize_url(href)
        if not norm or norm in seen:
            continue
        seen.add(norm)

        if current_norm and norm == current_norm:
            continue

        return href

    return None


def save_products(products):
    """Делает корректный JSON-массив и JSONL-файл."""
    with open("products.json", "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    with open("products.jsonl", "w", encoding="utf-8") as f:
        for item in products:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def main():
    """Главный цикл: проходим по страницам каталога и выгружаем товары."""
    current_url = CATALOG_URL
    visited = set()
    all_products = []

    while current_url and current_url not in visited:
        visited.add(current_url)

        html = fetch_html(current_url)
        products = parse_catalog_products(html)

        for product in products:
            all_products.append(extract_product_details(product))

        next_url = get_next_page_url(html, current_url=current_url)
        if not next_url:
            break

        current_norm = normalize_url(current_url)
        next_norm = normalize_url(next_url)
        if next_norm and current_norm and next_norm == current_norm:
            break

        current_url = next_url

    save_products(all_products)
    print(f"Saved {len(all_products)} products")
    return all_products


if __name__ == "__main__":
    main()