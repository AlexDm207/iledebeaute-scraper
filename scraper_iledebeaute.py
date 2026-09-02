import json
import os
import re
import time
from datetime import UTC, datetime
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

import cloudscraper
from bs4 import BeautifulSoup
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

# --- Configuration via environment ---
BASE_URL = "https://iledebeaute.ru"
CATALOG_URL = os.getenv("CATALOG_URL", "https://iledebeaute.ru/catalog/tip-has_discount-iz-prom/")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
RAW_PRODUCTS_TOPIC = os.getenv("RAW_PRODUCTS_TOPIC", "raw_products")
METRICS_PORT = int(os.getenv("METRICS_PORT", 8005))
SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", 0))  # 0 means run once
FILE_OUTPUT_PATH = os.getenv("FILE_OUTPUT_PATH")  # if set, write JSONL instead of Kafka
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", 30))

SHOP = "iledebeaute"

# --- Prometheus metrics ---
PAGES_PARSED = Counter("scraper_pages_parsed_total", "Pages parsed", ["shop"])  # pages visited
REQUEST_ERRORS = Counter("scraper_request_errors_total", "Request errors", ["shop"])  # request errors
PRODUCTS_PUBLISHED = Counter("scraper_products_published_total", "Products published", ["shop"])  # product messages
LAST_SUCCESSFUL_RUN = Gauge("scraper_last_successful_run_timestamp", "Last successful run", ["shop"])  # timestamp

# --- scraper session ---
# cloudscraper helps bypass some anti-bot protections
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False}
)
scraper.headers.update(
    {
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": f"{BASE_URL}/",
        "User-Agent": "Mozilla/5.0",
    }
)


# --- Utility helpers ---
def text_of(elem, default=""):
    """Return normalized text of a BeautifulSoup element."""
    if not elem:
        return default
    return elem.get_text(" ", strip=True)


def normalize_url(url: str) -> str:
    """Canonicalize URL to compare visited pages reliably (strip fragment, sort query)."""
    if not url:
        return url
    parsed = urlsplit(url)
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    query_items = sorted(query_items, key=lambda x: x[0])
    query = urlencode(query_items, doseq=True)
    canonical = urlunsplit(parsed._replace(query=query, fragment=""))
    return canonical.rstrip("/ ")


def extract_gtin(soup: BeautifulSoup):
    """Try to extract GTIN from JSON-LD scripts on the product page."""
    for tag in soup.select('script[type="application/ld+json"]'):
        if not tag.string:
            continue
        try:
            data = json.loads(tag.string)
        except Exception:
            continue
        # data can be dict or list
        if isinstance(data, dict):
            entries = [data]
        else:
            entries = data
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            for field in ("gtin", "gtin8", "gtin12", "gtin13", "gtin14"):
                x = str(entry.get(field, ""))
                digits = re.sub(r"\D", "", x)
                if len(digits) in {8, 12, 13, 14}:
                    return digits
    return None


# --- Parsing product page ---
def parse_product(url: str):
    """Fetches a product page and extracts required fields.

    Returns a dict matching normalizer expectations (current_price_text, old_price_text, name, volume, etc.)
    """
    try:
        r = scraper.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Prefer localized product block near the title to avoid picking unrelated prices
        # Title
        title_el = soup.select_one("h1[itemprop='name'], h1")
        name = text_of(title_el)

        # Current price: element with itemprop="price" (content attribute contains numeric price)
        price_el = soup.select_one('[itemprop="price"]')
        current_price_text = None
        if price_el:
            # visible text like "3 496¤"
            current_price_text = text_of(price_el)
            # fallback: content attribute (numbers like "3496") -> format to human readable
            if not current_price_text and price_el.get("content"):
                content = price_el.get("content")
                # format: 3496 -> "3 496"
                current_price_text = re.sub(r"(?<=\d)(?=(\d{3})+$)", " ", str(content))

        # Old price: known wrapper class css-1nvlaef (observed on site)
        old_el = soup.select_one("div.css-1nvlaef")
        old_price_text = text_of(old_el) if old_el else None

        # Volume: specific block inside product listing (example: div.css-tkbfxj)
        volume_el = soup.select_one("div.css-tkbfxj, span.css-tkbfxj")
        volume = text_of(volume_el) if volume_el else None

        # in_stock: try to detect explicit phrases in the product block
        product_block = title_el and title_el.find_parent("div") or soup
        product_text = text_of(product_block)
        lower = product_text.lower()
        if "нет в наличии" in lower or "нет в продаже" in lower:
            in_stock = False
        elif "в наличии" in lower or "есть в наличии" in lower:
            in_stock = True
        else:
            # fallback: if current price exists, treat as in stock
            in_stock = bool(current_price_text)

        # GTIN
        gtin = extract_gtin(soup)

        product_id = url.rstrip("/").split("/")[-1]

        PAGES_PARSED.labels(shop=SHOP).inc()

        return {
            "shop": SHOP,
            "source_url": url,
            "source_product_id": product_id,
            "gtin": gtin,
            "name": name,
            "current_price_text": current_price_text,
            "old_price_text": old_price_text,
            "volume": volume,
            "in_stock": in_stock,
            "collected_at": datetime.now(UTC).isoformat(),
        }
    except Exception as error:
        REQUEST_ERRORS.labels(shop=SHOP).inc()
        print(f"Page error {url}: {error}")
        return None


# --- Catalog navigation ---
def parse_catalog_page(url: str):
    """Return list of product urls found on the catalog page and potential next page URL."""
    try:
        r = scraper.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # find product anchors
        hrefs = []
        for a in soup.select("a[href*='/product/']"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            hrefs.append(full)

        # next page candidates: 'Показать еще' or links with PAGEN_1= or page= in query
        next_candidates = []
        for a in soup.select("a[href]"):
            t = text_of(a).lower()
            href = a.get("href")
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            if "показать еще" in t or "показать ещё" in t:
                next_candidates.append(full)
            elif "pagen_1" in href.lower() or "page=" in href.lower():
                next_candidates.append(full)

        # pick first next candidate that's not the same as current
        current_norm = normalize_url(url)
        next_url = None
        for c in next_candidates:
            if normalize_url(c) != current_norm:
                next_url = c
                break

        return list(dict.fromkeys(hrefs)), next_url
    except Exception as error:
        REQUEST_ERRORS.labels(shop=SHOP).inc()
        print(f"Catalog page error {url}: {error}")
        return [], None


# --- Publishing ---
def publish_product_kafka(producer: Producer, product: dict):
    key = f"{product['shop']}:{product['source_product_id']}"
    producer.produce(RAW_PRODUCTS_TOPIC, key=key.encode("utf-8"), value=json.dumps(product, ensure_ascii=False).encode("utf-8"))
    producer.poll(0)
    PRODUCTS_PUBLISHED.labels(shop=SHOP).inc()


def write_product_file(fp, product: dict):
    fp.write(json.dumps(product, ensure_ascii=False) + "\n")
    PRODUCTS_PUBLISHED.labels(shop=SHOP).inc()


# --- Main run loop ---
def scrape_once(producer=None, file_fp=None):
    seen_urls = set()
    current_url = CATALOG_URL
    total_found = 0

    while current_url and normalize_url(current_url) not in seen_urls:
        seen_urls.add(normalize_url(current_url))
        print(f"Fetching catalog: {current_url}")
        product_urls, next_url = parse_catalog_page(current_url)
        product_urls = [u for u in product_urls if normalize_url(u) not in seen_urls]
        print(f"Found {len(product_urls)} product links on page")

        for idx, url in enumerate(product_urls, start=1):
            print(f"Parsing product {idx}/{len(product_urls)}: {url}")
            product = parse_product(url)
            if not product:
                continue
            total_found += 1
            if producer:
                publish_product_kafka(producer, product)
            elif file_fp:
                write_product_file(file_fp, product)

        if not next_url:
            break

        # avoid loops where next page equals current
        if normalize_url(next_url) in seen_urls:
            break

        current_url = next_url

    LAST_SUCCESSFUL_RUN.labels(shop=SHOP).set_to_current_time()
    print(f"Scraping finished: {total_found} products processed")


def main():
    # Start Prometheus metrics endpoint
    start_http_server(METRICS_PORT)

    producer = None
    file_fp = None

    # If FILE_OUTPUT_PATH is set, write JSONL instead of sending to Kafka
    if FILE_OUTPUT_PATH:
        os.makedirs(os.path.dirname(FILE_OUTPUT_PATH), exist_ok=True)
        file_fp = open(FILE_OUTPUT_PATH, "w", encoding="utf-8")
        print(f"Writing output to {FILE_OUTPUT_PATH}")
    else:
        if not KAFKA_BOOTSTRAP_SERVERS:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS must be set when FILE_OUTPUT_PATH is not provided")
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    try:
        if SCRAPE_INTERVAL_SECONDS and SCRAPE_INTERVAL_SECONDS > 0:
            while True:
                try:
                    scrape_once(producer=producer, file_fp=file_fp)
                except Exception as e:
                    REQUEST_ERRORS.labels(shop=SHOP).inc()
                    print(f"Scraper run error: {e}")
                print(f"Next run in {SCRAPE_INTERVAL_SECONDS} seconds")
                time.sleep(SCRAPE_INTERVAL_SECONDS)
        else:
            scrape_once(producer=producer, file_fp=file_fp)
    finally:
        if file_fp:
            file_fp.close()


if __name__ == "__main__":
    main()
