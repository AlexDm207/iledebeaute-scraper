import cloudscraper
from bs4 import BeautifulSoup

url = "https://iledebeaute.ru/catalog/tip-has_discount-iz-prom/"

scraper = cloudscraper.create_scraper()
r = scraper.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
print("status:", r.status_code)
print("html_len:", len(r.text))
print("contains_catalog:", "Скидки" in r.text or "скидки" in r.text.lower())

soup = BeautifulSoup(r.text, "html.parser")
for a in soup.select("a[href*='/product/']")[:10]:
    print(a.get("href"))