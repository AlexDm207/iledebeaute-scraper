# iledebeaute-scraper

Минимальный стартовый проект для парсинга каталога скидок ИЛЬ ДЕ БОТЭ.

## Установка

```bash
python -m pip install -r requirements.txt
```

## Запуск

```bash
python scraper.py
```

## Что проверять

- доступность страницы;
- наличие ссылок на карточки товара;
- наличие заголовков, цен, URL товаров;
- при необходимости — добавление User-Agent/headers или `cloudscraper`.

## Структура

- `scraper.py` — базовый парсер каталога;
- `requirements.txt` — зависимости;
- `README.md` — краткая инструкция.
