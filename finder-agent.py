#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tile Mailer Finder — Автоматический парсинг emails компаний
═══════════════════════════════════════════════════════════════
• Находит компании через 2GIS API + DuckDuckGo
• Парсит emails с сайтов компаний (BeautifulSoup)
• Проверяет emails через Hunter.io API
• Сохраняет в Google Sheets
"""

import requests
import re
import os
import time
import logging
import json as json_module
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import urljoin

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════

SHEET_ID      = os.environ.get('SHEET_ID', '')
CREDS_JSON    = os.environ.get('GOOGLE_CREDS', '')
TWOGIS_API_KEY = os.environ.get('TWOGIS_API_KEY', '')
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ══════════════════════════════════════════════════════
#  ПОИСКОВЫЕ ЗАПРОСЫ ДЛЯ DUCKDUCKGO
# ══════════════════════════════════════════════════════

SEARCH_QUERIES = [
    'строительная компания Санкт-Петербург сайт email',
    'дизайн-студия интерьера СПб контакты',
    'архитектурное бюро Санкт-Петербург',
    'ремонт квартир СПб компания email',
    'плитка керамогранит магазин СПб',
    'строительный магазин Санкт-Петербург',
    'управляющая компания СПб контакты',
    'гостиница отель Санкт-Петербург email',
    'ресторан кафе СПб официальный сайт',
    'спортивный клуб фитнес Санкт-Петербург',
    'салон красоты СПб сайт контакты',
    'отделочные работы Санкт-Петербург',
    'укладка плитки СПб мастера',
    'застройщик новостройки СПб контакты',
    'девелопер Ленинградская область',
    'торговый центр СПб администрация',
    'бизнес-центр Санкт-Петербург аренда',
    'дизайнер интерьера Всеволожск Гатчина',
    'строительство домов ЛО компания',
    'проектная организация СПб email',
    'ремонт ванной плитка СПб',
    'плиточник укладка кафель СПб',
    'мебельная компания Санкт-Петербург сайт',
    'сантехника ванные комнаты СПб контакты',
    'хостел мини-отель Санкт-Петербург',
]

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def get_sheet():
    try:
        creds_dict = json_module.loads(CREDS_JSON)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(SHEET_ID).sheet1
        log.info('✅ Google Sheets подключён')
        return sheet
    except Exception as ex:
        log.error(f'❌ Google Sheets error: {ex}')
        return None

def get_existing_emails(sheet):
    try:
        values = sheet.col_values(2)
        return set(e.lower().strip() for e in values if '@' in e)
    except:
        return set()

def add_to_sheet(sheet, name, website, email, source):
    try:
        sheet.append_row([
            name,
            email,
            website,
            source,
            datetime.now().isoformat()[:10],
            'new',
            '',
            ''
        ])
        log.info(f'  ✓ {name} → {email}')
        return True
    except Exception as ex:
        log.error(f'  ❌ Sheet error: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ 2GIS API (правильный endpoint)
# ══════════════════════════════════════════════════════

def search_2gis(query):
    """Ищет компании через 2GIS API v3"""
    if not TWOGIS_API_KEY:
        return []
    try:
        params = {
            'q': query,
            'key': TWOGIS_API_KEY,
            'fields': 'items.contact_groups,items.links',
            'page_size': 50,
        }
        res = requests.get(
            'https://catalog.api.2gis.com/3.0/items/search',
            params=params, timeout=10
        )
        data = res.json()
        items = data.get('result', {}).get('items', [])

        companies = []
        for item in items:
            name = item.get('name', '')
            website = ''
            for group in item.get('contact_groups', []):
                for contact in group.get('contacts', []):
                    if contact.get('type') == 'website':
                        website = contact.get('value', '')
                        break
            if not website:
                for link in item.get('links', []):
                    if link.get('type') == 'website':
                        website = link.get('value', '')
                        break
            if name:
                companies.append({'name': name, 'website': website})

        log.info(f'  2GIS: {len(companies)} компаний по запросу "{query}"')
        return companies
    except Exception as ex:
        log.error(f'  2GIS error: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ DUCKDUCKGO
# ══════════════════════════════════════════════════════

def search_duckduckgo(query):
    """Ищет сайты через DuckDuckGo"""
    try:
        url = f'https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}'
        res = requests.get(url, headers=HEADERS, timeout=12)
        soup = BeautifulSoup(res.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('http') and 'duckduckgo' not in href:
                links.append(href)
        unique = list(dict.fromkeys(links))[:6]
        log.info(f'  DuckDuckGo: {len(unique)} сайтов')
        return unique
    except Exception as ex:
        log.error(f'  DuckDuckGo error: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL СО САЙТА
# ══════════════════════════════════════════════════════

SKIP_EMAILS = {'noreply', 'no-reply', 'test', 'example', 'domain',
               'email', 'postmaster', 'webmaster', 'mailer-daemon'}

def is_good_email(email):
    local = email.split('@')[0].lower()
    if any(s in local for s in SKIP_EMAILS):
        return False
    if len(email) < 6 or len(email) > 60:
        return False
    if not re.match(r'^[\w.+\-]+@[\w\-]+\.[\w.]{2,}$', email):
        return False
    return True

def parse_emails_from_page(url):
    """Парсит emails с одной страницы"""
    try:
        res = requests.get(url, headers=HEADERS, timeout=8, allow_redirects=True)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        emails = set()
        for e in re.findall(r'[\w.+\-]+@[\w\-]+\.[\w.]{2,}', soup.get_text()):
            emails.add(e.lower())
        for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
            e = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
            if '@' in e:
                emails.add(e)
        return [e for e in emails if is_good_email(e)], soup
    except:
        return [], None

def extract_emails_from_url(url):
    """Ищет emails на сайте — главная + страница контактов"""
    if not url.startswith('http'):
        url = 'https://' + url

    emails, soup = parse_emails_from_page(url)

    # Если на главной пусто — ищем страницу контактов
    if not emails and soup:
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            text = a.get_text().lower()
            if any(k in href or k in text for k in ['contact', 'контакт', 'about', 'feedback']):
                full_url = href if href.startswith('http') else urljoin(url, href)
                extra, _ = parse_emails_from_page(full_url)
                emails.extend(extra)
                if emails:
                    break

    return list(set(emails))

# ══════════════════════════════════════════════════════
#  HUNTER.IO API
# ══════════════════════════════════════════════════════

def find_email_hunter(website):
    if not HUNTER_API_KEY or not website:
        return None
    try:
        domain = website.split('://')[1].split('/')[0].replace('www.', '') if '://' in website else website
        res = requests.get(
            'https://api.hunter.io/v2/domain-search',
            params={'domain': domain, 'api_key': HUNTER_API_KEY, 'limit': 5},
            timeout=10
        )
        emails = res.json().get('data', {}).get('emails', [])
        return emails[0].get('value') if emails else None
    except:
        return None

# ══════════════════════════════════════════════════════
#  ГЛАВНЫЙ ЦИКЛ
# ══════════════════════════════════════════════════════

def main():
    log.info('═════════════════════════════════════════════')
    log.info(' Tile Mailer Finder — запуск')
    log.info('═════════════════════════════════════════════')

    sheet = get_sheet()
    if not sheet:
        return

    existing = get_existing_emails(sheet)
    log.info(f'📊 В базе уже: {len(existing)} emails')
    total = 0

    for query in SEARCH_QUERIES:
        log.info(f'\n🔍 {query}')

        # Источник 1: 2GIS
        companies = search_2gis(query + ' Санкт-Петербург')

        # Источник 2: DuckDuckGo — парсим сайты напрямую
        links = search_duckduckgo(query)
        for link in links:
            domain = link.split('://')[1].split('/')[0] if '://' in link else link
            companies.append({'name': domain, 'website': link})

        # Обрабатываем все найденные компании
        seen = set()
        for company in companies:
            website = company.get('website', '')
            if not website or website in seen:
                continue
            seen.add(website)

            time.sleep(1)

            # Ищем email на сайте
            email = None
            emails = extract_emails_from_url(website)
            if emails:
                # Берём первый email не из базы
                for e in emails:
                    if e not in existing:
                        email = e
                        break

            # Если не нашли на сайте — ищем через Hunter
            if not email:
                candidate = find_email_hunter(website)
                if candidate and candidate not in existing:
                    email = candidate

            if email:
                add_to_sheet(sheet, company['name'], website, email, 'Web+2GIS')
                existing.add(email)
                total += 1

        time.sleep(2)

    log.info('\n═════════════════════════════════════════════')
    log.info(f'✅ Добавлено новых: {total}')
    log.info('═════════════════════════════════════════════')

if __name__ == '__main__':
    main()
