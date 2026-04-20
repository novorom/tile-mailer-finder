#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tile Mailer Finder — Автоматический парсинг emails компаний
═══════════════════════════════════════════════════════════════
• Находит компании через 2GIS API (бесплатно)
• Парсит emails с сайтов компаний (BeautifulSoup)
• Проверяет emails через Hunter.io API (парсинг корпоративных адресов)
• Сохраняет в Google Sheets
• Запускается по расписанию (каждый день ночью)
"""

import requests
import re
import os
import time
import logging
import json as json_module
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════

# Google Sheets
SHEET_ID = os.environ.get('SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

# 2GIS API
TWOGIS_API_KEY = os.environ.get('TWOGIS_API_KEY', '')
TWOGIS_API_URL = 'https://catalog.api.2gis.com/3.0/items/search'

# Yandex Search API (https://developer.tech.yandex.ru/ - Поиск по организациям)
YANDEX_API_KEY = os.environ.get('YANDEX_API_KEY', '')
YANDEX_API_URL = 'https://search-maps.yandex.ru/v1/'

# Hunter.io API
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')
HUNTER_API_URL = 'https://api.hunter.io/v2/domain-search'

# User Agent для парсинга
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# ══════════════════════════════════════════════════════
#  КАТЕГОРИИ И ПОИСКИ
# ══════════════════════════════════════════════════════

SEARCH_CATEGORIES = [
    'строительная компания',
    'дизайн-студия',
    'архитектурное бюро',
    'ремонт квартир',
    'магазин плитки',
    'строительный магазин',
    'управляющая компания',
    'гостиница',
    'ресторан',
    'спортивный клуб',
    'салон красоты',
    'отделочные работы',
    'укладка плитки',
    'застройщик',
    'девелопер',
    'ландшафтный дизайн',
    'торговый центр',
    'бизнес-центр'
]

LOCATIONS = [
    'Санкт-Петербург',
    'Всеволожск',
    'Кировск',
    'Павловск',
    'Пушкин',
    'Гатчина',
    'Петергоф',
    'Сестрорецк',
    'Выборг',
    'Кингисепп'
]

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def get_sheet():
    """Подключается к Google Sheets"""
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
        return sheet
    except Exception as ex:
        log.error(f'Google Sheets error: {ex}')
        return None

def add_company_to_sheet(sheet, company_name, website, email, source, category):
    """Добавляет компанию в Sheets с проверкой на дубликаты"""
    try:
        # Простая проверка на дубликат по email (читаем первый столбец B)
        # В идеале нужно хранить список в памяти, если таблица большая
        existing_emails = sheet.col_values(2) # Email в колонке B
        if email in existing_emails:
            log.info(f'× Пропуск (уже есть): {email}')
            return False

        sheet.append_row([
            company_name,      # Название
            email or '',       # Email
            website or '',     # Сайт
            source,            # Источник
            category,          # Категория
            datetime.now().isoformat(),  # Дата добавления
            'new',             # Статус
            ''                 # Примечания
        ])
        log.info(f'✓ Добавлено: {company_name} ({email})')
        return True
    except Exception as ex:
        log.error(f'Add to sheet error: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ 2GIS API
# ══════════════════════════════════════════════════════

def get_2gis_region_id(location):
    """Пытается получить region_id для города в 2GIS"""
    try:
        params = {'q': location, 'key': TWOGIS_API_KEY, 'type': 'adm_div.city'}
        res = requests.get('https://catalog.api.2gis.com/3.0/items', params=params, timeout=5)
        data = res.json()
        for item in data.get('result', {}).get('items', []):
            if item.get('subtype') == 'city' or item.get('type') == 'adm_div':
                return item.get('id')
    except:
        pass
    return None

def search_2gis(category, location):
    """
    Ищет компании в 2GIS
    """
    if not TWOGIS_API_KEY:
        log.warning('2GIS API key not set')
        return []
    
    try:
        # Пытаемся найти ID региона для более точного поиска
        region_id = get_2gis_region_id(location)
        
        params = {
            'q': category,
            'key': TWOGIS_API_KEY,
            'fields': 'items.contact_groups',
            'limit': 50
        }
        if region_id:
            params['region_id'] = region_id
        else:
            params['q'] = f"{category} {location}"
        
        response = requests.get(TWOGIS_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'error' in data:
            log.error(f"2GIS API error: {data.get('error', {}).get('message', 'Unknown error')}")
            return []
            
        companies = []
        for item in data.get('result', {}).get('items', []):
            website = ''
            phone = ''
            for group in item.get('contact_groups', []):
                for contact in group.get('contacts', []):
                    if contact.get('type') == 'website':
                        website = contact.get('value', '')
                    elif contact.get('type') == 'phone':
                        phone = contact.get('value', '')
            
            companies.append({
                'name': item.get('name'),
                'website': website,
                'phone': phone,
                'address': item.get('address_name', ''),
                'category': category
            })
        
        log.info(f'2GIS: найдено {len(companies)} компаний ({category} в {location}, region_id: {region_id})')
        return companies
    except Exception as ex:
        log.error(f'2GIS search error: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ YANDEX API
# ══════════════════════════════════════════════════════

def search_yandex(category, location):
    """
    Ищет компании через Яндекс Поиск по организациям
    """
    if not YANDEX_API_KEY:
        log.warning('Yandex API key not set (YANDEX_API_KEY)')
        return []
    
    try:
        query = f"{category} {location}"
        params = {
            'apikey': YANDEX_API_KEY,
            'text': query,
            'lang': 'ru_RU',
            'type': 'biz',
            'results': 50
        }
        
        response = requests.get(YANDEX_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        companies = []
        for feature in data.get('features', []):
            props = feature.get('properties', {}).get('CompanyMetaData', {})
            
            website = props.get('url', '')
            phone = ''
            phones = props.get('Phones', [])
            if phones:
                phone = phones[0].get('formatted', '')
                
            companies.append({
                'name': props.get('name'),
                'website': website,
                'phone': phone,
                'address': props.get('address', ''),
                'category': category
            })
            
        log.info(f'Yandex: найдено {len(companies)} компаний по запросу "{query}"')
        return companies
    except Exception as ex:
        log.error(f'Yandex search error: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL СО САЙТА
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    """Извлекает emails со сайта (главная + страница контактов)"""
    if not url.startswith('http'):
        url = 'http://' + url
        
    def find_in_page(page_url):
        try:
            res = requests.get(page_url, headers=HEADERS, timeout=10)
            res.raise_for_status()
            text = res.text
            soup = BeautifulSoup(text, 'html.parser')
            
            found = set()
            # Поиск в тексте
            for email in re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', soup.get_text()):
                found.add(email.lower())
            
            # Поиск в mailto
            for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
                email = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if '@' in email:
                    found.add(email)
            return found, soup
        except:
            return set(), None

    emails, soup = find_in_page(url)
    
    # Если на главной пусто — ищем страницу контактов
    if not emails and soup:
        contact_links = []
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            text = a.get_text().lower()
            if any(k in href or k in text for k in ['contact', 'контакт', 'about', 'о-нас', 'feedback']):
                full_url = href
                if not href.startswith('http'):
                    from urllib.parse import urljoin
                    full_url = urljoin(url, href)
                contact_links.append(full_url)
        
        for link in list(set(contact_links))[:3]: # Проверяем максимум 3 ссылки
            extra_emails, _ = find_in_page(link)
            emails.update(extra_emails)

    # Фильтруем заведомо мусорные emails
    filtered = []
    garbage = {'noreply@', 'test@', 'example@', 'domain@', 'email@'}
    for email in emails:
        if not any(g in email for g in garbage):
            # Простейшая валидация длины и символов
            if 5 < len(email) < 50 and '.' in email.split('@')[1]:
                filtered.append(email)
    
    return list(set(filtered))

# ══════════════════════════════════════════════════════
#  ПОИСК EMAIL ЧЕРЕЗ HUNTER.IO
# ══════════════════════════════════════════════════════

def find_email_hunter(domain, company_name):
    """
    Ищет корпоративные emails через Hunter.io Domain Search
    """
    if not HUNTER_API_KEY:
        return None
    
    try:
        # Извлекаем домен
        if '://' in domain:
            domain = domain.split('://')[1].split('/')[0]
        domain = domain.replace('www.', '')
        
        params = {
            'domain': domain,
            'api_key': HUNTER_API_KEY,
            'limit': 10
        }
        
        response = requests.get(HUNTER_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        emails = data.get('data', {}).get('emails', [])
        if emails:
            # Возвращаем первый найденный email
            return emails[0].get('value')
        
        return None
    
    except Exception as ex:
        log.debug(f'Hunter.io error: {ex}')
        return None

# ══════════════════════════════════════════════════════
#  ГЛАВНЫЙ ПРОЦЕСС
# ══════════════════════════════════════════════════════

def main():
    log.info('═════════════════════════════════════════════')
    log.info(' Tile Mailer Finder — запуск поиска')
    log.info('═════════════════════════════════════════════')
    
    sheet = get_sheet()
    if not sheet:
        log.error('❌ Не могу подключиться к Google Sheets')
        return
    
    total_found = 0
    
    # Ищем по каждой категории и местоположению
    for location in LOCATIONS:
        for category in SEARCH_CATEGORIES:
            log.info(f'\n🔍 Ищу: {category} в {location}')
            
            # Способ 1: 2GIS API
            companies_2gis = search_2gis(category, location)
            
            # Способ 2: Yandex API
            companies_yandex = search_yandex(category, location)
            
            # Объединяем результаты (убираем дубли по сайту, если он есть)
            all_companies = companies_2gis + companies_yandex
            unique_companies = []
            seen_sites = set()
            
            for c in all_companies:
                site = c['website'].lower().strip('/') if c['website'] else None
                if site:
                    if site not in seen_sites:
                        seen_sites.add(site)
                        unique_companies.append(c)
                else:
                    unique_companies.append(c)

            for company in unique_companies:
                time.sleep(1)  # Rate limit
                
                # Пытаемся найти email на сайте
                email = None
                if company['website']:
                    emails = extract_emails_from_url(company['website'])
                    if emails:
                        email = emails[0]
                
                # Если email не найден на сайте — ищем через Hunter.io
                if not email and company['website']:
                    email = find_email_hunter(company['website'], company['name'])
                
                # Добавляем в Sheets если нашли email
                if email:
                    add_company_to_sheet(
                        sheet,
                        company['name'],
                        company['website'],
                        email,
                        source='Search API',
                        category=category
                    )
                    total_found += 1
                else:
                    log.info(f'⚠ Email не найден: {company["name"]}')
            
            time.sleep(2)  # Rate limit между поисками
    
    log.info('\n═════════════════════════════════════════════')
    log.info(f'✅ Найдено {total_found} компаний с email')
    log.info('═════════════════════════════════════════════')

if __name__ == '__main__':
    main()
