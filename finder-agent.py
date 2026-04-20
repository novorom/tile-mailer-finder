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

# 2GIS API (бесплатно, нужна регистрация)
TWOGIS_API_KEY = os.environ.get('TWOGIS_API_KEY', '')
TWOGIS_API_URL = 'https://catalog.api.2gis.com/3.0/items/search'

# Hunter.io API (бесплатно 50 запросов/день, нужна регистрация)
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')
HUNTER_API_URL = 'https://api.hunter.io/v2/email-finder'

# User Agent для парсинга
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# ══════════════════════════════════════════════════════
#  КАТЕГОРИИ И ПОИСКИ
# ══════════════════════════════════════════════════════

# Категории 2GIS (нужно уточнить коды для СПб)
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
]

LOCATIONS = [
    'Санкт-Петербург',
    'Всеволожск',
    'Кировск',
    'Павловск',
    'Пушкин',
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
    """Добавляет компанию в Sheets"""
    try:
        sheet.append_row([
            company_name,      # Название
            email or '',       # Email
            website or '',     # Сайт
            source,            # Источник (2GIS / Hunter / сайт)
            category,          # Категория
            datetime.now().isoformat(),  # Дата добавления
            'active',          # Статус
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

def search_2gis(category, location):
    """
    Ищет компании в 2GIS
    
    Требует:
    1. Регистрация на https://dev.2gis.com
    2. API ключ из личного кабинета
    3. Переменная окружения TWOGIS_API_KEY
    """
    if not TWOGIS_API_KEY:
        log.warning('2GIS API key not set')
        return []
    
    try:
        params = {
            'q': category,
            'city': location,
            'key': TWOGIS_API_KEY,
            'limit': 50
        }
        
        response = requests.get(TWOGIS_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        companies = []
        for item in data.get('result', {}).get('items', []):
            company = {
                'name': item.get('name'),
                'website': item.get('website', ''),
                'phone': item.get('phone', ''),
                'address': item.get('address', ''),
                'category': category
            }
            companies.append(company)
        
        log.info(f'2GIS: найдено {len(companies)} компаний ({category} в {location})')
        return companies
    
    except Exception as ex:
        log.error(f'2GIS search error: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL СО САЙТА
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    """Извлекает emails со сайта"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=8)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        emails = set()
        
        # Поиск в тексте
        for email in re.findall(r'[\w.+\-]+@[\w\-]+\.[\w.]{2,}', soup.get_text()):
            emails.add(email.lower())
        
        # Поиск в mailto ссылках
        for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
            email = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
            if '@' in email:
                emails.add(email)
        
        # Фильтруем generic emails
        filtered = []
        generic = {'info@', 'admin@', 'support@', 'noreply@', 'test@'}
        for email in emails:
            if not any(email.startswith(g) for g in generic):
                filtered.append(email)
        
        return filtered
    
    except Exception as ex:
        log.debug(f'Parse error {url}: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПОИСК EMAIL ЧЕРЕЗ HUNTER.IO
# ══════════════════════════════════════════════════════

def find_email_hunter(domain, company_name):
    """
    Ищет корпоративные emails через Hunter.io
    
    Требует:
    1. Регистрация на https://hunter.io
    2. API ключ из личного кабинета
    3. Бесплатно: 50 запросов/день
    4. Переменная окружения HUNTER_API_KEY
    """
    if not HUNTER_API_KEY:
        return None
    
    try:
        # Извлекаем домен если передана полная ссылка
        if '://' in domain:
            domain = domain.split('://')[1].split('/')[0]
        
        params = {
            'domain': domain,
            'company': company_name,
            'type': 'generic',  # general, personal
        }
        headers = {'Authorization': f'Bearer {HUNTER_API_KEY}'}
        
        response = requests.get(HUNTER_API_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('data', {}).get('email'):
            return data['data']['email']
        
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
            companies = search_2gis(category, location)
            
            for company in companies:
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
                        source='2GIS',
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
