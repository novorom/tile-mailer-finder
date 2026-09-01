#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
America Remote Job Finder — Поиск удаленной работы в США
═══════════════════════════════════════════════════════════════
• Поиск удаленных позиций Sales Manager / Export Sales Manager в США
• B2B продажи, экспорт, бизнес развитие
• Керамическая промышленность, строительство, материалы
• Парсинг emails с сайтов компаний
• Сохранение в Google Sheets (столбцы D, E, F)
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
import google.generativeai as genai
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════

SHEET_ID = os.environ.get('SPAIN_SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID', '')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
GEMINI_API_KEY_2 = os.environ.get('GEMINI_API_KEY_2', '')  # Альтернативный ключ
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Категории для поиска удаленной работы в США
REMOTE_JOB_CATEGORIES = [
    # Remote Sales Manager positions
    'remote sales manager USA',
    'remote export sales manager',
    'remote B2B sales representative',
    'remote account manager',
    'remote business development manager',
    'remote sales director',
    'work from home sales positions',
    'remote sales jobs USA',
    
    # Industry-specific remote sales
    'remote sales manager construction materials',
    'remote sales manager ceramic industry',
    'remote sales manager building materials',
    'remote export sales manager USA',
    'remote international sales manager',
    
    # Remote commercial roles
    'remote commercial director',
    'remote business development director',
    'remote account executive',
    'remote sales representative',
    
    # Location-specific remote work
    'remote sales jobs California',
    'remote sales jobs New York',
    'remote sales jobs Texas',
    'remote sales jobs Florida',
    
    # Company types
    'remote sales manufacturing companies',
    'remote sales distribution companies',
    'remote sales wholesale companies',
    'remote sales export companies',
]

# Статический список американских компаний для удаленной работы (fallback)
STATIC_AMERICAN_COMPANIES = [
    # Керамическая промышленность
    {'name': 'Mohawk Industries', 'website': 'https://www.mohawkind.com'},
    {'name': 'Dal-Tile Corporation', 'website': 'https://www.daltile.com'},
    {'name': 'Shaw Industries', 'website': 'https://www.shawinc.com'},
    {'name': 'Armstrong World Industries', 'website': 'https://www.armstrongceilings.com'},
    {'name': 'Interface Inc', 'website': 'https://www.interface.com'},
    {'name': 'Crossville Inc', 'website': 'https://www.crossvilleinc.com'},
    {'name': 'Florida Tile', 'website': 'https://www.floridatile.com'},
    
    # Строительные материалы
    {'name': 'Home Depot', 'website': 'https://www.homedepot.com'},
    {'name': 'Lowe\'s Companies', 'website': 'https://www.lowes.com'},
    {'name': 'Builders FirstSource', 'website': 'https://www.bldr.com'},
    {'name': '84 Lumber', 'website': 'https://www.84lumber.com'},
    {'name': 'Menards', 'website': 'https://www.menards.com'},
    
    # Оптовые дистрибьюторы
    {'name': 'Ferguson Enterprises', 'website': 'https://www.ferguson.com'},
    {'name': 'Watsco Inc', 'website': 'https://www.watsco.com'},
    {'name': 'HD Supply', 'website': 'https://www.hdsupply.com'},
    {'name': 'Sonepar', 'website': 'https://www.sonepar.com'},
    
    # Экспорт и международная торговля
    {'name': 'C.H. Robinson', 'website': 'https://www.chrobinson.com'},
    {'name': 'Expeditors International', 'website': 'https://www.expeditors.com'},
    {'name': 'Flexport', 'website': 'https://www.flexport.com'},
    {'name': 'Kuehne+Nagel', 'website': 'https://home.kuehne-nagel.com'},
    
    # Производство и дистрибуция
    {'name': 'USG Corporation', 'website': 'https://www.usg.com'},
    {'name': 'Owens Corning', 'website': 'https://www.owenscorning.com'},
    {'name': 'Johns Manville', 'website': 'https://www.jm.com'},
]

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def get_sheet():
    creds_data = json_module.loads(CREDS_JSON)
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json_module.dump(creds_data, f)
        creds_file = f.name
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(creds_file, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1
    return sheet

def add_company_to_sheet(sheet, company_name, email, website, job_title):
    """Добавляет компанию в столбцы D, E, F (company, email, website, job_title)"""
    try:
        # Находим первую пустую строку в столбце D
        all_values = sheet.col_values(4)  # Столбец D
        next_row = len(all_values) + 1
        
        # Добавляем данные в столбцы D, E, F
        sheet.update_cell(next_row, 4, company_name)  # D
        sheet.update_cell(next_row, 5, email)  # E
        sheet.update_cell(next_row, 6, website)  # F
        sheet.update_cell(next_row, 7, job_title)  # G (job_title)
        
        log.info(f'  ➕ Добавлено: {company_name} | {email}')
        return True
    except Exception as ex:
        log.warning(f'Ошибка добавления в таблицу: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ GOOGLE CUSTOM SEARCH
# ══════════════════════════════════════════════════════

def search_google_web(query):
    """Поиск через Google Custom Search API"""
    url = 'https://www.googleapis.com/customsearch/v1'
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_CSE_ID,
        'q': query,
        'num': 10
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = []
        if 'items' in data:
            for item in data['items']:
                results.append({
                    'title': item.get('title', ''),
                    'link': item.get('link', ''),
                    'snippet': item.get('snippet', '')
                })
        return results
    except Exception as ex:
        log.warning(f'Ошибка Google Search: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    """Извлекает emails с веб-страницы"""
    log.info(f"       Парсинг: {url}")
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        emails = set()
        
        # Поиск в mailto ссылках
        for mailto in soup.select('a[href^="mailto:"]'):
            email = mailto['href'].replace('mailto:', '').split('?')[0].strip()
            if '@' in email:
                emails.add(email.lower())
        
        # Поиск в тексте (регулярка)
        text = soup.get_text()
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        found_emails = re.findall(email_pattern, text)
        for email in found_emails:
            # Исключаем emails с цифрами перед @
            if not re.search(r'\d+@', email):
                emails.add(email.lower())
        
        # Поиск в data-email атрибутах
        for elem in soup.find_all(attrs={'data-email': True}):
            email = elem['data-email']
            if '@' in email:
                emails.add(email.lower())
        
        # Поиск в meta тегах
        for meta in soup.find_all('meta'):
            if meta.get('name') in ['email', 'contact-email', 'reply-to']:
                content = meta.get('content', '')
                if '@' in content:
                    emails.add(content.lower())
        
        # Фильтрация
        valid_emails = []
        skip_patterns = ['example', 'test', 'noreply', 'no-reply', 'donotreply', 
                        'support', 'admin', 'postmaster', 'webmaster', 'spam']
        
        for email in emails:
            email_lower = email.lower()
            if not any(pattern in email_lower for pattern in skip_patterns):
                if len(email) < 80:
                    valid_emails.append(email)
        
        return valid_emails
    except Exception as ex:
        log.warning(f'Ошибка парсинга {url}: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ GEMINI AI
# ══════════════════════════════════════════════════════

def search_gemini_leads(query):
    """Поиск компаний через Gemini AI с альтернативным ключом"""
    api_keys = [GEMINI_API_KEY, GEMINI_API_KEY_2]
    
    for api_key in api_keys:
        if not api_key:
            continue
            
        try:
            genai.configure(api_key=api_key)
            models_to_try = ['gemini-3.1-flash-lite', 'gemini-1.5-flash', 'gemini-pro']
            
            for model_name in models_to_try:
                try:
                    model = genai.GenerativeModel(model_name)
                    # Тестовый запрос для проверки доступности
                    model.generate_content("test", generation_config={"max_output_tokens": 1})
                    log.info(f'     [Gemini AI] модель: {model_name}')
                    
                    prompt = f"""
                    Find 5-10 US companies that are hiring for: {query}
                    
                    Return ONLY a JSON array with this format:
                    [
                        {{
                            "name": "Company Name",
                            "website": "https://example.com"
                        }}
                    ]
                    
                    Focus on companies in: ceramic industry, construction materials, manufacturing, wholesale, export.
                    """
                    
                    response = model.generate_content(prompt, generation_config={"max_output_tokens": 2000})
                    text = response.text
                    
                    # Извлечение JSON из ответа
                    json_match = re.search(r'\[.*\]', text, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                        companies = json_module.loads(json_str)
                        return companies
                    
                except Exception as ex:
                    log.warning(f'Gemini {model_name} error: {ex}')
                    continue
                    
        except Exception as ex:
            log.warning(f'Gemini API key error: {ex}')
            continue
    
    return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ HUNTER.IO
# ══════════════════════════════════════════════════════

def search_hunter_emails(domain):
    """Поиск emails через Hunter.io"""
    if not HUNTER_API_KEY:
        return []
    
    try:
        url = f'https://api.hunter.io/v2/email-finder?domain={domain}&api_key={HUNTER_API_KEY}'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('data', {}).get('email'):
            return [data['data']['email']]
        return []
    except Exception as ex:
        log.warning(f'Hunter.io error: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════

def main():
    log.info('═══════════════════════════════════════════')
    log.info(' America Remote Job Finder — запуск')
    log.info('═══════════════════════════════════════════')
    
    sheet = get_sheet()
    
    # Загружаем существующие emails из столбца E
    try:
        existing_emails = set()
        col_e_values = sheet.col_values(5)  # Столбец E
        for email in col_e_values:
            if email and '@' in email:
                existing_emails.add(email.lower())
        log.info(f'Существующих email в базе: {len(existing_emails)}')
    except Exception as e:
        log.warning(f'Ошибка загрузки существующих email: {e}')
        existing_emails = set()
    
    total_added = 0
    processed_domains = set()
    
    # Сначала обрабатываем статический список компаний
    log.info(f'Статических компаний: {len(STATIC_AMERICAN_COMPANIES)}')
    for company in STATIC_AMERICAN_COMPANIES:
        domain = urlparse(company['website']).netloc.lower().replace('www.', '')
        if domain not in processed_domains:
            processed_domains.add(domain)
            log.info(f'   » {company["name"]} (Static)')
            
            # Проверяем существование сайта
            if not check_site_exists(company['website']):
                log.info(f'     [!] Сайт недоступен')
                continue
            
            # Парсим emails с сайта
            emails = extract_emails_from_url(company['website'])
            
            for email in emails:
                if email.lower() not in existing_emails:
                    if add_company_to_sheet(sheet, company['name'], email, company['website'], 'Static'):
                        existing_emails.add(email.lower())
                        total_added += 1
                        log.info(f'     [OK] Email: {email}')
                else:
                    log.info(f'     [!] Email уже есть в базе')
            
            time.sleep(2)
    
    # Затем пробуем поиск через категории (если Google Search работает)
    google_search_works = False
    try:
        test_search = search_google_web('test')
        if test_search:
            google_search_works = True
    except:
        google_search_works = False
    
    if not google_search_works:
        log.warning('Google Search недоступен (403), пропускаем динамический поиск')
    else:
        for category in REMOTE_JOB_CATEGORIES:
            log.info(f'\n🔍 Категория: {category}')
            
            candidates = []
            
            # Google Search (основной источник)
            try:
                google_results = search_google_web(category)
                for result in google_results[:5]:
                    website = result.get('link', '')
                    if website:
                        domain = urlparse(website).netloc.lower().replace('www.', '')
                        company_name = domain.split('.')[0].capitalize()
                        candidates.append({
                            'name': company_name,
                            'website': website,
                            'source': 'Google Search'
                        })
            except Exception as e:
                log.warning(f'Google Search error: {e}')
            
            # Gemini fallback (с альтернативным ключом)
            try:
                gemini_results = search_gemini_leads(category)
                for company in gemini_results:
                    if 'name' in company and 'website' in company:
                        candidates.append({
                            'name': company['name'],
                            'website': company['website'],
                            'source': 'Gemini AI'
                        })
            except Exception as e:
                log.warning(f'Gemini error: {e}')
            
            # Уникализация по домену
            unique = {}
            for c in candidates:
                domain = None
                if c.get('website'):
                    try:
                        domain = urlparse(c['website']).netloc.lower().replace('www.', '')
                    except:
                        pass
                
                if domain and domain not in unique:
                    unique[domain] = c
            
            log.info(f'   Уникальных компаний: {len(unique)}')
            
            for domain, company in unique.items():
                if domain in processed_domains:
                    log.info(f'     [!] Домен {domain} уже обрабатывался')
                    continue
                
                processed_domains.add(domain)
                
                website = company.get('website')
                company_name = company.get('name', domain)
                
                log.info(f'   » {company_name} ({company.get("source")})')
                
                # Проверяем существование сайта
                if not check_site_exists(website):
                    log.info(f'     [!] Сайт недоступен')
                    continue
                
                # Парсим emails с сайта
                emails = extract_emails_from_url(website)
                
                for email in emails:
                    if email.lower() not in existing_emails:
                        if add_company_to_sheet(sheet, company_name, email, website, category):
                            existing_emails.add(email.lower())
                            total_added += 1
                            log.info(f'     [OK] Email: {email}')
                    else:
                        log.info(f'     [!] Email уже есть в базе')
                
                time.sleep(2)  # Задержка между запросами
            
            time.sleep(3)  # Задержка между категориями
    
    log.info('═══════════════════════════════════════════')
    log.info(f'ИТОГ: добавлено компаний: {total_added}')
    log.info('═══════════════════════════════════════════')

def check_site_exists(url):
    """Проверяет доступность сайта"""
    try:
        response = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        return response.status_code < 400
    except:
        return False

if __name__ == '__main__':
    main()
