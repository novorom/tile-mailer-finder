#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tile Mailer Finder — Автоматический парсинг emails компаний
═══════════════════════════════════════════════════════════════
• Находит компании через Google Places API, Google Web и Прямой парсинг каталогов
• Каталоги: Zoon.ru, Orgpage.ru, Flamp.ru, Yell.ru
• Парсит emails с сайтов компаний (BeautifulSoup + Google Gemini AI)
• Проверяет/ищет через Hunter.io API
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
import google.generativeai as genai
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

SHEET_ID = os.environ.get('SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID', '')   # Для Google Custom Search
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '') # Для умного поиска email
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
}

SEARCH_CATEGORIES = [
    'строительство домов',
    'дизайн интерьера',
    'архитектурное проектирование',
    'ремонт квартир под ключ',
    'керамическая плитка спб',
    'сантехника оптом',
    'магазин напольных покрытий'
]

LOCATIONS = ['Санкт-Петербург']

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def get_sheet():
    if not SHEET_ID or not CREDS_JSON:
        log.warning('Google Sheets credentials not set (SHEET_ID or GOOGLE_CREDS)')
        return None
    try:
        creds_dict = json_module.loads(CREDS_JSON)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(SHEET_ID).sheet1
        log.info('✅ Google Sheets подключён')
        return sheet
    except Exception as ex:
        log.error(f'❌ Google Sheets error: {ex}')
        return None

def add_company_to_sheet(sheet, email):
    if not sheet:
        log.info(f'[NO SHEET] Found: {email}')
        return False
    try:
        # Проверка на дубликат по email в колонке A
        existing_emails = sheet.col_values(1)
        if email in existing_emails:
            return False

        sheet.append_row([email])
        log.info(f'✓ Добавлено: {email}')
        return True
    except Exception as ex:
        log.error(f'❌ Add to sheet error: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  GOOGLE SEARCH APIs
# ══════════════════════════════════════════════════════

def search_google_places(category, location):
    if not GOOGLE_API_KEY:
        log.debug("Google API Key not set")
        return []
    log.info(f"     [Google Maps] поиск: {category}...")
    try:
        # Пытаемся использовать New Places API (searchText)
        url = "https://places.googleapis.com/v1/places:searchText"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": GOOGLE_API_KEY,
            "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.websiteUri"
        }
        data = {"textQuery": f"{category} {location}", "languageCode": "ru", "maxResultCount": 20}
        res = requests.post(url, headers=headers, json=data, timeout=10)
        
        if res.status_code == 200:
            results = res.json().get('places', [])
            companies = []
            for p in results:
                companies.append({
                    'name': p.get('displayName', {}).get('text'),
                    'website': p.get('websiteUri', ''),
                    'address': p.get('formattedAddress', ''),
                    'source': 'Google Maps'
                })
            return companies
        else:
            log.error(f"     [Google Maps] ошибка API: {res.status_code} {res.text[:100]}")
            # Пытаемся использовать старый Text Search как основной резерв
            log.info("     [Google Maps] пробуем старый API (Text Search)...")
            old_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
            params = {"query": f"{category} {location}", "key": GOOGLE_API_KEY, "language": "ru"}
            res_old = requests.get(old_url, params=params, timeout=10)
            if res_old.status_code == 200:
                data = res_old.json()
                old_companies = []
                for item in data.get('results', []):
                    old_companies.append({
                        'name': item.get('name'),
                        'website': None, 
                        'place_id': item.get('place_id'),
                        'source': 'Google Maps (Old)'
                    })
                log.info(f"     [Google Maps Old] найдено: {len(old_companies)}")
                return old_companies
            else:
                log.error(f"     [Google Maps Old] тоже ошибка: {res_old.status_code}")
                return []
    except Exception as e:
        log.debug(f"Google Places error: {e}")
        return []

def search_google_web(category, location, num=10):
    """Поиск сайтов через Google Custom Search"""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return []
    
    log.info(f"     [Google Web] поиск: {category}...")
    try:
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'q': f"{category} {location}",
            'key': GOOGLE_API_KEY,
            'cx': GOOGLE_CSE_ID,
            'num': num,
            'lr': 'lang_ru'
        }
        res = requests.get(url, params=params, timeout=10)
        items = res.json().get('items', [])
        companies = []
        for item in items:
            name = item.get('title', '').split('—')[0].split('|')[0].strip()
            companies.append({
                'name': name,
                'website': item.get('link'),
                'source': 'Google Search'
            })
        log.info(f"     [Google Web] найдено: {len(companies)}")
        return companies
    except Exception as e:
        log.error(f"     [Google Web] ошибка: {e}")
        return []

# ══════════════════════════════════════════════════════
#  DUCKDUCKGO SEARCH (Бесплатный резерв)
# ══════════════════════════════════════════════════════

def search_duckduckgo(category, location, num=5):
    """Поиск сайтов через DuckDuckGo (без ключей)"""
    log.info(f"     [DuckDuckGo] поиск: {category}...")
    try:
        query = f"{category} {location} спб контакты"
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        companies = []
        for link in soup.select('a.result__a')[:num]:
            href = link['href']
            if 'http' in href and 'duckduckgo.com' not in href:
                companies.append({
                    'name': link.get_text(strip=True),
                    'website': href,
                    'source': 'DuckDuckGo'
                })
        log.info(f"     [DuckDuckGo] найдено: {len(companies)}")
        return companies
    except Exception as e:
        log.debug(f"DuckDuckGo error: {e}")
        return []

# ══════════════════════════════════════════════════════
#  GEMINI LEAD GENERATION (План "Б")
# ══════════════════════════════════════════════════════

def search_gemini_leads(category, location, num=40):
    """Генерация списка компаний через Gemini, если поиск не дал результатов"""
    if not GEMINI_API_KEY:
        return []
    
    log.info(f"     [Gemini AI] генерация списка компаний: {category}...")
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # Пробуем доступные модели по очереди
        models_to_try = ['gemini-3.1-flash-lite-preview', 'gemini-1.5-flash', 'gemini-pro']
        model = None
        for m_name in models_to_try:
            try:
                model = genai.GenerativeModel(m_name)
                # Пробный запрос
                model.generate_content("test", generation_config={"max_output_tokens": 1})
                log.info(f"     [Gemini AI] использую модель: {m_name}")
                break
            except:
                continue
        
        if not model:
            log.error("     [Gemini AI] не удалось найти доступную модель")
            return []
        
        prompt = (
            f"Составь список из {num} известных компаний в сфере '{category}' в городе {location}. "
            "Для каждой компании укажи её название и, если знаешь, официальный сайт. "
            "Верни результат в формате JSON: [{\"name\": \"...\", \"website\": \"...\"}]"
        )
        
        response = model.generate_content(prompt)
        res_text = response.text.strip()
        
        # Если это не JSON, пробуем просто распарсить по строкам
        if '[' not in res_text:
            log.info("     [Gemini AI] ответ в текстовом формате, извлекаю сайты...")
            companies = []
            import re
            # Ищем домены
            domains = re.findall(r'[a-zA-Z0-9.-]+\.(?:ru|com|net|org|su)', res_text)
            for d in list(set(domains))[:num]:
                companies.append({
                    'name': d.split('.')[0].capitalize(),
                    'website': f"https://{d}",
                    'source': 'Gemini AI (Memory)'
                })
            return companies
        
        # Извлекаем JSON из ответа
        if '```json' in res_text:
            res_text = res_text.split('```json')[1].split('```')[0].strip()
        
        data = json_module.loads(res_text)
        for item in data:
            item['source'] = 'Gemini AI'
        log.info(f"     [Gemini AI] создано лидов: {len(data)}")
        return data
    except Exception as e:
        log.error(f"     [Gemini AI] ошибка: {e}")
        return []

# ══════════════════════════════════════════════════════
#  GOOGLE GEMINI (Умное извлечение email)
# ══════════════════════════════════════════════════════

def extract_emails_with_gemini(html_content):
    """Использует Gemini для поиска email в тексте страницы"""
    if not GEMINI_API_KEY:
        return []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for s in soup(['script', 'style', 'nav', 'footer']): s.decompose()
        text = soup.get_text(separator=' ', strip=True)[:10000]

        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = (
            "Найди все корпоративные email-адреса в тексте ниже. "
            "Верни ТОЛЬКО список адресов через запятую. Если адресов нет, напиши 'NONE'.\n\n"
            f"Текст:\n{text}"
        )
        
        response = model.generate_content(prompt)
        res_text = response.text.strip()
        
        if 'NONE' in res_text.upper():
            return []
            
        emails = [e.strip().lower() for e in res_text.split(',') if '@' in e]
        return list(set(emails))
    except Exception as e:
        log.debug(f"Gemini error: {e}")
        return []

# ══════════════════════════════════════════════════════
#  СКРЕЙПИНГ КАТАЛОГОВ
# ══════════════════════════════════════════════════════

def scrape_zoon(query):
    try:
        url = f"https://zoon.ru/search/?query%5B%5D={query}&city=spb"
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        companies = []
        for item in soup.select('div.search-results-item')[:10]:
            link = item.select_one('a.title-link')
            if link:
                name = link.get_text(strip=True)
                href = link['href']
                if not href.startswith('http'): href = "https://zoon.ru" + href
                companies.append({'name': name, 'profile_url': href, 'source': 'Zoon'})
        
        for c in companies:
            try:
                time.sleep(0.5)
                p_res = requests.get(c['profile_url'], headers=HEADERS, timeout=10)
                p_soup = BeautifulSoup(p_res.text, 'html.parser')
                site = p_soup.select_one('a.js-service-website')
                if site: c['website'] = site['href'].split('?')[0].strip('/')
            except: pass
        return companies
    except: return []

def scrape_orgpage(query):
    try:
        url = f"https://www.orgpage.ru/поиск/?query={query}&location=Санкт-Петербург"
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        companies = []
        for item in soup.select('div.result-item')[:10]:
            link = item.select_one('a.item-title')
            if link:
                name = link.get_text(strip=True)
                href = link['href']
                if not href.startswith('http'): href = "https://www.orgpage.ru" + href
                companies.append({'name': name, 'profile_url': href, 'source': 'Orgpage'})
        
        for c in companies:
            try:
                time.sleep(0.5)
                p_res = requests.get(c['profile_url'], headers=HEADERS, timeout=10)
                p_soup = BeautifulSoup(p_res.text, 'html.parser')
                email_tag = p_soup.select_one('a.email-link')
                if email_tag: c['email'] = email_tag.get_text(strip=True)
                site_tag = p_soup.select_one('a.website-link')
                if site_tag: c['website'] = site_tag['href']
            except: pass
        return companies
    except: return []

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL СО САЙТА
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    if not url or not isinstance(url, str): return []
    if not url.startswith('http'): url = 'http://' + url
    
    def find(page_url):
        try:
            res = requests.get(page_url, headers=HEADERS, timeout=10)
            text = res.text
            found = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text))
            soup = BeautifulSoup(text, 'html.parser')
            for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
                e = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if '@' in e: found.add(e)
            return found, soup
        except: return set(), None

    emails, soup = find(url)
    
    # Умный поиск через Gemini
    if not emails and soup and GEMINI_API_KEY:
        log.info(f'     [Gemini] интеллектуальный поиск email...')
        gemini_found = extract_emails_with_gemini(str(soup))
        if gemini_found:
            emails.update(gemini_found)

    if not emails and soup:
        # Ищем страницу контактов
        for a in soup.find_all('a', href=True):
            h, t = a['href'].lower(), a.get_text().lower()
            if any(k in h or k in t for k in ['contact', 'контакт', 'about', 'о-нас']):
                full = h if h.startswith('http') else urljoin(url, h)
                extra, _ = find(full)
                emails.update(extra)
                if emails: break

    garbage = {'noreply@', 'test@', 'example@', 'sentry@', 'wix@', 'domain@'}
    filtered = [e.lower() for e in emails if not any(g in e.lower() for g in garbage) and 5 < len(e) < 50]
    return list(set(filtered))

# ══════════════════════════════════════════════════════
#  HUNTER.IO API
# ══════════════════════════════════════════════════════

def find_email_hunter(domain, company_name):
    if not HUNTER_API_KEY:
        return None
    try:
        clean_domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
        params = {'domain': clean_domain, 'api_key': HUNTER_API_KEY, 'limit': 5}
        res = requests.get('https://api.hunter.io/v2/domain-search', params=params, timeout=10)
        emails = res.json().get('data', {}).get('emails', [])
        if emails: return emails[0].get('value')
    except: pass
    return None

# ══════════════════════════════════════════════════════
#  ГЛАВНЫЙ ЦИКЛ
# ══════════════════════════════════════════════════════

def main():
    log.info('🚀 Запуск Tile Mailer Finder (Расширенный поиск)')
    sheet = get_sheet()
    total = 0
    
    for category in SEARCH_CATEGORIES:
        log.info(f'\n🔎 Категория: {category}')
        candidates = []
        
        # Собираем со всех источников
        p_res = search_google_places(category, 'Санкт-Петербург')
        candidates.extend(p_res)
        
        w_res = search_google_web(category, 'Санкт-Петербург')
        candidates.extend(w_res)
        
        d_res = []
        if len(w_res) == 0:
            d_res = search_duckduckgo(category, 'Санкт-Петербург')
            candidates.extend(d_res)
        
        z_res = scrape_zoon(category)
        candidates.extend(z_res)
        
        o_res = scrape_orgpage(category)
        candidates.extend(o_res)
        
        g_res = []
        if len(candidates) == 0:
            g_res = search_gemini_leads(category, 'Санкт-Петербург')
            candidates.extend(g_res)
        
        log.info(f"   Результаты сборов: Maps({len(p_res)}), Web({len(w_res)}), DDG({len(d_res)}), Zoon({len(z_res)}), Org({len(o_res)}), Gemini({len(g_res)})")
        
        # Уникализация по имени
        unique = {}
        for c in candidates:
            n = c['name'].lower().strip()
            if n not in unique: unique[n] = c
            
        log.info(f'   Найдено кандидатов: {len(unique)}')
        
        for name, company in unique.items():
            log.info(f'   » {company["name"]} ({company.get("source")})')
            email = company.get('email')
            site = company.get('website')
            
            if not email and site:
                log.info(f'     Сайт: {site} -> парсим...')
                found = extract_emails_from_url(site)
                if found:
                    email = found[0]
                    log.info(f'     [OK] Email найден: {email}')
            
            # Hunter.io если пусто
            if not email and site:
                email = find_email_hunter(site, company['name'])
                if email: log.info(f'     [OK] Email (Hunter): {email}')
            
            if email:
                if add_company_to_sheet(sheet, email):
                    total += 1
            else:
                log.info('     [!] Email не найден')
            time.sleep(1)
            
    log.info(f'\n✅ Завершено. Добавлено новых email: {total}')

if __name__ == '__main__':
    main()
