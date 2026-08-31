#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spain Ceramic Job Finder — Поиск работы в сфере керамической плитки в Кастельоне
═══════════════════════════════════════════════════════════════
• Поиск компаний керамической промышленности в Испании (Кастельон)
• Производители плитки, керамогранита, сырья, глазурей
• Экспортные компании керамики
• Парсинг emails с сайтов компаний
• Сохранение в Google Sheets
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
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7',
}

# Категории для керамической промышленности в Испании
CERAMIC_CATEGORIES = [
    # Производители керамической плитки
    'fabricantes azulejos cerámica Castellón',
    'fabricantes gres porcelánico Castellón',
    'fabricantes baldosas cerámicas España',
    
    # Производители сырья для керамики
    'fabricantes arcillas cerámicas Castellón',
    'fabricantes esmaltes cerámicos Castellón',
    'fabricantes fritas cerámicas Castellón',
    'fabricantes pigmentos cerámicos España',
    'proveedores materias primas cerámica',
    
    # Экспортеры керамики
    'exportadores azulejos cerámica España',
    'exportadores gres porcelánico',
    'ceramic tile export companies Spain',
    
    # Оптовые торговцы керамикой
    'distribuidores azulejos cerámica mayorista',
    'distribuidores gres porcelánico España',
    
    # Оборудование для керамики
    'fabricantes maquinaria cerámica',
    'hornos cerámicos industriales',
    
    # Дизайн и инновации в керамике
    'laboratorios investigación cerámica',
    'diseño cerámico innovación',
]

# Категории для других экспортных отраслей в регионе Валенсии
EXPORT_CATEGORIES = [
    # Цитрусовые и фрукты (Valencia - крупный экспортер)
    'exportadores cítricos Valencia',
    'exportadores naranjas Valencia',
    'exportadores frutas Valencia',
    'citrus fruit export companies Spain',
    
    # Мебель (Valencia - мебельный кластер)
    'fabricantes muebles Valencia',
    'exportadores muebles España',
    'furniture export companies Spain',
    
    # Текстиль и обувь
    'fabricantes textiles Valencia',
    'exportadores textiles España',
    'fabricantes calzado España',
    'textile export companies Spain',
    
    # Автозапчасти (Ford в Villarreal)
    'fabricantes autopartes Valencia',
    'exportadores autopartes España',
    'automotive parts export Spain',
    
    # Морепродукты
    'exportadores productos del mar Valencia',
    'seafood export companies Spain',
    
    # Вино
    'bodegas Valencia exportación',
    'wine export companies Valencia',
    
    # Химическая промышленность
    'fabricantes productos químicos Valencia',
    'chemical export companies Spain',
    
    # Пластмассы
    'fabricantes plásticos Valencia',
    'plastic export companies Spain',
    
    # Металлообработка
    'fabricantes metal Valencia',
    'metal export companies Spain',
    
    # Строительные материалы (кроме керамики)
    'exportadores materiales construcción España',
    'construction materials export Spain',
    
    # Оборудование и техника
    'exportadores maquinaria industrial España',
    'industrial machinery export Spain',
]

# Локации в радиусе 100 км от Беникасима (Benicàssim)
LOCATIONS = [
    'Benicàssim',
    'Benicasim',
    'Castellón de la Plana',
    'Castellón',
    'Villarreal',
    'Vila-real',
    'Onda',
    'Alcora',
    'Nules',
    'Burriana',
    'Sagunto',
    'Valencia',
    'Gandia',
    'Paterna',
    'Torrent',
    'Alzira',
    'Requena',
    'Vinaròs',
    'Sant Mateu',
    'Morella',
    'Comunidad Valenciana',
    'Provincia de Castellón',
    'Provincia de Valencia',
]

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def retry_gspread_call(func, *args, max_retries=5, initial_delay=2, backoff_factor=2, **kwargs):
    """Выполняет gspread функцию с экспоненциальной задержкой"""
    import random
    import socket
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as ex:
            code = ex.code
            is_transient = code in [429, 500, 502, 503, 504]
            if not is_transient or attempt == max_retries - 1:
                raise ex
            jitter = random.uniform(0.5, 1.5)
            sleep_time = delay * jitter
            log.warning(f"Ошибка Google Sheets API [{code}]: {ex.message}. Попытка {attempt+1}/{max_retries} через {sleep_time:.2f} сек...")
            time.sleep(sleep_time)
            delay *= backoff_factor
        except (requests.exceptions.RequestException, socket.error) as ex:
            if attempt == max_retries - 1:
                raise ex
            jitter = random.uniform(0.5, 1.5)
            sleep_time = delay * jitter
            log.warning(f"Сетевая ошибка: {ex}. Попытка {attempt+1}/{max_retries} через {sleep_time:.2f} сек...")
            time.sleep(sleep_time)
            delay *= backoff_factor

def get_sheet():
    if not SHEET_ID or not CREDS_JSON:
        log.warning('Google Sheets credentials not set')
        return None
    try:
        creds_dict = json_module.loads(CREDS_JSON)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(credentials)
        sheet = retry_gspread_call(lambda: gc.open_by_key(SHEET_ID).sheet1)
        log.info('✅ Google Sheets подключён')
        return sheet
    except Exception as ex:
        log.error(f'❌ Google Sheets error: {ex}')
        return None

def add_company_to_sheet(sheet, email, local_existing_emails):
    if not sheet:
        log.info(f'[NO SHEET] Found: {email}')
        return False
    try:
        if email in local_existing_emails:
            return False
        retry_gspread_call(sheet.append_row, [email, 'active', ''])
        local_existing_emails.add(email)
        log.info(f'✓ Добавлено: {email}')
        return True
    except Exception as ex:
        log.error(f'❌ Add to sheet error: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  GOOGLE SEARCH
# ══════════════════════════════════════════════════════

def search_google_web(category, location, num=10):
    """Поиск через Google Custom Search"""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        log.debug("     [Google Web] нет API ключа или CSE ID")
        return []
    
    log.info(f"     [Google Web] поиск: {category} {location}...")
    try:
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'q': f"{category} {location}",
            'key': GOOGLE_API_KEY,
            'cx': GOOGLE_CSE_ID,
            'num': num,
            'lr': 'lang_es'
        }
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        
        if 'error' in data:
            log.error(f"     [Google Web] API error: {data['error'].get('message', 'Unknown')}")
            return []
        
        items = data.get('items', [])
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
#  DUCKDUCKGO SEARCH
# ══════════════════════════════════════════════════════

def search_duckduckgo(category, location, num=5):
    """Поиск через DuckDuckGo"""
    log.info(f"     [DuckDuckGo] поиск: {category} {location}...")
    try:
        query = f"{category} {location} contacto email"
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
#  GEMINI LEAD GENERATION
# ══════════════════════════════════════════════════════

def search_gemini_leads(category, location, num=40):
    """Генерация списка компаний через Gemini"""
    if not GEMINI_API_KEY:
        return []
    
    log.info(f"     [Gemini AI] генерация: {category} {location}...")
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        models_to_try = ['gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-2.0-flash-exp']
        model = None
        for m_name in models_to_try:
            try:
                model = genai.GenerativeModel(m_name)
                model.generate_content("test", generation_config={"max_output_tokens": 1})
                log.info(f"     [Gemini AI] модель: {m_name}")
                break
            except Exception as e:
                log.debug(f"     [Gemini AI] модель {m_name} недоступна: {e}")
                continue
        
        if not model:
            log.error("     [Gemini AI] нет доступной модели")
            return []
        
        prompt = (
            f"Find {num} real companies in '{category}' in {location}. "
            "Include manufacturers, exporters, and suppliers. "
            "Return ONLY a JSON array with objects containing: "
            "name (company name), website (official website URL). "
            "No explanations, just the JSON."
        )
        
        response = model.generate_content(prompt, generation_config={"max_output_tokens": 2000})
        text = response.text.strip()
        
        # Извлекаем JSON из ответа
        json_match = re.search(r'\[.*\]', text, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            companies_data = json_module.loads(json_str)
            companies = []
            for c in companies_data:
                if 'name' in c and 'website' in c:
                    companies.append({
                        'name': c['name'],
                        'website': c['website'],
                        'source': 'Gemini AI'
                    })
            log.info(f"     [Gemini AI] найдено: {len(companies)}")
            return companies
        else:
            log.warning("     [Gemini AI] не удалось извлечь JSON")
            return []
    except Exception as e:
        log.error(f"     [Gemini AI] ошибка: {e}")
        return []

# ══════════════════════════════════════════════════════
#  EMAIL EXTRACTION
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    """Извлечение email с сайта"""
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
        
        return list(emails)
    except Exception as e:
        log.debug(f"Ошибка парсинга {url}: {e}")
        return []

def check_site_exists(url):
    """Проверка существования сайта"""
    try:
        res = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        return res.status_code < 400
    except:
        return False

def generate_common_emails(url):
    """Генерация стандартных email шаблонов"""
    try:
        domain = urlparse(url).netloc.lower().replace('www.', '')
        common_patterns = [
            'info@',
            'contact@',
            'info@',
            'ventas@',
            'export@',
            'comercial@',
            'rrhh@',
            'jobs@',
        ]
        emails = [pattern + domain for pattern in common_patterns]
        return emails
    except:
        return []

def find_email_hunter(url, company_name):
    """Поиск email через Hunter.io"""
    if not HUNTER_API_KEY:
        return None
    try:
        domain = urlparse(url).netloc.lower().replace('www.', '')
        api_url = f"https://api.hunter.io/v2/email-finder?domain={domain}&company={company_name}&api_key={HUNTER_API_KEY}"
        res = requests.get(api_url, timeout=10)
        data = res.json()
        if data.get('data', {}).get('email'):
            return data['data']['email']
    except:
        pass
    return None

# ══════════════════════════════════════════════════════
#  ИСПАНСКИЕ ИСТОЧНИКИ
# ══════════════════════════════════════════════════════

def scrape_ascer(category):
    """Парсинг ASCER (Spanish Ceramic Tile Manufacturers' Association)"""
    log.info(f"     [ASCER] поиск: {category}")
    companies = []
    try:
        # ASCER directory
        url = "https://www.ascer.es/es/empresas-asociadas"
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        for link in soup.select('a[href*="empresa"]')[:20]:
            href = link.get('href')
            if href:
                company_url = urljoin(url, href)
                companies.append({
                    'name': link.get_text(strip=True),
                    'website': company_url,
                    'source': 'ASCER'
                })
        
        log.info(f"     [ASCER] найдено: {len(companies)}")
    except Exception as e:
        log.debug(f"ASCER error: {e}")
    return companies

def scrape_tile_of_spain(category):
    """Парсинг Tile of Spain (ASCER export brand)"""
    log.info(f"     [Tile of Spain] поиск: {category}")
    companies = []
    try:
        url = "https://www.tileofspain.com/en/companies"
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        for link in soup.select('a[href*="company"]')[:20]:
            href = link.get('href')
            if href:
                company_url = urljoin(url, href)
                companies.append({
                    'name': link.get_text(strip=True),
                    'website': company_url,
                    'source': 'Tile of Spain'
                })
        
        log.info(f"     [Tile of Spain] найдено: {len(companies)}")
    except Exception as e:
        log.debug(f"Tile of Spain error: {e}")
    return companies

def scrape_ferias_valencia(category):
    """Парсинг выставок в Валенсии (Cevisama)"""
    log.info(f"     [Cevisama] поиск: {category}")
    companies = []
    try:
        url = "https://www.cevisama.feriavalencia.com/en/exhibitors"
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        for link in soup.select('a[href*="exhibitor"]')[:20]:
            href = link.get('href')
            if href:
                company_url = urljoin(url, href)
                companies.append({
                    'name': link.get_text(strip=True),
                    'website': company_url,
                    'source': 'Cevisama'
                })
        
        log.info(f"     [Cevisama] найдено: {len(companies)}")
    except Exception as e:
        log.debug(f"Cevisama error: {e}")
    return companies

# ══════════════════════════════════════════════════════
#  MAIN LOOP
# ══════════════════════════════════════════════════════

def main():
    log.info('🇪🇸 Spain Export Job Finder — запуск')
    log.info(f'Керамических категорий: {len(CERAMIC_CATEGORIES)}')
    log.info(f'Экспортных категорий: {len(EXPORT_CATEGORIES)}')
    log.info(f'Локаций: {len(LOCATIONS)}')
    
    sheet = get_sheet()
    
    # Загружаем существующие emails
    local_existing_emails = set()
    if sheet:
        try:
            all_records = retry_gspread_call(sheet.get_all_values)
            for row in all_records:
                if row and row[0]:
                    local_existing_emails.add(row[0].lower().strip())
            log.info(f'Загружено существующих email: {len(local_existing_emails)}')
        except Exception as e:
            log.error(f'Ошибка загрузки существующих email: {e}')
    
    total = 0
    processed_domains = set()
    
    # Объединяем все категории
    all_categories = CERAMIC_CATEGORIES + EXPORT_CATEGORIES
    
    for category in all_categories:
        for location in LOCATIONS:
            log.info(f'\n🔍 Категория: {category}')
            log.info(f'📍 Локация: {location}')
            
            candidates = []
            
            # Google Search (с локацией)
            w_res = search_google_web(category, location)
            candidates.extend(w_res)
            
            # Google Search (без локации - больше результатов)
            w_res2 = search_google_web(category, "Spain")
            candidates.extend(w_res2)
            
            # DuckDuckGo (с локацией)
            d_res = search_duckduckgo(category, location)
            candidates.extend(d_res)
            
            # DuckDuckGo (без локации)
            d_res2 = search_duckduckgo(category, "Spain")
            candidates.extend(d_res2)
            
            # ASCER (без локации - уже специфичен для Испании)
            ascer_res = scrape_ascer(category)
            candidates.extend(ascer_res)
            
            # Tile of Spain (без локации)
            tos_res = scrape_tile_of_spain(category)
            candidates.extend(tos_res)
            
            # Cevisama (без локации)
            cev_res = scrape_ferias_valencia(category)
            candidates.extend(cev_res)
            
            # Gemini fallback (всегда пытаемся)
            g_res = search_gemini_leads(category, location)
            candidates.extend(g_res)
            
            log.info(f"   Результаты: Web({len(w_res)}), Web2({len(w_res2)}), DDG({len(d_res)}), DDG2({len(d_res2)}), ASCER({len(ascer_res)}), ToS({len(tos_res)}), Cevisama({len(cev_res)}), Gemini({len(g_res)})")
            
            # Уникализация
            unique = {}
            for c in candidates:
                n = c['name'].lower().strip()
                if n not in unique:
                    unique[n] = c
            
            log.info(f'   Уникальных: {len(unique)}')
            
            for name, company in unique.items():
                log.info(f'   » {company["name"]} ({company.get("source")})')
                email = company.get('email')
                site = company.get('website')
                
                # Извлекаем домен
                domain = None
                if site and isinstance(site, str):
                    try:
                        domain = urlparse(site).netloc.lower().replace('www.', '')
                    except:
                        pass
                
                if domain and domain in processed_domains:
                    log.info(f'     [!] Домен {domain} уже обрабатывался')
                    continue
                
                if domain:
                    processed_domains.add(domain)
                
                # Проверяем существующий email
                if email:
                    email = email.lower().strip()
                    if email in local_existing_emails:
                        log.info(f'     [!] Email уже есть в таблице')
                        continue
                
                # Парсим сайт
                if not email and site:
                    log.info(f'     Сайт: {site}')
                    if not check_site_exists(site):
                        log.info(f'     [!] Сайт недоступен')
                        continue
                    found = extract_emails_from_url(site)
                    if found:
                        new_found = [e for e in found if e not in local_existing_emails]
                        if new_found:
                            email = new_found[0]
                            log.info(f'     [OK] Email: {email}')
                
                # Генерируем шаблоны
                if not email and site:
                    common_emails = generate_common_emails(site)
                    if common_emails:
                        new_common = [e for e in common_emails if e not in local_existing_emails]
                        if new_common:
                            email = new_common[0]
                            log.info(f'     [OK] Email (шаблон): {email}')
                
                # Hunter.io
                if not email and site:
                    email = find_email_hunter(site, company['name'])
                    if email:
                        email = email.lower().strip()
                        if email in local_existing_emails:
                            log.info(f'     [!] Hunter email уже есть')
                            email = None
                        else:
                            log.info(f'     [OK] Email (Hunter): {email}')
                
                if email:
                    if add_company_to_sheet(sheet, email, local_existing_emails):
                        total += 1
                else:
                    log.info('     [!] Email не найден')
                time.sleep(1)
    
    log.info(f'\n✅ Завершено. Добавлено новых email: {total}')

if __name__ == '__main__':
    main()
