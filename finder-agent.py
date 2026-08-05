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
    # Застройщики и крупные генподрядчики
    'застройщики жилой недвижимости',
    'генеральные подрядчики строительство',
    'строительные компании коммерческая недвижимость',
    'строительство торговых центров и складов',
    'строительство бизнес-центров',
    
    # Средние и малые подрядчики, отделка
    'отделочные работы подряд',
    'ремонтно-строительные компании',
    'комплексный ремонт коммерческих помещений',
    'ремонт офисов под ключ',
    'ремонт ресторанов и кафе',
    'ремонт квартир в новостройках',
    'ремонт элитной недвижимости',
    'ремонт ванных комнат и санузлов',
    'укладка крупноформатного керамогранита',
    'фасадные работы керамогранит',
    
    # Комплектаторы и оптовики (перепродажа)
    'комплектация строительных объектов',
    'строительные базы опт',
    'салоны керамической плитки',
    'магазины сантехники и плитки',
    'интернет-магазины строительных материалов',
    'оптовая продажа напольных покрытий',

    # Архитекторы и дизайнеры (спецификаторы)
    'архитектурные бюро',
    'студии дизайна интерьера',
    'проектирование загородных домов',
    'дизайнеры интерьера комплектация',

    # Специализированное строительство
    'строительство загородных домов и коттеджей',
    'строительство бассейнов и спа'
]

LOCATIONS = [
    # Санкт-Петербург и область
    'Санкт-Петербург', 
    'Ленинградская область', 
    'Мурино', 
    'Кудрово', 
    'Всеволожск', 
    'Гатчина', 
    'Выборг',
    'Сосновый Бор',
    'Тихвин',
    'Луга',
    
    # Новгородская область
    'Великий Новгород',
    'Новгородская область',
    
    # Псковская область
    'Псков',
    'Псковская область',
    
    # Карелия
    'Петрозаводск',
    'Республика Карелия',
    
    # Мурманская область
    'Мурманск',
    'Мурманская область',
    
    # Архангельская область
    'Архангельск',
    'Архангельская область',
    
    # Калининградская область
    'Калининград',
    'Калининградская область',
    'Советск',
    'Черняховск',
    'Балтийск',
    'Светлогорск',
    'Гурьевск',
    'Зеленоградск',
    
    # Вологодская область
    'Вологда',
    'Вологодская область',
    
    # Коми
    'Сыктывкар',
    'Республика Коми'
]

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def retry_gspread_call(func, *args, max_retries=5, initial_delay=2, backoff_factor=2, **kwargs):
    """Выполняет gspread функцию с экспоненциальной задержкой в случае временных ошибок API."""
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
            log.warning(f"Сетевая ошибка при запросе к Google Sheets: {ex}. Попытка {attempt+1}/{max_retries} через {sleep_time:.2f} сек...")
            time.sleep(sleep_time)
            delay *= backoff_factor

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

        retry_gspread_call(sheet.append_row, [email])
        local_existing_emails.add(email)
        log.info(f'✓ Добавлено: {email}')
        return True
    except Exception as ex:
        log.error(f'❌ Add to sheet error: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  GOOGLE SEARCH APIs
# ══════════════════════════════════════════════════════

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
        models_to_try = ['gemini-3.1-flash-lite', 'gemini-1.5-flash', 'gemini-pro']
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
            f"Найди {num} реально существующих компаний в сфере '{category}' в локации {location}. "
            "Мне нужны как крупные игроки, так и небольшие подрядчики/фирмы. "
            "Не придумывай названия, используй только реальные данные. "
            "Для каждой компании обязательно найди её официальный сайт. "
            "В ответе должен быть только JSON-массив и ничего больше. "
            "Формат: [{\"name\": \"Название\", \"website\": \"https://...\"}]"
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

def scrape_construction_portals(category):
    """Парсинг строительных порталов СПб и СЗ региона"""
    companies = []
    
    # Список строительных порталов для парсинга
    portals = [
        {
            'name': 'IRN.ru (Строительный портал)',
            'url': 'https://www.irn.ru',
            'search_url': f'https://www.irn.ru/search/?q={requests.utils.quote(category)}&region=spb',
            'projects_url': f'https://www.irn.ru/projects/?region=spb'  # Завершенные проекты
        },
        {
            'name': 'Stroi.ru (Строительный портал)',
            'url': 'https://www.stroi.ru',
            'search_url': f'https://www.stroi.ru/search?q={requests.utils.quote(category)}',
            'projects_url': f'https://www.stroi.ru/projects/'  # Завершенные проекты
        },
        {
            'name': 'Stroitelstvo.ru',
            'url': 'https://www.stroitelstvo.ru',
            'search_url': f'https://www.stroitelstvo.ru/search?q={requests.utils.quote(category)}',
            'projects_url': f'https://www.stroitelstvo.ru/projects/'  # Завершенные проекты
        }
    ]
    
    for portal in portals:
        try:
            # Поиск по категории
            log.info(f"     [{portal['name']}] поиск по категории...")
            time.sleep(1)
            res = requests.get(portal['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Общая логика для поиска компаний на порталах
            for item in soup.select('div.company-item, div.search-result, tr.search-row')[:5]:
                try:
                    name_elem = item.select_one('a.company-name, a.title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = portal['url'] + href if href.startswith('/') else portal['url'] + '/' + href
                        
                        # Поиск email на странице компании
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': portal['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': portal['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': portal['name']
                                })
                except: pass
            
            # Поиск в завершенных проектах
            log.info(f"     [{portal['name']}] поиск в завершенных проектах...")
            time.sleep(1)
            try:
                proj_res = requests.get(portal['projects_url'], headers=HEADERS, timeout=10)
                proj_soup = BeautifulSoup(proj_res.text, 'html.parser')
                
                # Поиск компаний в завершенных проектах
                for proj_item in proj_soup.select('div.project-item, div.completed-project, tr.project-row')[:5]:
                    try:
                        company_elem = proj_item.select_one('a.company-name, a.developer, a.contractor')
                        if company_elem:
                            company_name = company_elem.get_text(strip=True)
                            company_href = company_elem.get('href', '')
                            if company_href and not company_href.startswith('http'):
                                company_href = portal['url'] + company_href if company_href.startswith('/') else portal['url'] + '/' + company_href
                            
                            # Поиск email на странице компании
                            if company_href:
                                time.sleep(0.5)
                                try:
                                    comp_res = requests.get(company_href, headers=HEADERS, timeout=10)
                                    comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                    if emails:
                                        companies.append({
                                            'name': company_name,
                                            'email': emails[0],
                                            'website': company_href,
                                            'source': f"{portal['name']} (Завершенные проекты)"
                                        })
                                    else:
                                        companies.append({
                                            'name': company_name,
                                            'website': company_href,
                                            'source': f"{portal['name']} (Завершенные проекты)"
                                        })
                                except:
                                    companies.append({
                                        'name': company_name,
                                        'website': company_href,
                                        'source': f"{portal['name']} (Завершенные проекты)"
                                    })
                    except: pass
            except Exception as e:
                log.debug(f"Ошибка парсинга завершенных проектов {portal['name']}: {e}")
                
        except Exception as e:
            log.debug(f"Ошибка парсинга {portal['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено на строительных порталах")
    return companies

def scrape_builder_databases(category):
    """Парсинг баз строителей и подрядчиков"""
    companies = []
    
    # Список баз строителей для парсинга
    databases = [
        {
            'name': 'Stroyportal.ru (База строителей)',
            'url': 'https://www.stroyportal.ru',
            'search_url': f'https://www.stroyportal.ru/catalog/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Stroymaterialy.ru (База поставщиков)',
            'url': 'https://www.stroymaterialy.ru',
            'search_url': f'https://www.stroymaterialy.ru/companies/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Stroyka.ru (База подрядчиков)',
            'url': 'https://www.stroyka.ru',
            'search_url': f'https://www.stroyka.ru/firms/?q={requests.utils.quote(category)}&region=78'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Поиск компаний в базе
            for item in soup.select('div.company-card, div.firm-item, tr.firm-row')[:5]:
                try:
                    name_elem = item.select_one('a.company-title, a.firm-name, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        # Поиск email на странице компании
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах строителей")
    return companies

def scrape_developer_databases(category):
    """Парсинг баз застройщиков"""
    companies = []
    
    # Список баз застройщиков для парсинга
    developers = [
        {
            'name': 'N1.ru (База застройщиков)',
            'url': 'https://www.n1.ru',
            'search_url': f'https://www.n1.ru/zhilie-kompleksy/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Cian.ru (База застройщиков)',
            'url': 'https://www.cian.ru',
            'search_url': f'https://www.cian.ru/developers/?q={requests.utils.quote(category)}&region=1'
        },
        {
            'name': 'Domofond.ru (База застройщиков)',
            'url': 'https://www.domofond.ru',
            'search_url': f'https://www.domofond.ru/developers/?q={requests.utils.quote(category)}&region=spb'
        }
    ]
    
    for developer in developers:
        try:
            log.info(f"     [{developer['name']}] поиск...")
            time.sleep(1)
            res = requests.get(developer['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Поиск застройщиков
            for item in soup.select('div.developer-card, div.builder-item, tr.developer-row')[:5]:
                try:
                    name_elem = item.select_one('a.developer-name, a.builder-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = developer['url'] + href if href.startswith('/') else developer['url'] + '/' + href
                        
                        # Поиск email на странице застройщика
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': developer['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': developer['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': developer['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {developer['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах застройщиков")
    return companies

def scrape_sro_databases(category):
    """Парсинг баз СРО (саморегулируемых организаций)"""
    companies = []
    
    # Список баз СРО для парсинга
    sro_databases = [
        {
            'name': 'Nostroy.ru (Реестр СРО)',
            'url': 'https://www.nostroy.ru',
            'search_url': f'https://www.nostroy.ru/members/?q={requests.utils.quote(category)}&region=78'
        },
        {
            'name': 'Reestrstro.ru (Реестр СРО)',
            'url': 'https://www.reestrstro.ru',
            'search_url': f'https://www.reestrstro.ru/search/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Sro-register.ru (Реестр СРО)',
            'url': 'https://www.sro-register.ru',
            'search_url': f'https://www.sro-register.ru/members/?q={requests.utils.quote(category)}'
        }
    ]
    
    for sro_db in sro_databases:
        try:
            log.info(f"     [{sro_db['name']}] поиск...")
            time.sleep(1)
            res = requests.get(sro_db['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Поиск компаний в реестре СРО
            for item in soup.select('div.member-card, div.sro-member, tr.member-row')[:5]:
                try:
                    name_elem = item.select_one('a.member-name, a.company-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = sro_db['url'] + href if href.startswith('/') else sro_db['url'] + '/' + href
                        
                        # Поиск email на странице компании
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': sro_db['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': sro_db['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': sro_db['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {sro_db['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в реестрах СРО")
    return companies

def scrape_material_suppliers(category):
    """Парсинг баз поставщиков строительных материалов"""
    companies = []
    
    # Список баз поставщиков материалов
    suppliers = [
        {
            'name': 'Stroyshop.ru (База поставщиков)',
            'url': 'https://www.stroyshop.ru',
            'search_url': f'https://www.stroyshop.ru/companies/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Stroymaterialy.net (База поставщиков)',
            'url': 'https://www.stroymaterialy.net',
            'search_url': f'https://www.stroymaterialy.net/suppliers/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Torgstroymaterialy.ru (База поставщиков)',
            'url': 'https://www.torgstroymaterialy.ru',
            'search_url': f'https://www.torgstroymaterialy.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        }
    ]
    
    for supplier in suppliers:
        try:
            log.info(f"     [{supplier['name']}] поиск...")
            time.sleep(1)
            res = requests.get(supplier['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.supplier-card, div.company-item, tr.supplier-row')[:5]:
                try:
                    name_elem = item.select_one('a.supplier-name, a.company-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = supplier['url'] + href if href.startswith('/') else supplier['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': supplier['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': supplier['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': supplier['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {supplier['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах поставщиков")
    return companies

def scrape_design_architect_databases(category):
    """Парсинг баз дизайнеров и архитекторов"""
    companies = []
    
    # Список баз дизайнеров и архитекторов
    databases = [
        {
            'name': 'Architect.ru (База архитекторов)',
            'url': 'https://www.architect.ru',
            'search_url': f'https://www.architect.ru/architects/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Design.ru (База дизайнеров)',
            'url': 'https://www.design.ru',
            'search_url': f'https://www.design.ru/designers/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Interiordesign.ru (База дизайнеров интерьеров)',
            'url': 'https://www.interiordesign.ru',
            'search_url': f'https://www.interiordesign.ru/designers/?q={requests.utils.quote(category)}&region=spb'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.designer-card, div.architect-item, tr.designer-row')[:5]:
                try:
                    name_elem = item.select_one('a.designer-name, a.architect-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах дизайнеров и архитекторов")
    return companies

def scrape_repair_companies(category):
    """Парсинг баз ремонтных компаний"""
    companies = []
    
    # Список баз ремонтных компаний
    databases = [
        {
            'name': 'Remont.ru (База ремонтных компаний)',
            'url': 'https://www.remont.ru',
            'search_url': f'https://www.remont.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Remontnik.ru (База ремонтников)',
            'url': 'https://www.remontnik.ru',
            'search_url': f'https://www.remontnik.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Stroiremont.ru (База ремонтных компаний)',
            'url': 'https://www.stroiremont.ru',
            'search_url': f'https://www.stroiremont.ru/firms/?q={requests.utils.quote(category)}&region=78'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.repair-card, div.firm-item, tr.repair-row')[:5]:
                try:
                    name_elem = item.select_one('a.repair-name, a.firm-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах ремонтных компаний")
    return companies

def scrape_construction_objects(category):
    """Парсинг баз строительных объектов"""
    companies = []
    
    # Список баз строительных объектов
    databases = [
        {
            'name': 'Stroyobject.ru (База строй объектов)',
            'url': 'https://www.stroyobject.ru',
            'search_url': f'https://www.stroyobject.ru/objects/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Objectstroy.ru (База строй объектов)',
            'url': 'https://www.objectstroy.ru',
            'search_url': f'https://www.objectstroy.ru/objects/?q={requests.utils.quote(category)}&region=78'
        },
        {
            'name': 'Stroyportal-objects.ru (База строй объектов)',
            'url': 'https://www.stroyportal-objects.ru',
            'search_url': f'https://www.stroyportal-objects.ru/search/?q={requests.utils.quote(category)}'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.object-card, div.construction-item, tr.object-row')[:5]:
                try:
                    name_elem = item.select_one('a.object-name, a.construction-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах строй объектов")
    return companies

def scrape_contractors_database(category):
    """Парсинг баз подрядчиков"""
    companies = []
    
    # Список баз подрядчиков
    databases = [
        {
            'name': 'Contractor.ru (База подрядчиков)',
            'url': 'https://www.contractor.ru',
            'search_url': f'https://www.contractor.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Podryadchik.ru (База подрядчиков)',
            'url': 'https://www.podryadchik.ru',
            'search_url': f'https://www.podryadchik.ru/firms/?q={requests.utils.quote(category)}&region=78'
        },
        {
            'name': 'Stroy-contractor.ru (База подрядчиков)',
            'url': 'https://www.stroy-contractor.ru',
            'search_url': f'https://www.stroy-contractor.ru/companies/?q={requests.utils.quote(category)}'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.contractor-card, div.firm-item, tr.contractor-row')[:5]:
                try:
                    name_elem = item.select_one('a.contractor-name, a.firm-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах подрядчиков")
    return companies

def scrape_finishers_database(category):
    """Парсинг баз мастеров-отделочников"""
    companies = []
    
    # Список баз мастеров-отделочников
    databases = [
        {
            'name': 'Otdelka.ru (База отделочников)',
            'url': 'https://www.otdelka.ru',
            'search_url': f'https://www.otdelka.ru/masters/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Master-otdelka.ru (База отделочников)',
            'url': 'https://www.master-otdelka.ru',
            'search_url': f'https://www.master-otdelka.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Otdelchnik.ru (База отделочников)',
            'url': 'https://www.otdelchnik.ru',
            'search_url': f'https://www.otdelchnik.ru/masters/?q={requests.utils.quote(category)}&region=78'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.master-card, div.finisher-item, tr.master-row')[:5]:
                try:
                    name_elem = item.select_one('a.master-name, a.finisher-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в базах мастеров-отделочников")
    return companies

def scrape_stroyradar_similar(category):
    """Парсинг Стройрадара и похожих источников"""
    companies = []
    
    # Список Стройрадара и похожих источников
    databases = [
        {
            'name': 'Stroyradar.ru (Стройрадар)',
            'url': 'https://www.stroyradar.ru',
            'search_url': f'https://www.stroyradar.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Stroyinfo.ru (Стройинфо)',
            'url': 'https://www.stroyinfo.ru',
            'search_url': f'https://www.stroyinfo.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Stroybase.ru (Стройбаза)',
            'url': 'https://www.stroybase.ru',
            'search_url': f'https://www.stroybase.ru/firms/?q={requests.utils.quote(category)}&region=78'
        }
    ]
    
    for database in databases:
        try:
            log.info(f"     [{database['name']}] поиск...")
            time.sleep(1)
            res = requests.get(database['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.company-card, div.firm-item, tr.company-row')[:5]:
                try:
                    name_elem = item.select_one('a.company-name, a.firm-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = database['url'] + href if href.startswith('/') else database['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': database['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': database['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': database['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {database['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено на Стройрадаре и похожих")
    return companies

def scrape_business_directories(category):
    """Парсинг бизнес-каталогов и справочников компаний"""
    companies = []
    
    # Список бизнес-каталогов и справочников
    directories = [
        {
            'name': 'Yell.ru (Бизнес-справочник)',
            'url': 'https://www.yell.ru',
            'search_url': f'https://www.yell.ru/search/?q={requests.utils.quote(category)}&city=Санкт-Петербург'
        },
        {
            'name': 'Gde.ru (Бизнес-справочник)',
            'url': 'https://www.gde.ru',
            'search_url': f'https://www.gde.ru/search/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': '2gis.ru (Бизнес-справочник)',
            'url': 'https://www.2gis.ru',
            'search_url': f'https://www.2gis.ru/search/?q={requests.utils.quote(category)}&region=spb'
        }
    ]
    
    for directory in directories:
        try:
            log.info(f"     [{directory['name']}] поиск...")
            time.sleep(1)
            res = requests.get(directory['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.company-card, div.firm-item, tr.company-row')[:5]:
                try:
                    name_elem = item.select_one('a.company-name, a.firm-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = directory['url'] + href if href.startswith('/') else directory['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': directory['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': directory['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': directory['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {directory['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в бизнес-каталогах")
    return companies

def scrape_marketplaces(category):
    """Парсинг торговых площадок и маркетплейсов стройматериалов"""
    companies = []
    
    # Список торговых площадок и маркетплейсов
    marketplaces = [
        {
            'name': 'Stroyopt.ru (Маркетплейс стройматериалов)',
            'url': 'https://www.stroyopt.ru',
            'search_url': f'https://www.stroyopt.ru/catalog/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Stroytorg.ru (Торговая площадка)',
            'url': 'https://www.stroytorg.ru',
            'search_url': f'https://www.stroytorg.ru/sellers/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Stroymarket.ru (Маркетплейс)',
            'url': 'https://www.stroymarket.ru',
            'search_url': f'https://www.stroymarket.ru/companies/?q={requests.utils.quote(category)}'
        }
    ]
    
    for marketplace in marketplaces:
        try:
            log.info(f"     [{marketplace['name']}] поиск...")
            time.sleep(1)
            res = requests.get(marketplace['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.seller-card, div.company-item, tr.seller-row')[:5]:
                try:
                    name_elem = item.select_one('a.seller-name, a.company-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = marketplace['url'] + href if href.startswith('/') else marketplace['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': marketplace['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': marketplace['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': marketplace['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {marketplace['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено на маркетплейсах")
    return companies

def scrape_tile_shops(category):
    """Парсинг салонов и магазинов плитки"""
    companies = []
    
    # Список салонов и магазинов плитки
    shops = [
        {
            'name': 'Tileshop.ru (Салоны плитки)',
            'url': 'https://www.tileshop.ru',
            'search_url': f'https://www.tileshop.ru/companies/?q={requests.utils.quote(category)}&region=spb'
        },
        {
            'name': 'Keramogranit.ru (Магазины керамогранита)',
            'url': 'https://www.keramogranit.ru',
            'search_url': f'https://www.keramogranit.ru/shops/?q={requests.utils.quote(category)}&region=78'
        },
        {
            'name': 'Plitka.ru (Салоны плитки)',
            'url': 'https://www.plitka.ru',
            'search_url': f'https://www.plitka.ru/salons/?q={requests.utils.quote(category)}&region=spb'
        }
    ]
    
    for shop in shops:
        try:
            log.info(f"     [{shop['name']}] поиск...")
            time.sleep(1)
            res = requests.get(shop['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.shop-card, div.salon-item, tr.shop-row')[:5]:
                try:
                    name_elem = item.select_one('a.shop-name, a.salon-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = shop['url'] + href if href.startswith('/') else shop['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': shop['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': shop['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': shop['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {shop['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено в салонах плитки")
    return companies

def scrape_forums_communities(category):
    """Парсинг профильных форумов и сообществ"""
    companies = []
    
    # Список профильных форумов и сообществ
    forums = [
        {
            'name': 'Stroyforum.ru (Строительный форум)',
            'url': 'https://www.stroyforum.ru',
            'search_url': f'https://www.stroyforum.ru/search/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Mastergrad.ru (Форум мастеров)',
            'url': 'https://www.mastergrad.ru',
            'search_url': f'https://www.mastergrad.ru/search/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Remontnik.ru (Форум ремонтников)',
            'url': 'https://www.remontnik.ru',
            'search_url': f'https://www.remontnik.ru/forum/search/?q={requests.utils.quote(category)}'
        }
    ]
    
    for forum in forums:
        try:
            log.info(f"     [{forum['name']}] поиск...")
            time.sleep(1)
            res = requests.get(forum['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.user-card, div.member-item, tr.user-row')[:5]:
                try:
                    name_elem = item.select_one('a.user-name, a.member-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = forum['url'] + href if href.startswith('/') else forum['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': forum['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': forum['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': forum['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {forum['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено на форумах")
    return companies

def scrape_exhibitions_fairs(category):
    """Парсинг выставок и ярмарок"""
    companies = []
    
    # Список выставок и ярмарок
    exhibitions = [
        {
            'name': 'Expostroy.ru (Выставки строительства)',
            'url': 'https://www.expostroy.ru',
            'search_url': f'https://www.expostroy.ru/exhibitors/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Stroyexpo.ru (Строительные выставки)',
            'url': 'https://www.stroyexpo.ru',
            'search_url': f'https://www.stroyexpo.ru/participants/?q={requests.utils.quote(category)}'
        },
        {
            'name': 'Buildfair.ru (Строительные ярмарки)',
            'url': 'https://www.buildfair.ru',
            'search_url': f'https://www.buildfair.ru/exhibitors/?q={requests.utils.quote(category)}'
        }
    ]
    
    for exhibition in exhibitions:
        try:
            log.info(f"     [{exhibition['name']}] поиск...")
            time.sleep(1)
            res = requests.get(exhibition['search_url'], headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            for item in soup.select('div.exhibitor-card, div.participant-item, tr.exhibitor-row')[:5]:
                try:
                    name_elem = item.select_one('a.exhibitor-name, a.participant-title, h3 a, h4 a')
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        href = name_elem.get('href', '')
                        if href and not href.startswith('http'):
                            href = exhibition['url'] + href if href.startswith('/') else exhibition['url'] + '/' + href
                        
                        if href:
                            time.sleep(0.5)
                            try:
                                comp_res = requests.get(href, headers=HEADERS, timeout=10)
                                comp_soup = BeautifulSoup(comp_res.text, 'html.parser')
                                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', comp_soup.text)
                                if emails:
                                    companies.append({
                                        'name': name,
                                        'email': emails[0],
                                        'website': href,
                                        'source': exhibition['name']
                                    })
                                else:
                                    companies.append({
                                        'name': name,
                                        'website': href,
                                        'source': exhibition['name']
                                    })
                            except:
                                companies.append({
                                    'name': name,
                                    'website': href,
                                    'source': exhibition['name']
                                })
                except: pass
        except Exception as e:
            log.debug(f"Ошибка парсинга {exhibition['name']}: {e}")
            continue
    
    log.info(f"     [{len(companies)}] компаний найдено на выставках")
    return companies

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL СО САЙТА
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    if not url or not isinstance(url, str): return []
    if not url.startswith('http'): url = 'http://' + url
    
    from urllib.parse import urlparse
    try:
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    except Exception:
        base_url = url

    def find(page_url):
        try:
            res = requests.get(page_url, headers=HEADERS, timeout=10)
            text = res.text
            found = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text))
            
            # Поиск обфусцированных email (например, info [at] site.ru, info (собака) site.ru)
            obfuscated = re.findall(r'[a-zA-Z0-9._%+-]+\s*(?:\[at\]|\(at\)|\{at\}|\[собака\]|\(собака\)|\{собака\}|@)\s*[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            for obs in obfuscated:
                cleaned = re.sub(r'\s*(?:\[at\]|\(at\)|\{at\}|\[собака\]|\(собака\)|\{собака\})\s*', '@', obs)
                if '@' in cleaned:
                    found.add(cleaned.strip().lower())

            soup = BeautifulSoup(text, 'html.parser')
            
            # Поиск в footer и header
            for section in ['footer', 'header']:
                section_elem = soup.find(section)
                if section_elem:
                    section_text = section_elem.get_text()
                    section_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', section_text)
                    found.update(e.lower() for e in section_emails)
            
            # Поиск в mailto ссылках
            for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
                e = a['href'].replace('mailto:', '').split('?')[0].strip().lower()
                if '@' in e: found.add(e)
            
            # Поиск в data-email атрибутах
            for elem in soup.find_all(attrs={'data-email': True}):
                e = elem['data-email'].strip().lower()
                if '@' in e: found.add(e)
            
            # Поиск в data-* атрибутах с email
            for elem in soup.find_all(True):
                for attr, value in elem.attrs.items():
                    if attr.startswith('data-') and isinstance(value, str):
                        if '@' in value and '.' in value.split('@')[-1]:
                            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', value)
                            found.update(e.lower() for e in emails)
            
            # Поиск в мета-тегах
            for meta in soup.find_all('meta'):
                for attr in ['content', 'name', 'property']:
                    if meta.get(attr):
                        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', meta[attr])
                        found.update(e.lower() for e in emails)
            
            # Поиск в JSON-LD структурированных данных
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    import json
                    data = json.loads(script.string)
                    json_str = json.dumps(data).lower()
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', json_str)
                    found.update(e.lower() for e in emails)
                except: pass
            
            # Поиск в JavaScript коде
            for script in soup.find_all('script'):
                if script.string:
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', script.string)
                    found.update(e.lower() for e in emails)
            
            return found, soup
        except: return set(), None

    emails, soup = find(url)
    
    # Умный поиск через Gemini на главной
    if not emails and soup and GEMINI_API_KEY:
        log.info(f'     [Gemini] интеллектуальный поиск email на главной...')
        gemini_found = extract_emails_with_gemini(str(soup))
        if gemini_found:
            emails.update(gemini_found)

    # Ищем страницы контактов по ссылкам
    if soup:
        contact_links = []
        for a in soup.find_all('a', href=True):
            h, t = a['href'].lower(), a.get_text().lower()
            if any(k in h or k in t for k in ['contact', 'контакт', 'about', 'о-нас', 'о компании', 'feedback', 'обратная', 'связь', 'контакты']):
                full = h if h.startswith('http') else urljoin(url, h)
                contact_links.append(full)
        
        # Стандартные пути напрямую (на случай JS-генерации)
        for path in ['/contacts', '/contact', '/about', '/contacts/', '/about/', '/kontakty', '/kontakt', '/o-nas', '/o-kompanii', '/feedback', '/obratnaya-svyaz', '/svyaz', '/kontakty/', '/kontakt/', '/o-nas/', '/o-kompanii/', '/feedback/', '/obratnaya-svyaz/', '/svyaz/']:
            contact_links.append(urljoin(base_url, path))
            
        seen_links = set()
        unique_contact_links = []
        for link in contact_links:
            if link not in seen_links:
                seen_links.add(link)
                unique_contact_links.append(link)
                
        for link in unique_contact_links[:10]:  # Увеличили с 5 до 10
            extra_emails, extra_soup = find(link)
            if extra_emails:
                emails.update(extra_emails)
                break
            if extra_soup and GEMINI_API_KEY:
                # Резервный Gemini на странице контактов
                gemini_found = extract_emails_with_gemini(str(extra_soup))
                if gemini_found:
                    emails.update(gemini_found)
                    break

    garbage = {'noreply@', 'test@', 'example@', 'sentry@', 'wix@', 'domain@'}
    invalid_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico', '.css', '.js', '.pdf', '.zip', '.tar', '.gz')
    filtered = []
    for e in emails:
        e_lower = e.lower().strip()
        if any(g in e_lower for g in garbage):
            continue
        if e_lower.endswith(invalid_extensions):
            continue
        if '@2x' in e_lower or '@3x' in e_lower:
            continue
        # Фильтрация emails с цифрами перед @ (например, 335-51-11info@setlgroup.ru)
        if '@' in e_lower:
            local_part = e_lower.split('@')[0]
            if any(char.isdigit() for char in local_part):
                continue
        if 5 < len(e_lower) < 50:
            filtered.append(e_lower)
    return list(set(filtered))

def check_site_exists(url):
    """Проверяет существование сайта перед парсингом"""
    if not url or not isinstance(url, str):
        return False
    if not url.startswith('http'):
        url = 'http://' + url
    
    try:
        response = requests.head(url, headers=HEADERS, timeout=5, allow_redirects=True)
        return response.status_code in [200, 301, 302, 303, 307, 308]
    except:
        return False

def generate_common_emails(domain):
    """Генерирует стандартные email шаблоны для домена"""
    if not domain:
        return []
    
    clean_domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
    
    common_prefixes = [
        'info', 'contact', 'sales', 'support', 'office', 'admin', 
        'manager', 'director', 'hr', 'marketing', 'billing', 'reception',
        'info@', 'contact@', 'sales@', 'support@', 'office@', 'admin@',
        'manager@', 'director@', 'hr@', 'marketing@', 'billing@', 'reception@'
    ]
    
    emails = []
    for prefix in common_prefixes:
        if '@' in prefix:
            email = prefix.replace('@', '') + '@' + clean_domain
        else:
            email = prefix + '@' + clean_domain
        emails.append(email)
    
    return list(set(emails))

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
    
    # Кэшируем существующие email один раз при старте
    local_existing_emails = set()
    if sheet:
        try:
            log.info('Загрузка существующих email из Google Sheets...')
            local_existing_emails = set(retry_gspread_call(sheet.col_values, 1))
            log.info(f'Загружено {len(local_existing_emails)} существующих адресов.')
        except Exception as e:
            log.warning(f'Не удалось загрузить существующие email: {e}')
            
    processed_domains = set()
    total = 0
    
    # Перемешиваем категории и локации для разнообразия поиска
    import random
    combined_tasks = []
    for location in LOCATIONS:
        for category in SEARCH_CATEGORIES:
            combined_tasks.append((category, location))
    random.shuffle(combined_tasks)
    
    # Ограничиваем количество комбинаций за один запуск (например, 10 комбинаций)
    # чтобы не упираться в лимиты API и выполнять поиск стабильно каждый день
    selected_tasks = combined_tasks[:10]
    log.info(f'Выбрано {len(selected_tasks)} случайных поисковых комбинаций для этого запуска.')
    
    for category, location in selected_tasks:
        log.info(f'\n🔎 Категория: {category} ({location})')
        candidates = []
        
        # Собираем со всех источников
        w_res = search_google_web(category, location)
        candidates.extend(w_res)
        
        d_res = []
        if len(w_res) == 0:
            d_res = search_duckduckgo(category, location)
            candidates.extend(d_res)
        
        z_res = scrape_zoon(category)
        candidates.extend(z_res)
        
        o_res = scrape_orgpage(category)
        candidates.extend(o_res)
        
        # Добавляем парсинг строительных порталов
        cp_res = scrape_construction_portals(category)
        candidates.extend(cp_res)
        
        # Добавляем парсинг баз строителей
        bd_res = scrape_builder_databases(category)
        candidates.extend(bd_res)
        
        # Добавляем парсинг баз застройщиков
        dev_res = scrape_developer_databases(category)
        candidates.extend(dev_res)
        
        # Добавляем парсинг реестров СРО
        sro_res = scrape_sro_databases(category)
        candidates.extend(sro_res)
        
        # Добавляем парсинг баз поставщиков материалов
        sup_res = scrape_material_suppliers(category)
        candidates.extend(sup_res)
        
        # Добавляем парсинг баз дизайнеров и архитекторов
        des_res = scrape_design_architect_databases(category)
        candidates.extend(des_res)
        
        # Добавляем парсинг баз ремонтных компаний
        rep_res = scrape_repair_companies(category)
        candidates.extend(rep_res)
        
        # Добавляем парсинг баз строй объектов
        obj_res = scrape_construction_objects(category)
        candidates.extend(obj_res)
        
        # Добавляем парсинг баз подрядчиков
        ctr_res = scrape_contractors_database(category)
        candidates.extend(ctr_res)
        
        # Добавляем парсинг баз мастеров-отделочников
        fin_res = scrape_finishers_database(category)
        candidates.extend(fin_res)
        
        # Добавляем парсинг Стройрадара и похожих
        rad_res = scrape_stroyradar_similar(category)
        candidates.extend(rad_res)
        
        # Добавляем парсинг бизнес-каталогов
        dir_res = scrape_business_directories(category)
        candidates.extend(dir_res)
        
        # Добавляем парсинг маркетплейсов стройматериалов
        mkt_res = scrape_marketplaces(category)
        candidates.extend(mkt_res)
        
        # Добавляем парсинг салонов плитки
        shop_res = scrape_tile_shops(category)
        candidates.extend(shop_res)
        
        # Добавляем парсинг форумов и сообществ
        forum_res = scrape_forums_communities(category)
        candidates.extend(forum_res)
        
        # Добавляем парсинг выставок и ярмарок
        expo_res = scrape_exhibitions_fairs(category)
        candidates.extend(expo_res)
        
        g_res = []
        if len(candidates) == 0:
            g_res = search_gemini_leads(category, location)
            candidates.extend(g_res)
        
        log.info(f"   Результаты сборов: Web({len(w_res)}), DDG({len(d_res)}), Zoon({len(z_res)}), Org({len(o_res)}), Portals({len(cp_res)}), Builders({len(bd_res)}), Developers({len(dev_res)}), SRO({len(sro_res)}), Suppliers({len(sup_res)}), Design({len(des_res)}), Repair({len(rep_res)}), Objects({len(obj_res)}), Contractors({len(ctr_res)}), Finishers({len(fin_res)}), Radar({len(rad_res)}), Directories({len(dir_res)}), Marketplaces({len(mkt_res)}), Shops({len(shop_res)}), Forums({len(forum_res)}), Exhibitions({len(expo_res)}), Gemini({len(g_res)})")

        # Уникализация по имени
        unique = {}
        for c in candidates:
            n = c['name'].lower().strip()
            if n not in unique: unique[n] = c
            
        log.info(f'   Найдено уникальных кандидатов: {len(unique)}')
        
        for name, company in unique.items():
            log.info(f'   » {company["name"]} ({company.get("source")})')
            email = company.get('email')
            site = company.get('website')
            
            # Извлекаем домен для уникализации и избежания повторного парсинга
            domain = None
            if site and isinstance(site, str):
                from urllib.parse import urlparse
                try:
                    domain = urlparse(site).netloc.lower().replace('www.', '')
                except:
                    pass
            
            if domain and domain in processed_domains:
                log.info(f'     [!] Домен {domain} уже обрабатывался в этом запуске, пропускаем.')
                continue
            
            if domain:
                processed_domains.add(domain)
                
            # Проверяем, есть ли уже email (если он вернулся из источника напрямую)
            if email:
                email = email.lower().strip()
                if email in local_existing_emails:
                    log.info(f'     [!] Email {email} уже есть в таблице, пропускаем.')
                    continue
            
            # Парсим сайт, если email не найден
            if not email and site:
                log.info(f'     Сайт: {site} -> проверяем существование...')
                if not check_site_exists(site):
                    log.info(f'     [!] Сайт не существует или недоступен, пропускаем.')
                    continue
                log.info(f'     Сайт существует -> парсим...')
                found = extract_emails_from_url(site)
                if found:
                    new_found = [e for e in found if e not in local_existing_emails]
                    if new_found:
                        email = new_found[0]
                        log.info(f'     [OK] Email найден: {email}')
                    else:
                        log.info(f'     [!] Все найденные email ({found}) уже есть в таблице.')
            
            # Генерируем стандартные email шаблоны если парсинг не дал результатов
            if not email and site:
                log.info(f'     [Генерация] Пробуем стандартные email шаблоны...')
                common_emails = generate_common_emails(site)
                if common_emails:
                    new_common = [e for e in common_emails if e not in local_existing_emails]
                    if new_common:
                        email = new_common[0]
                        log.info(f'     [OK] Email (шаблон): {email}')
                else:
                    log.info(f'     [!] Шаблоны не дали результатов.')
            
            # Hunter.io если пусто
            if not email and site:
                email = find_email_hunter(site, company['name'])
                if email:
                    email = email.lower().strip()
                    if email in local_existing_emails:
                        log.info(f'     [!] Найденный через Hunter email {email} уже есть в таблице.')
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
