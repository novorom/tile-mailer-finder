#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для исправления статусов в Google Sheets
Заполняет пустой столбец B статусом "active"
"""

import os
import json
import time
import logging
import requests
import socket
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import RefreshError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

SHEET_ID = os.environ.get('SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

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
            log.warning(f"Ошибка API [{code}]: {ex.message}. Попытка {attempt+1}/{max_retries} через {sleep_time:.2f} сек...")
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
        log.error('SHEET_ID или GOOGLE_CREDS не заданы')
        return None
    try:
        creds_dict = json.loads(CREDS_JSON)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(credentials)
        sheet = retry_gspread_call(lambda: gc.open_by_key(SHEET_ID).sheet1)
        log.info('✅ Google Sheets подключён')
        return sheet
    except Exception as ex:
        log.error(f'❌ Ошибка подключения: {ex}')
        return None

def fix_empty_statuses(sheet):
    """Заполняет пустой столбец B статусом 'active'"""
    all_rows = retry_gspread_call(sheet.get_all_values)
    if not all_rows:
        log.error('Таблица пуста')
        return 0
    
    updates = []
    for i, row in enumerate(all_rows, start=1):
        if not row or not row[0].strip():
            continue
        email = row[0].strip().lower()
        if email == 'email':
            continue
        
        # Если столбец B пустой - ставим 'active'
        status_val = row[1].strip().lower() if len(row) > 1 else ''
        if not status_val or status_val == 'nan':
            updates.append({'range': f'B{i}', 'values': [['active']]})
    
    if updates:
        log.info(f'Найдено {len(updates)} записей без статуса, обновляем...')
        # Пакетное обновление (по 100 за раз)
        batch_size = 100
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            retry_gspread_call(sheet.batch_update, batch)
            log.info(f'Обновлено {min(i+batch_size, len(updates))}/{len(updates)}')
            time.sleep(1)
    else:
        log.info('Все записи уже имеют статус')
    
    return len(updates)

def main():
    log.info('═══════════════════════════════════════════')
    log.info(' Исправление статусов в Google Sheets')
    log.info('═══════════════════════════════════════════')
    
    sheet = get_sheet()
    if not sheet:
        return
    
    fixed = fix_empty_statuses(sheet)
    log.info(f'✅ Исправлено записей: {fixed}')

if __name__ == '__main__':
    main()
