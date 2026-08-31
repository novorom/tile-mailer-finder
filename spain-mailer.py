#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spain Job Mailer — Рассылка резюме в керамическую промышленность Испании
───────────────────────────────────────────────────────────────────────
• Отправляет резюме Романа Новожилова компаниям в сфере керамики
• Использует ту же Google Sheet, что и основной finder
• Отправка через Brevo SMTP
• 1 рассыльщик × 100 писем/день
"""

import smtplib
import socket
import gspread
import requests
import re
import sys
import os
import time
import logging
import json
import tempfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════

BREVO_HOST = 'smtp-relay.brevo.com'
BREVO_PORT = 587
BREVO_USER = os.environ.get("BREVO_USER", "a5784a001@smtp-brevo.com")
BREVO_PASS = os.environ.get('BREVO_PASS', '')

SENDER_EMAIL = 'pasechnick616@gmail.com'
SENDER_NAME  = 'Roman Novozhilov — Ceramic Industry Professional'
REPLY_TO     = 'novorom@mail.ru'

SHEET_ID   = os.environ.get('SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

SEND_HOUR_FROM = 9
SEND_HOUR_TO   = 18
MSK = timezone(timedelta(hours=3))

DAILY_LIMIT = 100

MAILER_INDEX = int(os.environ.get('MAILER_INDEX', '0'))
TOTAL_MAILERS = int(os.environ.get('TOTAL_MAILERS', '1'))

# ══════════════════════════════════════════════════════
#  ПРОВЕРКА ОКНА ОТПРАВКИ
# ══════════════════════════════════════════════════════

def is_send_window() -> bool:
    """Возвращает True если сейчас 9:00–18:00 МСК"""
    now = datetime.now(MSK)
    hour = now.hour
    if not (SEND_HOUR_FROM <= hour < SEND_HOUR_TO):
        log.info(f'Сейчас {now.strftime("%H:%M")} МСК — вне окна {SEND_HOUR_FROM}:00–{SEND_HOUR_TO}:00, рассылка пропущена')
        return False
    return True

# ══════════════════════════════════════════════════════
#  ПИСЬМО С РЕЗЮМЕ
# ══════════════════════════════════════════════════════

EMAIL_SUBJECT = "Roman Novozhilov — Sales Manager (20+ years B2B) — Ceramic Tile Industry"

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.6;max-width:650px;margin:0 auto;padding:20px">

  <p>Dear Hiring Manager,</p>

  <p>I am writing to express my interest in <strong>Sales Manager</strong>, <strong>Export Sales Manager</strong>, or <strong>Commercial Director</strong> positions within the ceramic tile industry.</p>

  <p>With <strong>20+ years of experience in B2B wholesale sales of ceramic tiles and porcelain</strong>, I am seeking to join a leading Spanish ceramic manufacturer, exporter, or distributor to leverage my extensive expertise in international markets.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Key Qualifications:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>20+ years in B2B wholesale sales</strong> of ceramic tiles and porcelain</li>
      <li>Experience with major Russian ceramic manufacturers (Cersanit, Kerama Marazzi, Ural Granite, Granitea, etc.)</li>
      <li>Extensive network in construction, distribution, and retail sectors</li>
      <li>Export sales and international business development expertise</li>
      <li>Fluent in Russian, professional English, basic Spanish</li>
      <li>Available for relocation to Benicàssim / Castellón area</li>
    </ul>
  </div>

  <p>I am particularly interested in companies specializing in:</p>
  <ul>
    <li>Ceramic tile and porcelain manufacturing</li>
    <li>Ceramic tile export and international distribution</li>
    <li>Raw materials for ceramic production (clays, glazes, frits, pigments)</li>
    <li>B2B wholesale and retail of ceramic products</li>
  </ul>

  <p>I am ready to contribute to your company's growth in international markets with my proven sales track record and industry knowledge.</p>

  <p>Please find my detailed CV attached. I would welcome the opportunity to discuss how my experience can benefit your organization.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Sales Manager — 20+ years B2B Ceramic Tile Sales<br><br>
    📞 <a href="tel:+79052050900" style="color:#e87722">+7 (905) 205-09-00</a><br>
    📧 <a href="mailto:novorom@mail.ru" style="color:#e87722">novorom@mail.ru</a><br>
    📍 Available for relocation to Benicàssim / Castellón, Spain
  </div>

  <p style="font-size:11px;color:#aaa;margin-top:16px">
    If you are not interested in receiving job applications, please reply "Remove".
  </p>

</body>
</html>
"""

EMAIL_BODY_TEXT = """\
Dear Hiring Manager,

I am writing to express my interest in Sales Manager, Export Sales Manager, or Commercial Director positions within the ceramic tile industry.

With 20+ years of experience in B2B wholesale sales of ceramic tiles and porcelain, I am seeking to join a leading Spanish ceramic manufacturer, exporter, or distributor to leverage my extensive expertise in international markets.

Key Qualifications:
- 20+ years in B2B wholesale sales of ceramic tiles and porcelain
- Experience with major Russian ceramic manufacturers (Cersanit, Kerama Marazzi, Ural Granite, Granitea, etc.)
- Extensive network in construction, distribution, and retail sectors
- Export sales and international business development expertise
- Fluent in Russian, professional English, basic Spanish
- Available for relocation to Benicàssim / Castellón area

I am particularly interested in companies specializing in:
- Ceramic tile and porcelain manufacturing
- Ceramic tile export and international distribution
- Raw materials for ceramic production (clays, glazes, frits, pigments)
- B2B wholesale and retail of ceramic products

I am ready to contribute to your company's growth in international markets with my proven sales track record and industry knowledge.

Please find my detailed CV attached. I would welcome the opportunity to discuss how my experience can benefit your organization.

Roman Novozhilov
Sales Manager — 20+ years B2B Ceramic Tile Sales
+7 (905) 205-09-00
novorom@mail.ru
Available for relocation to Benicàssim / Castellón, Spain

If you are not interested in receiving job applications, please reply "Remove".
"""

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def retry_gspread_call(func, *args, max_retries=5, initial_delay=2, backoff_factor=2, **kwargs):
    """Выполняет gspread функцию с экспоненциальной задержкой"""
    import random
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
    creds_data = json.loads(CREDS_JSON)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(creds_data, f)
        creds_file = f.name
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(creds_file, scopes=scope)
    client = gspread.authorize(creds)
    sheet = retry_gspread_call(lambda: client.open_by_key(SHEET_ID).sheet1)
    return sheet

def load_all_records(sheet):
    all_rows = retry_gspread_call(sheet.get_all_values)
    if not all_rows:
        return {}, []

    records = {}
    for row_num, row in enumerate(all_rows, start=1):
        if not row or not row[0].strip():
            continue
        email = row[0].strip().lower()
        status = row[1].strip() if len(row) > 1 else ''
        sent = row[2].strip() if len(row) > 2 else ''
        records[email] = {
            'status': status,
            'sent': sent,
            'row': row_num
        }
    return records, all_rows

def mark_sent(sheet, email, month_str):
    """Отмечает email как отправленный"""
    records, all_rows = load_all_records(sheet)
    if email in records:
        row_num = records[email]['row']
        retry_gspread_call(sheet.update_cell, row_num, 3, month_str)
        log.info(f'✓ Отмечено как отправлено: {email}')

def mark_dead(sheet, email):
    """Отмечает email как недействительный"""
    records, all_rows = load_all_records(sheet)
    if email in records:
        row_num = records[email]['row']
        retry_gspread_call(sheet.update_cell, row_num, 1, 'dead')
        log.info(f'✓ Отмечено как dead: {email}')

# ══════════════════════════════════════════════════════
#  EMAIL SENDING
# ══════════════════════════════════════════════════════

def send_email(to_email):
    """Отправляет письмо с резюме"""
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = EMAIL_SUBJECT
        msg['From'] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        msg['To'] = to_email
        msg['Reply-To'] = REPLY_TO

        # Текстовая версия
        text_part = MIMEText(EMAIL_BODY_TEXT, 'plain', 'utf-8')
        msg.attach(text_part)

        # HTML версия
        html_part = MIMEText(EMAIL_BODY_HTML, 'html', 'utf-8')
        msg.attach(html_part)

        # Прикрепляем резюме (если файл есть)
        cv_path = os.path.join(os.path.dirname(__file__), 'ROMAN NOVOZHILOV.docx')
        if os.path.exists(cv_path):
            with open(cv_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename="Roman_Novozhilov_CV.docx"'
                )
                msg.attach(part)
            log.info(f'Резюме прикреплено')
        else:
            log.warning(f'Резюме не найдено: {cv_path}')

        # Отправка через SMTP
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT) as server:
            server.starttls()
            server.login(BREVO_USER, BREVO_PASS)
            server.send_message(msg)
        
        log.info(f'✓ Письмо отправлено: {to_email}')
        return True
    except Exception as e:
        log.error(f'❌ Ошибка отправки {to_email}: {e}')
        return False

# ══════════════════════════════════════════════════════
#  MAIN MAILING LOOP
# ══════════════════════════════════════════════════════

def run_mailing(sheet, records):
    """Основной цикл рассылки"""
    now = datetime.now(MSK)
    month_str = now.strftime('%Y-%m')
    
    pending = [
        (email, meta)
        for email, meta in records.items()
        if meta['status'] == 'active' and meta['sent'] != month_str and (meta['row'] % TOTAL_MAILERS) == (MAILER_INDEX % TOTAL_MAILERS)
    ]
    
    log.info(f'Найдено кандидатов для отправки: {len(pending)}')
    
    sent_count = 0
    for email, meta in pending:
        if sent_count >= DAILY_LIMIT:
            log.info(f'Достигнут дневной лимит: {DAILY_LIMIT}')
            break
        
        log.info(f'Отправка: {email}')
        success = send_email(email)
        
        if success:
            mark_sent(sheet, email, month_str)
            sent_count += 1
        else:
            # Если ошибка - помечаем как dead
            mark_dead(sheet, email)
        
        time.sleep(2)  # Пауза между отправками
    
    log.info(f'Отправлено писем: {sent_count}')

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════

def main():
    log.info('🇪🇸 Spain Job Mailer — запуск')
    log.info(f'MAILER_INDEX: {MAILER_INDEX}/{TOTAL_MAILERS}')
    log.info(f'DAILY_LIMIT: {DAILY_LIMIT}')
    
    if not is_send_window():
        return
    
    sheet = get_sheet()
    if not sheet:
        log.error('Не удалось подключиться к Google Sheets')
        return
    
    records, _ = load_all_records(sheet)
    log.info(f'Всего записей в таблице: {len(records)}')
    
    run_mailing(sheet, records)
    log.info('✅ Завершено')

if __name__ == '__main__':
    main()
