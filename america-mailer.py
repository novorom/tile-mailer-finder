#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
America Remote Job Mailer — Рассылка резюме на удаленную работу в США
═══════════════════════════════════════════════════════════════
• Отправляет резюме Романа Новожилова на удаленные позиции в США
• Читает email из столбцов D, E, F Google Sheet
• Использует ту же Google Sheet, что и основной finder
• Отправка через Brevo SMTP
• 1 рассыльщик × 200 писем/день
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
SENDER_NAME  = 'Roman Novozhilov — Remote Sales Professional'
REPLY_TO     = 'novorom@mail.ru'

SHEET_ID   = os.environ.get('SPAIN_SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

SEND_HOUR_FROM = 9
SEND_HOUR_TO   = 20
MSK = timezone(timedelta(hours=3))

DAILY_LIMIT = 300

MAILER_INDEX = int(os.environ.get('MAILER_INDEX', '0'))
TOTAL_MAILERS = int(os.environ.get('TOTAL_MAILERS', '1'))

# Отключить проверку времени для тестовой отправки
SKIP_TIME_CHECK = os.environ.get('SKIP_TIME_CHECK', '').lower() == 'true'

# ══════════════════════════════════════════════════════
#  ПРОВЕРКА ОКНА ОТПРАВКИ
# ══════════════════════════════════════════════════════

def is_send_window() -> bool:
    """Возвращает True если сейчас 9:00–18:00 МСК или SKIP_TIME_CHECK=true"""
    if SKIP_TIME_CHECK:
        log.info('SKIP_TIME_CHECK=true, проверка времени отключена')
        return True
    
    now = datetime.now(MSK)
    hour = now.hour
    if not (SEND_HOUR_FROM <= hour < SEND_HOUR_TO):
        log.info(f'Сейчас {now.strftime("%H:%M")} МСК — вне окна {SEND_HOUR_FROM}:00–{SEND_HOUR_TO}:00, рассылка пропущена')
        return False
    return True

# ══════════════════════════════════════════════════════
#  ПИСЬМО С РЕЗЮМЕ
# ══════════════════════════════════════════════════════

EMAIL_SUBJECT = "Remote Sales Manager Application — Roman Novozhilov (20+ Years B2B Export Experience, Remote-Ready)"

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.6;max-width:650px;margin:0 auto;padding:20px">

  <p>Dear Hiring Manager,</p>

  <p>I am writing to express my strong interest in <strong>remote Sales Manager</strong>, <strong>Export Sales Manager</strong>, or <strong>Business Development Manager</strong> positions at your company.</p>

  <p>With <strong>20+ years of proven B2B wholesale sales and export experience</strong>, I am currently based in Spain and seeking remote opportunities to contribute to US companies' international growth. I have successfully worked remotely since 2022, managing multiple e-commerce projects and automation systems with demonstrated results.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Why I Can Add Value to Your Remote Team:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>20+ years in B2B wholesale sales</strong> with major manufacturers (ceramic tiles, porcelain, construction materials)</li>
      <li><strong>Proven remote work experience</strong> — successfully managed independent e-commerce projects and automation systems since 2022</li>
      <li><strong>Engineering degree in Electronic Systems</strong> — technical background that helps me understand complex products</li>
      <li><strong>Deep knowledge of Russian/CIS markets</strong> — valuable for your export expansion to Eastern Europe</li>
      <li><strong>Extensive network</strong> in construction, distribution, and retail sectors</li>
      <li><strong>Proven export sales track record</strong> and international business development</li>
      <li><strong>AI & automation expertise</strong> — built automated pipelines on GitHub Actions, using Claude, Windsurf, Devin for productivity. <strong>Developed this job search bot with AI assistance</strong> that lives in GitHub and runs automatically via GitHub Actions, demonstrating proactivity and technical aptitude</li>
      <li><strong>E-commerce experience</strong> — running own online stores with 3,000+ contact database</li>
      <li><strong>Sales team training</strong> and trade show management experience</li>
      <li><strong>Fluent in Russian and English</strong> (C1), basic Spanish</li>
      <li><strong>Flexible timezone coverage</strong> — based in Spain (EST+6), available for US business hours with overlap</li>
      <li><strong>Self-motivated and disciplined</strong> — proven ability to work independently and deliver results remotely</li>
    </ul>
  </div>

  <p>I am particularly interested in remote positions with US companies in:</p>
  <ul>
    <li>Ceramic tiles and construction materials</li>
    <li>Building materials and manufacturing</li>
    <li>Wholesale and distribution</li>
    <li>Export and international trade</li>
    <li>B2B sales and business development</li>
  </ul>

  <p>I am available for a video interview at your convenience and can start immediately. I am comfortable with all standard remote collaboration tools (Slack, Teams, Zoom, Asana, Trello) and have a proven track record of successful remote collaboration.</p>

  <p>Please find my detailed CV attached. I would welcome the opportunity to discuss how my experience can benefit your organization's growth and how I can contribute to your remote team.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Remote Sales Manager — 20+ years B2B Export Sales<br><br>
    📞 <a href="tel:+34605650553" style="color:#e87722">+34 605 650 553</a><br>
    📧 <a href="mailto:novorom@gmail.com" style="color:#e87722">novorom@gmail.com</a><br>
    🔗 <a href="https://www.linkedin.com/in/roman-novozhilov-b956b780/" style="color:#e87722">LinkedIn Profile</a><br>
    📍 Spain (Remote — EST+6 Timezone — Available Immediately)
  </div>

  <p style="font-size:11px;color:#aaa;margin-top:16px">
    If you are not interested in receiving job applications, please reply "Remove".
  </p>

</body>
</html>
"""

EMAIL_BODY_TEXT = """\
Dear Hiring Manager,

I am writing to express my strong interest in remote Sales Manager, Export Sales Manager, or Business Development Manager positions at your company.

With 20+ years of proven B2B wholesale sales and export experience, I am currently based in Spain and seeking remote opportunities to contribute to US companies' international growth. I have successfully worked remotely since 2022, managing multiple e-commerce projects and automation systems with demonstrated results.

Why I Can Add Value to Your Remote Team:
- 20+ years in B2B wholesale sales with major manufacturers (ceramic tiles, porcelain, construction materials)
- Proven remote work experience — successfully managed independent e-commerce projects and automation systems since 2022
- Engineering degree in Electronic Systems — technical background that helps me understand complex products
- Deep knowledge of Russian/CIS markets — valuable for your export expansion to Eastern Europe
- Extensive network in construction, distribution, and retail sectors
- Proven export sales track record and international business development
- AI & automation expertise — built automated pipelines on GitHub Actions, using Claude, Windsurf, Devin for productivity. Developed this job search bot with AI assistance that lives in GitHub and runs automatically via GitHub Actions, demonstrating proactivity and technical aptitude
- E-commerce experience — running own online stores with 3,000+ contact database
- Sales team training and trade show management experience
- Fluent in Russian and English (C1), basic Spanish
- Flexible timezone coverage — based in Spain (EST+6), available for US business hours with overlap
- Self-motivated and disciplined — proven ability to work independently and deliver results remotely

I am particularly interested in remote positions with US companies in:
- Ceramic tiles and construction materials
- Building materials and manufacturing
- Wholesale and distribution
- Export and international trade
- B2B sales and business development

I am available for a video interview at your convenience and can start immediately. I am comfortable with all standard remote collaboration tools (Slack, Teams, Zoom, Asana, Trello) and have a proven track record of successful remote collaboration.

Please find my detailed CV attached. I would welcome the opportunity to discuss how my experience can benefit your organization's growth and how I can contribute to your remote team.

Roman Novozhilov
Remote Sales Manager — 20+ years B2B Export Sales
+34 605 650 553
novorom@gmail.com
LinkedIn: https://www.linkedin.com/in/roman-novozhilov-b956b780/
Spain (Remote — EST+6 Timezone — Available Immediately)

If you are not interested in receiving job applications, please reply "Remove".
"""

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

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
    sheet = client.open_by_key(SHEET_ID).sheet1
    return sheet

def load_america_records(sheet):
    """Загружает записи из столбцов D, E, F (company, email, website)"""
    try:
        all_values = sheet.get_all_values()
        if not all_values:
            return {}
        
        records = {}
        # Столбцы: D=4, E=5, F=6, G=7
        for row_num, row in enumerate(all_values, start=1):
            if len(row) < 5:
                continue
            
            company = row[3].strip() if len(row) > 3 else ''  # D
            email = row[4].strip() if len(row) > 4 else ''    # E
            website = row[5].strip() if len(row) > 5 else ''  # F
            job_title = row[6].strip() if len(row) > 6 else '' # G
            
            if email and '@' in email:
                records[email] = {
                    'row': row_num,
                    'company': company,
                    'email': email,
                    'website': website,
                    'job_title': job_title,
                    'status': 'active',
                    'sent': ''
                }
        
        return records
    except Exception as ex:
        log.warning(f'Ошибка загрузки записей: {ex}')
        return {}

def mark_sent(sheet, row_num, month_str):
    """Отмечает email как отправленный в столбце H (столбец 8)"""
    try:
        sheet.update_cell(row_num, 8, month_str)
    except Exception as ex:
        log.warning(f'Ошибка отметки отправки: {ex}')

def mark_dead(sheet, row_num, reason):
    """Отмечает email как dead в столбце I (столбец 9)"""
    try:
        sheet.update_cell(row_num, 9, f'dead:{reason[:60]}')
    except Exception as ex:
        log.warning(f'Ошибка отметки dead: {ex}')

# ══════════════════════════════════════════════════════
#  ОТПРАВКА
# ══════════════════════════════════════════════════════

DEAD_CODES    = {550, 551, 553, 554, 450, 421}
DEAD_KEYWORDS = [
    'user unknown', 'no such user', 'does not exist',
    'invalid address', 'address rejected', 'mailbox not found',
    'account does not exist', 'recipient rejected', 'bad destination',
    'no mailbox', 'undeliverable', 'invalid recipient'
]

def is_dead_bounce(error_msg):
    return any(kw in str(error_msg).lower() for kw in DEAD_KEYWORDS)

def to_smtp_address(email):
    if '@' not in email:
        return None
    local, domain = email.rsplit('@', 1)
    try:
        domain.encode('ascii')
        return email
    except UnicodeEncodeError:
        try:
            punycode = domain.encode('idna').decode('ascii')
            return f'{local}@{punycode}'
        except Exception:
            return None

def send_one_email(to_email):
    smtp_to = to_smtp_address(to_email)
    if smtp_to is None:
        return 'dead', 'unsupported domain encoding'
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = EMAIL_SUBJECT
    msg['From']    = f'{SENDER_NAME} <{SENDER_EMAIL}>'
    msg['Reply-To'] = REPLY_TO
    msg['To']      = to_email
    msg.attach(MIMEText(EMAIL_BODY_TEXT, 'plain', 'utf-8'))
    msg.attach(MIMEText(EMAIL_BODY_HTML, 'html',  'utf-8'))
    
    # Прикрепляем резюме
    try:
        resume_path = 'ROMAN_NOVOZHILOV_REMOTE.docx'
        if os.path.exists(resume_path):
            with open(resume_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="Roman_Novozhilov_Remote_Sales_Manager.docx"'
            )
            msg.attach(part)
            log.info('Резюме прикреплено')
        else:
            log.warning(f'Резюме не найдено: {resume_path}')
    except Exception as ex:
        log.warning(f'Ошибка прикрепления резюме: {ex}')
    
    try:
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT, timeout=15) as server:
            server.starttls()
            server.login(BREVO_USER, BREVO_PASS)
            server.sendmail(SENDER_EMAIL, smtp_to, msg.as_string())
        return 'ok', ''
    except smtplib.SMTPRecipientsRefused as ex:
        detail = str(ex)
        return ('dead' if is_dead_bounce(detail) else 'error'), detail
    except smtplib.SMTPResponseException as ex:
        detail = f'{ex.smtp_code} {ex.smtp_error}'
        if ex.smtp_code in DEAD_CODES and is_dead_bounce(detail):
            return 'dead', detail
        return 'error', detail
    except (smtplib.SMTPException, socket.error, UnicodeEncodeError, OSError) as ex:
        return 'dead', str(ex)

def run_mailing(sheet, records, month_str):
    sent = errors = dead = 0
    
    pending = [
        (email, meta)
        for email, meta in records.items()
        if meta['status'] == 'active' and meta['sent'] != month_str
    ]
    log.info(f'Ожидают отправки в этом месяце: {len(pending)}')
    
    for email, meta in pending:
        if sent + dead + errors >= DAILY_LIMIT:
            log.info(f'Достигнут дневной лимит {DAILY_LIMIT} — продолжим завтра')
            break
        
        status, detail = send_one_email(email)
        
        if status == 'ok':
            log.info(f'  ✅ {email}')
            mark_sent(sheet, meta['row'], month_str)
            sent += 1
        elif status == 'dead':
            log.warning(f'  💀 Мёртвый: {email}')
            mark_dead(sheet, meta['row'], detail[:60])
            dead += 1
        else:
            log.error(f'  ❌ Ошибка {email}: {detail[:80]}')
            errors += 1
        
        time.sleep(2)
    
    remaining = len(pending) - sent - dead - errors
    log.info(f'Сегодня: отправлено={sent}, мёртвых={dead}, ошибок={errors}, осталось={remaining}')
    return sent, dead, errors

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════

def main():
    log.info('═══════════════════════════════════════════')
    log.info(' America Remote Job Mailer — запуск')
    log.info('═══════════════════════════════════════════')
    
    if not is_send_window():
        log.info('Рассылка пропущена — вне окна отправки')
        return
    
    now_msk   = datetime.now(MSK)
    month_str = now_msk.strftime('%Y-%m')
    
    sheet   = get_sheet()
    records = load_america_records(sheet)
    log.info(f'Всего записей в таблице: {len(records)}')
    
    sent, dead, errors = run_mailing(sheet, records, month_str)
    
    log.info('═══════════════════════════════════════════')
    log.info(f'ИТОГ: отправлено={sent} | мёртвых={dead} | ошибок={errors}')
    log.info('═══════════════════════════════════════════')

if __name__ == '__main__':
    main()
