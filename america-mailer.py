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

EMAIL_SUBJECT = "Remote Sales / AI & Software — Roman Novozhilov (20+ Years B2B Experience)"

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.6;max-width:650px;margin:0 auto;padding:20px">

  <p>Dear Hiring Manager,</p>

  <p>I am writing to express my interest in <strong>remote opportunities</strong> with your company in <strong>Sales, Business Development, AI, automation, software testing, QA, and related technology roles</strong>.</p>

  <p>With <strong>20+ years of proven B2B wholesale sales, export, and international business development experience</strong>, I am currently based in Spain and looking for a remote opportunity with a US company. Since 2022, I have also been working independently on e-commerce, AI, automation, and technology-related projects, developing practical hands-on skills alongside my commercial background.</p>

  <p>I am open to both <strong>Sales / Business Development positions</strong> and <strong>remote roles related to AI, automation, software testing, QA, and technology</strong>, particularly where my combination of business experience, technical background, and ability to work with modern AI tools can bring value.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Why I Can Add Value to Your Team:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>20+ years of B2B wholesale sales and international business development</strong> with major manufacturers in ceramic tiles, porcelain, and construction materials</li>
      <li><strong>Extensive export sales experience</strong> and deep knowledge of international markets, distributors, and B2B customer development</li>
      <li><strong>Engineering degree in Electronic Systems</strong> — strong technical background and ability to understand complex products and technologies</li>
      <li><strong>Hands-on experience with AI and automation</strong> — building automated workflows and projects using modern AI tools and development environments</li>
      <li><strong>AI-assisted development experience</strong> — working with tools such as Claude, Cascade, Devin, Windsurf and GitHub Actions to build and automate practical projects</li>
      <li><strong>AI automation project</strong> — I developed my own automated job-search system, hosted entirely on GitHub and running automatically through GitHub Actions. This project demonstrates my ability to combine AI tools, automation, APIs, GitHub workflows, and business logic into a working system</li>
      <li><strong>Software testing and QA interest</strong> — experienced in testing, troubleshooting, identifying problems, analyzing results, and improving automated workflows and software-based processes</li>
      <li><strong>E-commerce experience</strong> — running my own online stores and managing a database of 3,000+ contacts</li>
      <li><strong>Sales team training and trade show management</strong> experience</li>
      <li><strong>Strong problem-solving and self-learning ability</strong> — comfortable learning new software, platforms, AI tools, and technologies independently</li>
      <li><strong>Fluent in Russian and English (C1)</strong>, with basic Spanish</li>
      <li><strong>Remote-ready since 2022</strong> — experienced working independently, managing projects, meeting deadlines, and delivering results without direct supervision</li>
      <li><strong>Based in Spain (EST+6)</strong> — available to work US business hours with the necessary time-zone overlap</li>
      <li><strong>Available immediately</strong></li>
    </ul>
  </div>

  <p><strong>I am particularly interested in remote opportunities with US companies in:</strong></p>
  <ul>
    <li>Sales / Sales Management / Business Development</li>
    <li>Export Sales and International Business Development</li>
    <li>AI and AI-powered business solutions</li>
    <li>AI Operations and AI-assisted workflows</li>
    <li>AI evaluation, testing, and training</li>
    <li>Automation and AI-powered process optimization</li>
    <li>Software Testing / QA</li>
    <li>SaaS and technology companies</li>
    <li>Technical Sales / Sales Engineering</li>
    <li>AI/SaaS Business Development</li>
    <li>Other remote roles where my combination of commercial and technical skills can be valuable</li>
  </ul>

  <p>I am particularly interested in opportunities where I can combine my <strong>extensive business and sales experience with my growing expertise in AI, automation, software, and modern technology</strong>.</p>

  <p>I am available for a video interview at your convenience and can start immediately. I am comfortable working with remote collaboration and project-management tools such as Slack, Microsoft Teams, Zoom, Asana, Trello, and GitHub.</p>

  <p>Please find my LinkedIn profile for my detailed CV and professional background. I would welcome the opportunity to discuss how my experience, technical skills, and ability to work with AI and automation can contribute to your company.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Remote Sales / AI & Automation / QA & Software<br>
    20+ years of B2B Sales & International Business Development<br><br>
    📞 <a href="tel:+34605650553" style="color:#e87722">+34 605 650 553</a><br>
    📧 <a href="mailto:novorom@gmail.com" style="color:#e87722">novorom@gmail.com</a><br>
    🔗 <a href="https://www.linkedin.com/in/roman-novozhilov-b956b780/?locale=en-US" style="color:#e87722">LinkedIn</a><br>
    📍 Spain — Remote / EST+6 — Available Immediately
  </div>

  <p style="font-size:11px;color:#aaa;margin-top:16px">
    If you are not interested in receiving job applications, please reply "Remove".
  </p>

</body>
</html>
"""

EMAIL_BODY_TEXT = """\
Dear Hiring Manager,

I am writing to express my interest in remote opportunities with your company in Sales, Business Development, AI, automation, software testing, QA, and related technology roles.

With 20+ years of proven B2B wholesale sales, export, and international business development experience, I am currently based in Spain and looking for a remote opportunity with a US company. Since 2022, I have also been working independently on e-commerce, AI, automation, and technology-related projects, developing practical hands-on skills alongside my commercial background.

I am open to both Sales / Business Development positions and remote roles related to AI, automation, software testing, QA, and technology, particularly where my combination of business experience, technical background, and ability to work with modern AI tools can bring value.

Why I Can Add Value to Your Team:
- 20+ years of B2B wholesale sales and international business development with major manufacturers in ceramic tiles, porcelain, and construction materials
- Extensive export sales experience and deep knowledge of international markets, distributors, and B2B customer development
- Engineering degree in Electronic Systems — strong technical background and ability to understand complex products and technologies
- Hands-on experience with AI and automation — building automated workflows and projects using modern AI tools and development environments
- AI-assisted development experience — working with tools such as Claude, Cascade, Devin, Windsurf and GitHub Actions to build and automate practical projects
- AI automation project — I developed my own automated job-search system, hosted entirely on GitHub and running automatically through GitHub Actions. This project demonstrates my ability to combine AI tools, automation, APIs, GitHub workflows, and business logic into a working system
- Software testing and QA interest — experienced in testing, troubleshooting, identifying problems, analyzing results, and improving automated workflows and software-based processes
- E-commerce experience — running my own online stores and managing a database of 3,000+ contacts
- Sales team training and trade show management experience
- Strong problem-solving and self-learning ability — comfortable learning new software, platforms, AI tools, and technologies independently
- Fluent in Russian and English (C1), with basic Spanish
- Remote-ready since 2022 — experienced working independently, managing projects, meeting deadlines, and delivering results without direct supervision
- Based in Spain (EST+6) — available to work US business hours with the necessary time-zone overlap
- Available immediately

I am particularly interested in remote opportunities with US companies in:
- Sales / Sales Management / Business Development
- Export Sales and International Business Development
- AI and AI-powered business solutions
- AI Operations and AI-assisted workflows
- AI evaluation, testing, and training
- Automation and AI-powered process optimization
- Software Testing / QA
- SaaS and technology companies
- Technical Sales / Sales Engineering
- AI/SaaS Business Development
- Other remote roles where my combination of commercial and technical skills can be valuable

I am particularly interested in opportunities where I can combine my extensive business and sales experience with my growing expertise in AI, automation, software, and modern technology.

I am available for a video interview at your convenience and can start immediately. I am comfortable working with remote collaboration and project-management tools such as Slack, Microsoft Teams, Zoom, Asana, Trello, and GitHub.

Please find my LinkedIn profile for my detailed CV and professional background. I would welcome the opportunity to discuss how my experience, technical skills, and ability to work with AI and automation can contribute to your company.

Best regards,

Roman Novozhilov
Remote Sales / AI & Automation / QA & Software
20+ years of B2B Sales & International Business Development
+34 605 650 553
novorom@gmail.com
LinkedIn: https://www.linkedin.com/in/roman-novozhilov-b956b780/?locale=en-US
Spain — Remote / EST+6 — Available Immediately

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
            log.warning('Таблица пуста')
            return {}
        
        log.info(f'Всего строк в таблице: {len(all_values)}')
        
        records = {}
        # Столбцы: D=4, E=5, F=6, G=7, H=8
        for row_num, row in enumerate(all_values, start=1):
            if len(row) < 5:
                continue
            
            company = row[3].strip() if len(row) > 3 else ''  # D
            email = row[4].strip() if len(row) > 4 else ''    # E
            website = row[5].strip() if len(row) > 5 else ''  # F
            job_title = row[6].strip() if len(row) > 6 else '' # G
            sent = row[7].strip() if len(row) > 7 else ''      # H
            
            if email and '@' in email:
                records[email] = {
                    'row': row_num,
                    'company': company,
                    'email': email,
                    'website': website,
                    'job_title': job_title,
                    'status': 'active',
                    'sent': sent
                }
        
        log.info(f'Загружено записей с email: {len(records)}')
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
    log.info(f'Попытка отправки на: {to_email}')
    smtp_to = to_smtp_address(to_email)
    if smtp_to is None:
        log.warning(f'Неподдерживаемая кодировка домена: {to_email}')
        return 'dead', 'unsupported domain encoding'
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = EMAIL_SUBJECT
    msg['From']    = f'{SENDER_NAME} <{SENDER_EMAIL}>'
    msg['Reply-To'] = REPLY_TO
    msg['Bcc']     = REPLY_TO  # Отправляем копию на REPLY_TO для контроля
    msg['To']      = to_email
    msg.attach(MIMEText(EMAIL_BODY_TEXT, 'plain', 'utf-8'))
    msg.attach(MIMEText(EMAIL_BODY_HTML, 'html',  'utf-8'))
    
    try:
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT, timeout=15) as server:
            server.starttls()
            server.login(BREVO_USER, BREVO_PASS)
            # Отправляем на основной адрес и BCC
            recipients = [smtp_to, REPLY_TO]
            log.info(f'Отправка на получателей: {recipients}')
            result = server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
            log.info(f'Результат sendmail: {result}')
        log.info(f'Успешно отправлено: {to_email}')
        return 'ok', ''
    except smtplib.SMTPRecipientsRefused as ex:
        detail = str(ex)
        log.error(f'SMTPRecipientsRefused для {to_email}: {detail}')
        log.error(f'Отклоненные получатели: {ex.recipients}')
        return ('dead' if is_dead_bounce(detail) else 'error'), detail
    except smtplib.SMTPResponseException as ex:
        detail = f'{ex.smtp_code} {ex.smtp_error}'
        log.error(f'SMTPResponseException для {to_email}: {detail}')
        if ex.smtp_code in DEAD_CODES and is_dead_bounce(detail):
            return 'dead', detail
        return 'error', detail
    except (smtplib.SMTPException, socket.error, UnicodeEncodeError, OSError) as ex:
        log.error(f'Исключение при отправке {to_email}: {type(ex).__name__}: {ex}')
        return 'dead', str(ex)

def run_mailing(sheet, records, month_str):
    sent = errors = dead = 0
    
    pending = [
        (email, meta)
        for email, meta in records.items()
        if meta['status'] == 'active' and meta['sent'] != month_str
    ]
    log.info(f'Ожидают отправки в этом месяце: {len(pending)}')
    
    if not pending:
        log.info('Нет записей для отправки - все уже отправлены в этом месяце')
        return 0, 0, 0
    
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
