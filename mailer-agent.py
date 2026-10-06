#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tile Mailer Agent — Плитка & Керамогранит СПб
─────────────────────────────────────────────
• Ищет новые email строителей/дизайнеров/проектировщиков СПб и ЛО
• Добавляет новые адреса в Google Sheets
• Удаляет мёртвые (bounced) email из базы
• Рассылает письмо через Brevo SMTP (бесплатно, база без ограничений)
• Отправка каждый день с 9:00 до 1:00 МСК (без выходных)
• 4 рассыльщика × 250 писем = 1000 писем/день — каждый день продолжает с того места где остановился
• 1-го числа месяца — сброс прогресса, новая рассылка
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
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
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
SENDER_NAME  = 'ООО ТФ Керамика'
REPLY_TO     = 'novorom@mail.ru'

SHEET_ID   = os.environ.get('SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

SEND_HOUR_FROM = 9
SEND_HOUR_TO   = 1
MSK = timezone(timedelta(hours=3))

DAILY_LIMIT = 300

MAILER_INDEX = int(os.environ.get('MAILER_INDEX', '0'))
TOTAL_MAILERS = int(os.environ.get('TOTAL_MAILERS', '1'))

# ══════════════════════════════════════════════════════
#  ПРОВЕРКА ОКНА ОТПРАВКИ
# ══════════════════════════════════════════════════════

def is_send_window() -> bool:
    """Возвращает True если сейчас 9:00–1:00 МСК (без ограничений по выходным)"""
    now = datetime.now(MSK)
    hour = now.hour
    # Окно через полночь: 9:00-24:00 ИЛИ 0:00-1:00
    if SEND_HOUR_FROM > SEND_HOUR_TO:
        # Окно через полночь
        if not (hour >= SEND_HOUR_FROM or hour < SEND_HOUR_TO):
            log.info(f'Сейчас {now.strftime("%H:%M")} МСК — вне окна {SEND_HOUR_FROM}:00–{SEND_HOUR_TO}:00, рассылка пропущена')
            return False
    else:
        # Обычное окно
        if not (SEND_HOUR_FROM <= hour < SEND_HOUR_TO):
            log.info(f'Сейчас {now.strftime("%H:%M")} МСК — вне окна {SEND_HOUR_FROM}:00–{SEND_HOUR_TO}:00, рассылка пропущена')
            return False
    return True

# ══════════════════════════════════════════════════════
#  ПИСЬМО
# ══════════════════════════════════════════════════════

EMAIL_SUBJECT = 'Плитка и керамогранит в наличии — весь каталог ТФ Керамика'

EMAIL_BODY_TEXT = """\
Здравствуйте!

Все наши остатки плитки и керамогранита, размеры, артикулы, сорта, наличие и цены собраны в каталоге ТФ Керамика:
https://tfkeramika.ru/#prices

Керамическая плитка:
https://tfkeramika.ru/catalog/keramicheskaya-plitka-spb/

Керамогранит:
https://tfkeramika.ru/catalog/keramogranit-spb/

Работаем по безналичному счёту с НДС. Поможем рассчитать количество для объекта, подтвердить актуальный остаток и согласовать самовывоз или доставку по Санкт-Петербургу и Ленинградской области.

Склад: Ленинградская область, Тосненский район, посёлок Войскорово, 14В
Пн–Пт, 09:00–18:00

Посмотреть каталог: https://tfkeramika.ru/
Написать в Telegram: https://t.me/flyroman
Написать на почту: novorom@mail.ru
Позвонить: +7 905 205-09-00

С уважением,
Роман Новожилов
Менеджер по продажам, ООО «ТФ Керамика»

Если не хотите получать наши предложения, ответьте на письмо словом «Отписаться».
"""

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<title>Плитка и керамогранит в наличии — каталог ТФ Керамика</title>
<style>
  body { margin:0; padding:0; background:#e9e8e4; }
  table { border-collapse:collapse; }
  a { color:#1f2429; }
  @media (max-width:640px) {
    .wrap { width:100% !important; }
    .px { padding-left:18px !important; padding-right:18px !important; }
    .h1 { font-size:30px !important; line-height:36px !important; }
    .half { display:block !important; width:100% !important; padding:0 0 10px !important; }
    .cta { display:block !important; width:auto !important; }
  }
</style>
</head>
<body style="margin:0;padding:0;background:#e9e8e4;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:#e9e8e4;font-size:1px;line-height:1px;">
  Полный список плитки и керамогранита, размеры, артикулы, остатки и цены — на сайте ТФ Керамика.
</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#e9e8e4">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" class="wrap" width="640" cellpadding="0" cellspacing="0" border="0" style="width:640px;max-width:640px;background:#f8f7f4;">
  <tr><td height="7" bgcolor="#f2c400" style="height:7px;font-size:0;line-height:7px;">&nbsp;</td></tr>
  <tr>
    <td class="px" bgcolor="#1f2429" style="background:#1f2429;padding:20px 32px;color:#ffffff;font-family:Arial,Helvetica,sans-serif;">
      <a href="https://tfkeramika.ru/" style="color:#ffffff;text-decoration:none;font-size:22px;line-height:28px;font-weight:bold;">ТФ Керамика</a>
      <div style="padding-top:4px;color:#c7c9ca;font-size:13px;line-height:18px;">Склад плитки и керамогранита · Санкт-Петербург и Ленинградская область</div>
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:34px 32px 18px;font-family:Arial,Helvetica,sans-serif;">
      <div style="display:inline-block;padding:6px 10px;background:#fff3b0;color:#554500;font-size:12px;line-height:16px;font-weight:bold;letter-spacing:.3px;">СТРОИТЕЛЯМ И ПОДРЯДЧИКАМ</div>
      <div class="h1" style="padding-top:16px;font-family:Arial,Helvetica,sans-serif;font-size:36px;line-height:42px;font-weight:800;color:#1f2429;">Все остатки — в каталоге на сайте</div>
      <div style="padding-top:12px;font-size:16px;line-height:25px;color:#535a60;">
        На сайте собраны все наши позиции плитки и керамогранита: фотографии, размеры, артикулы, сорта, наличие и цены. Откройте каталог, чтобы выбрать материал для объекта.
      </div>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:22px;">
        <tr><td bgcolor="#f2c400" style="background:#f2c400;border-radius:5px;">
          <a href="https://tfkeramika.ru/#prices" style="display:block;padding:14px 22px;font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:20px;font-weight:bold;color:#1f2429;text-decoration:none;text-align:center;">Смотреть все остатки и цены&nbsp; →</a>
        </td></tr>
      </table>
      <div style="padding-top:12px;font-size:13px;line-height:20px;color:#70777d;">Каталог обновляется по складским данным. Перед заказом мы подтвердим актуальное наличие и стоимость.</div>
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:8px 32px 0;font-family:Arial,Helvetica,sans-serif;">
      <div style="padding-bottom:12px;font-size:18px;line-height:24px;font-weight:bold;color:#1f2429;">Перейти сразу к нужному разделу</div>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td class="half" width="50%" valign="top" style="padding:0 6px 12px 0;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#ffffff" style="background:#ffffff;border:1px solid #deddd7;">
              <tr><td style="padding:17px 18px;">
                <a href="https://tfkeramika.ru/catalog/keramicheskaya-plitka-spb/" style="font-size:16px;line-height:22px;font-weight:bold;color:#1f2429;text-decoration:none;">Керамическая плитка&nbsp; →</a>
                <div style="padding-top:5px;font-size:13px;line-height:19px;color:#687078;">Позиции, размеры, артикулы и наличие</div>
              </td></tr>
            </table>
          </td>
          <td class="half" width="50%" valign="top" style="padding:0 0 12px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#ffffff" style="background:#ffffff;border:1px solid #deddd7;">
              <tr><td style="padding:17px 18px;">
                <a href="https://tfkeramika.ru/catalog/keramogranit-spb/" style="font-size:16px;line-height:22px;font-weight:bold;color:#1f2429;text-decoration:none;">Керамогранит&nbsp; →</a>
                <div style="padding-top:5px;font-size:13px;line-height:19px;color:#687078;">Каталог для пола, стен и технических задач</div>
              </td></tr>
            </table>
          </td>
        </tr>
      </table>
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:12px 32px 8px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#ffffff" style="background:#ffffff;border:1px solid #deddd7;">
        <tr><td style="padding:20px 22px;font-family:Arial,Helvetica,sans-serif;">
          <div style="font-size:16px;line-height:22px;font-weight:bold;color:#1f2429;">Для покупки на объект</div>
          <div style="padding-top:8px;font-size:14px;line-height:22px;color:#535a60;">
            Работаем по безналичному счёту с НДС. Поможем рассчитать количество, согласовать самовывоз со склада в Войскорово или доставку по Санкт-Петербургу и Ленинградской области.
          </div>
          <div style="padding-top:12px;font-size:14px;line-height:22px;color:#535a60;">
            <b style="color:#1f2429;">Как оформить:</b> выберите позиции в <a href="https://tfkeramika.ru/#prices" style="color:#1f2429;font-weight:bold;">каталоге сайта</a>, отправьте нам названия или артикулы — мы сверим остаток и подготовим счёт.
          </div>
        </td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:18px 32px 8px;font-family:Arial,Helvetica,sans-serif;">
      <div style="font-size:18px;line-height:24px;font-weight:bold;color:#1f2429;">Нужна помощь с подбором?</div>
      <div style="padding-top:6px;font-size:14px;line-height:21px;color:#535a60;">Напишите или позвоните — поможем выбрать материал по размеру, объёму и задаче.</div>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:15px;">
        <tr>
          <td style="padding-right:10px;padding-bottom:8px;">
            <a href="https://t.me/flyroman" style="display:inline-block;padding:11px 16px;border:1px solid #1f2429;border-radius:4px;color:#1f2429;text-decoration:none;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:18px;font-weight:bold;">Написать в Telegram</a>
          </td>
          <td style="padding-bottom:8px;">
            <a href="mailto:novorom@mail.ru" style="display:inline-block;padding:11px 16px;border:1px solid #c9c8c1;border-radius:4px;color:#1f2429;text-decoration:none;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:18px;font-weight:bold;">Написать на почту</a>
          </td>
        </tr>
      </table>
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:20px 32px 24px;font-family:Arial,Helvetica,sans-serif;font-size:13px;line-height:21px;color:#687078;">
      <b style="color:#1f2429;">Склад:</b> Ленинградская область, Тосненский район, посёлок Войскорово, 14В<br>
      Пн–Пт, 09:00–18:00<br>
      <a href="https://tfkeramika.ru/" style="color:#1f2429;font-weight:bold;">tfkeramika.ru</a> · <a href="tel:+79052050900" style="color:#1f2429;text-decoration:none;">+7 905 205-09-00</a> · <a href="mailto:novorom@mail.ru" style="color:#1f2429;">novorom@mail.ru</a><br>
      <span style="display:block;padding-top:12px;">С уважением,<br><b style="color:#1f2429;">Роман Новожилов</b><br>Менеджер по продажам, ООО «ТФ Керамика»</span>
    </td>
  </tr>
  <tr>
    <td class="px" bgcolor="#e9e8e4" style="background:#e9e8e4;padding:16px 32px;font-family:Arial,Helvetica,sans-serif;font-size:11px;line-height:17px;color:#687078;">
      Вы получили это письмо как клиент ООО «ТФ Керамика».<br>
      Если не хотите получать наши предложения, <a href="#unsubscribe" style="color:#687078;">отпишитесь здесь</a> или ответьте на письмо словом «Отписаться».
    </td>
  </tr>
</table>
</td></tr>
</table>
</body>
</html>
"""

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def retry_gspread_call(func, *args, max_retries=5, initial_delay=2, backoff_factor=2, **kwargs):
    """Выполняет gspread функцию с экспоненциальной задержкой в случае временных ошибок API."""
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
            log.warning(f"Ошибка Google Sheets API [{code}]: {str(ex)}. Попытка {attempt+1}/{max_retries} через {sleep_time:.2f} сек...")
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
        email  = row[0].strip().lower()
        if email == 'email':
            continue
        
        status_val = row[1].strip().lower() if len(row) > 1 else ''
        status = status_val if 'dead' in status_val else 'active'
        sent   = row[2].strip() if len(row) > 2 else ''
        records[email] = {'row': row_num, 'status': status, 'sent': sent}
    return records, all_rows

def reset_monthly_sent(sheet, records):
    log.info('Сброс ежемесячного прогресса...')
    updates = []
    for email, meta in records.items():
        if meta['sent']:
            updates.append({'range': f'C{meta["row"]}', 'values': [['']]})
    if updates:
        retry_gspread_call(sheet.batch_update, updates)
    log.info(f'Сброшено флагов: {len(updates)}')

def mark_sent(sheet, row_num, month_str):
    try:
        retry_gspread_call(sheet.update_cell, row_num, 3, month_str)
        log.info(f'✓ Отмечено как отправлено: строка {row_num}')
    except Exception as ex:
        log.warning(f'✗ Ошибка отметки отправки (строка {row_num}): {ex}')

def mark_dead(sheet, row_num, reason):
    try:
        retry_gspread_call(sheet.update_cell, row_num, 2, f'dead:{reason[:60]}')
    except Exception as ex:
        log.warning(f'Не удалось пометить строку {row_num}: {ex}')

def delete_dead_rows(sheet):
    all_rows = retry_gspread_call(sheet.get_all_values)
    if not all_rows:
        return 0
    dead = [
        i + 1
        for i, row in enumerate(all_rows)
        if len(row) > 1 and str(row[1]).startswith('dead:')
    ]
    for row_num in sorted(dead, reverse=True):
        retry_gspread_call(sheet.delete_rows, row_num)
        time.sleep(0.3)
    return len(dead)

def delete_rows_without_status(sheet):
    """Удаляет строки без статуса (пустой столбец B) - те, что не отправляются"""
    all_rows = retry_gspread_call(sheet.get_all_values)
    if not all_rows:
        return 0
    to_delete = []
    for i, row in enumerate(all_rows, start=1):
        if not row or not row[0].strip():
            continue
        email = row[0].strip().lower()
        if email == 'email':
            continue
        # Если столбец B (статус) пустой - удаляем
        status_val = row[1].strip().lower() if len(row) > 1 else ''
        if not status_val:
            to_delete.append(i)
    
    for row_num in sorted(to_delete, reverse=True):
        retry_gspread_call(sheet.delete_rows, row_num)
        time.sleep(0.3)
    return len(to_delete)

def reset_old_month_status(sheet, records, old_month):
    """Сбрасывает статус для указанного месяца (например, 2026-07)"""
    log.info(f'Сброс статуса для месяца {old_month}...')
    updates = []
    for email, meta in records.items():
        if meta['sent'] == old_month:
            updates.append({'range': f'C{meta["row"]}', 'values': [['']]})
    if updates:
        retry_gspread_call(sheet.batch_update, updates)
    log.info(f'Сброшено статусов {old_month}: {len(updates)}')

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

def is_valid_email_format(email):
    """Проверяет базовую валидность формата email"""
    if not email or not isinstance(email, str):
        return False
    
    # Проверка на переносы строк и пробелы
    if '\n' in email or '\r' in email or ' ' in email:
        return False
    
    # Проверка на лишние символы
    if email.startswith('>') or email.startswith('<') or email.endswith('>') or email.endswith('<'):
        return False
    
    # Проверка на несколько @
    if email.count('@') != 1:
        return False
    
    # Разделяем на локальную часть и домен
    local, domain = email.rsplit('@', 1)
    
    # Проверка локальной части
    if not local or len(local) > 64:
        return False
    
    # Проверка на точку в начале/конце локальной части
    if local.startswith('.') or local.endswith('.'):
        return False
    
    # Проверка домена
    if not domain or len(domain) > 255:
        return False
    
    # Проверка на точку в начале/конце домена
    if domain.startswith('.') or domain.endswith('.'):
        return False
    
    # Базовая проверка на допустимые символы
    import re
    if not re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email):
        return False
    
    return True

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
    # Проверка валидности формата email
    if not is_valid_email_format(to_email):
        return 'dead', 'invalid email format'
    
    smtp_to = to_smtp_address(to_email)
    if smtp_to is None:
        return 'dead', 'unsupported domain encoding'
    msg = MIMEMultipart('alternative')
    msg['Subject'] = EMAIL_SUBJECT
    msg['From']    = f'{SENDER_NAME} <{SENDER_EMAIL}>'
    msg['Reply-To'] = 'novorom@mail.ru'
    msg['To']      = to_email
    msg.attach(MIMEText(EMAIL_BODY_TEXT, 'plain', 'utf-8'))
    msg.attach(MIMEText(EMAIL_BODY_HTML, 'html',  'utf-8'))
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
        if meta['status'] == 'active' and meta['sent'] != month_str and (meta['row'] % TOTAL_MAILERS) == (MAILER_INDEX % TOTAL_MAILERS)
    ]
    log.info(f'Ожидают отправки в этом месяце (канал {MAILER_INDEX + 1}/{TOTAL_MAILERS}): {len(pending)}')

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
    log.info(f'Сегодня: отправлено={sent}, мёртвых={dead}, ошибок={errors}, осталось на следующие дни={remaining}')
    return sent, dead, errors

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════

def main():
    if '--test' in sys.argv:
        idx = sys.argv.index('--test')
        test_email = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        if test_email:
            log.info(f'Тест-отправка на: {test_email}')
            status, detail = send_one_email(test_email)
            log.info('✅ Тест отправлен!' if status == 'ok' else f'❌ Ошибка: {detail}')
        return
    
    # Ручные операции для очистки таблицы
    if '--delete-no-status' in sys.argv:
        log.info('═══════════════════════════════════════════')
        log.info(' Удаление строк без статуса')
        log.info('═══════════════════════════════════════════')
        sheet = get_sheet()
        if sheet:
            deleted = delete_rows_without_status(sheet)
            log.info(f'Удалено строк без статуса: {deleted}')
        return
    
    if '--reset-month' in sys.argv:
        idx = sys.argv.index('--reset-month')
        month = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        if month:
            log.info('═══════════════════════════════════════════')
            log.info(f' Сброс статуса для месяца {month}')
            log.info('═══════════════════════════════════════════')
            sheet = get_sheet()
            if sheet:
                records, _ = load_all_records(sheet)
                reset_old_month_status(sheet, records, month)
        return

    log.info('═══════════════════════════════════════════')
    log.info(' Tile Mailer Agent — запуск')
    log.info('═══════════════════════════════════════════')

    now_msk   = datetime.now(MSK)
    month_str = now_msk.strftime('%Y-%m')
    is_first  = (now_msk.day == 1)

    sheet   = get_sheet()
    records, _ = load_all_records(sheet)
    log.info(f'Адресов в базе: {len(records)}')

    if is_first:
        log.info('─── 1-е число — сброс прогресса рассылки ───')
        reset_monthly_sent(sheet, records)
        records, _ = load_all_records(sheet)

    if not is_send_window():
        log.info('Рассылка пропущена — вне окна 9:00–1:00 МСК')
        return

    log.info(f'─── Рассылка — месяц {month_str} ───')
    sent, dead, errors = run_mailing(sheet, records, month_str)

    log.info('─── Удаление мёртвых адресов ───')
    deleted = delete_dead_rows(sheet)
    log.info(f'Удалено: {deleted}')

    log.info('═══════════════════════════════════════════')
    log.info(f'ИТОГ: отправлено={sent} | мёртвых удалено={deleted} | ошибок={errors}')
    log.info('═══════════════════════════════════════════')

if __name__ == '__main__':
    main()
