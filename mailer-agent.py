#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tile Mailer Agent — Плитка & Керамогранит СПб
─────────────────────────────────────────────
• Ищет новые email строителей/дизайнеров/проектировщиков СПб и ЛО
• Добавляет новые адреса в Google Sheets
• Удаляет мёртвые (bounced) email из базы
• Рассылает письмо через Brevo SMTP (бесплатно, база без ограничений)
• Отправка ТОЛЬКО по будням с 12:00 до 16:00 МСК
• 300 писем/день — каждый день продолжает с того места где остановился
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
SENDER_NAME  = 'Роман Новожилов — Керамогранит и плитка'
REPLY_TO     = 'novorom@mail.ru'

SHEET_ID   = os.environ.get('SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

SEND_HOUR_FROM = 12
SEND_HOUR_TO   = 16
MSK = timezone(timedelta(hours=3))

DAILY_LIMIT = 290

# ══════════════════════════════════════════════════════
#  ПРОВЕРКА ОКНА ОТПРАВКИ
# ══════════════════════════════════════════════════════

def is_send_window() -> bool:
    """Возвращает True если сейчас будний день и 12:00–16:00 МСК"""
    now = datetime.now(MSK)
    weekday = now.weekday()
    hour    = now.hour
    if weekday >= 5:
        log.info(f'Сегодня выходной ({["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][weekday]}) — рассылка пропущена')
        return False
    if not (SEND_HOUR_FROM <= hour < SEND_HOUR_TO):
        log.info(f'Сейчас {now.strftime("%H:%M")} МСК — вне окна {SEND_HOUR_FROM}:00–{SEND_HOUR_TO}:00, рассылка пропущена')
        return False
    return True

# ══════════════════════════════════════════════════════
#  ПИСЬМО
# ══════════════════════════════════════════════════════

EMAIL_SUBJECT = "Керамическая плитка и керамогранит — апрель СПб — опт, розница, объектные продажи"

EMAIL_BODY_TEXT = """\
Добрый день!

Плитка CERSANIT (Церсанит) в наличии на складе в СПб — большая товарная программа!

────────────────────────────────────────
КЕРАМОГРАНИТ Cersanit, 598×185 мм — под дерево, под паркет
Цена: 995 руб./м²
https://cersanit.ru/catalog/2d/collections/f/size-is-18x60/

ШАХТИНСКАЯ облицовочная строительная плитка (глянец), разных цветов и белая, 20×30 см
Цена: 412 руб./м²

ДЕТСКАЯ плитка Нефрит-Керамика для школ и детских садов — серия Kids
Цена: от 617 руб./м²
https://nefrit.ru/collections/Kids/

УРАЛЬСКИЙ ГРАНИТ — GRANITEA / Гранитея / IDALGO / Идальго / Керамика Будущего
U100 (молочный, моноколор) 60×60 матовый
Цена: 774 руб./м²
https://www.uralgres.com/catalog/ural-granite/ural-facades/u100/

KERAMA MARAZZI — Керама-Марацци
SG701390R Фрегат бежевый обрезной КГ 20×80
Цена: 1 502 руб./м²
https://kerama-marazzi.com/catalog/gres/sg701390r/
────────────────────────────────────────

Наши заводы-производители:
Cersanit · Шахтинская плитка (GraciaCeramica) · Нефрит-Керамика · Квадро-Декор
Керама-Марацци · Азори (Керабуд) · Уральский Гранит (Idalgo) · Керамика Будущего
Granitea (Гранитея) · Daco (Дагестан)

Склад и большой шоурум в Янино.
Доставка и самовывоз.

С уважением,
Менеджер по продажам Роман Новожилов
+7 (905) 205-09-00
www.plitki-spb.ru
"""

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.7;max-width:620px;margin:0 auto;padding:20px">

  <p>Добрый день!</p>
  <p><strong>Плитка CERSANIT (Церсанит) в наличии на складе в СПб — большая товарная программа!</strong></p>

  <hr style="border:none;border-top:2px solid #e87722;margin:20px 0">

  <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
      <td style="padding:12px 0;border-bottom:1px solid #eee">
        <strong style="color:#e87722">КЕРАМОГРАНИТ Cersanit</strong>, 598×185 мм — под дерево, под паркет<br>
        <span style="font-size:18px;font-weight:bold;color:#1a1a2e">995 руб./м²</span><br>
        <a href="https://cersanit.ru/catalog/2d/collections/f/size-is-18x60/" style="color:#1565c0">Смотреть коллекцию →</a>
      </td>
    </tr>
    <tr>
      <td style="padding:12px 0;border-bottom:1px solid #eee">
        <strong style="color:#e87722">ШАХТИНСКАЯ плитка</strong> облицовочная, глянец, разных цветов и белая, 20×30 см<br>
        <span style="font-size:18px;font-weight:bold;color:#1a1a2e">412 руб./м²</span>
      </td>
    </tr>
    <tr>
      <td style="padding:12px 0;border-bottom:1px solid #eee">
        <strong style="color:#e87722">ДЕТСКАЯ плитка Нефрит-Керамика</strong> для школ и детских садов — серия Kids<br>
        <span style="font-size:18px;font-weight:bold;color:#1a1a2e">от 617 руб./м²</span><br>
        <a href="https://nefrit.ru/collections/Kids/" style="color:#1565c0">Смотреть коллекцию →</a>
      </td>
    </tr>
    <tr>
      <td style="padding:12px 0;border-bottom:1px solid #eee">
        <strong style="color:#e87722">УРАЛЬСКИЙ ГРАНИТ — GRANITEA / IDALGO / Керамика Будущего</strong><br>
        U100 (молочный, моноколор) 60×60 матовый<br>
        <span style="font-size:18px;font-weight:bold;color:#1a1a2e">774 руб./м²</span><br>
        <a href="https://www.uralgres.com/catalog/ural-granite/ural-facades/u100/" style="color:#1565c0">Смотреть →</a>
      </td>
    </tr>
    <tr>
      <td style="padding:12px 0">
        <strong style="color:#e87722">KERAMA MARAZZI</strong> — SG701390R Фрегат бежевый обрезной КГ 20×80<br>
        <span style="font-size:18px;font-weight:bold;color:#1a1a2e">1 502 руб./м²</span><br>
        <a href="https://kerama-marazzi.com/catalog/gres/sg701390r/" style="color:#1565c0">Смотреть →</a>
      </td>
    </tr>
  </table>

  <hr style="border:none;border-top:2px solid #e87722;margin:20px 0">

  <p style="font-size:13px;color:#555">
    <strong>Наши заводы-производители:</strong><br>
    Cersanit · Шахтинская плитка (GraciaCeramica) · Нефрит-Керамика · Квадро-Декор ·
    Керама-Марацци · Азори (Керабуд) · Уральский Гранит (Idalgo) · Керамика Будущего · Granitea · Daco
  </p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:20px">
    <strong>Склад и большой шоурум в Янино</strong> — доставка и самовывоз<br><br>
    Менеджер по продажам <strong>Роман Новожилов</strong><br>
    📞 <a href="tel:+79052050900" style="color:#e87722">+7 (905) 205-09-00</a><br>
    🌐 <a href="http://www.plitki-spb.ru" style="color:#e87722">www.plitki-spb.ru</a>
  </div>

  <p style="font-size:11px;color:#aaa;margin-top:16px">
    Если вы не хотите получать наши письма — просто ответьте «Отписаться».
  </p>

</body>
</html>
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

def load_all_records(sheet):
    all_rows = sheet.get_all_values()
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
        sheet.batch_update(updates)
    log.info(f'Сброшено флагов: {len(updates)}')

def mark_sent(sheet, row_num, month_str):
    sheet.update_cell(row_num, 3, month_str)

def mark_dead(sheet, row_num, reason):
    try:
        sheet.update_cell(row_num, 2, f'dead:{reason[:60]}')
    except Exception as ex:
        log.warning(f'Не удалось пометить строку {row_num}: {ex}')

def delete_dead_rows(sheet):
    all_rows = sheet.get_all_values()
    if not all_rows:
        return 0
    dead = [
        i + 1
        for i, row in enumerate(all_rows)
        if len(row) > 1 and str(row[1]).startswith('dead:')
    ]
    for row_num in sorted(dead, reverse=True):
        sheet.delete_rows(row_num)
        time.sleep(0.3)
    return len(dead)

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
        log.info('Рассылка пропущена — вне окна 12:00–16:00 МСК')
        return

    log.info(f'─── Рассылка — месяц {month_str} ───')
    sent, dead, errors = run_mailing(sheet, records, month_str)

    if dead > 0:
        log.info('─── Удаление мёртвых адресов ───')
        deleted = delete_dead_rows(sheet)
        log.info(f'Удалено: {deleted}')
    else:
        deleted = 0

    log.info('═══════════════════════════════════════════')
    log.info(f'ИТОГ: отправлено={sent} | мёртвых удалено={deleted} | ошибок={errors}')
    log.info('═══════════════════════════════════════════')

if __name__ == '__main__':
    main()
