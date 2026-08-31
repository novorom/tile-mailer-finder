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

SHEET_ID   = os.environ.get('SPAIN_SHEET_ID', '')
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

EMAIL_SUBJECT = "Solicitud de empleo: Jefe de Ventas / Exportaciones — Roman Novozhilov (CV adjunto)"

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.6;max-width:650px;margin:0 auto;padding:20px">

  <p>Dear Hiring Manager,</p>

  <p>I am writing to apply for <strong>Sales Manager</strong>, <strong>Export Sales Manager</strong>, or <strong>Commercial Director</strong> positions at your company.</p>

  <p>With <strong>20+ years of proven B2B wholesale sales and export experience</strong>, I am currently residing in Benicàssim, Spain, and ready to contribute to your company's international growth immediately.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Why I Can Add Value to Your Company:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>20+ years in B2B wholesale sales</strong> with major Russian manufacturers (ceramic tiles, porcelain)</li>
      <li><strong>Engineering degree in Electronic Systems</strong> — I understand the product, not just the sales pitch</li>
      <li><strong>Deep knowledge of Russian/CIS markets</strong> — valuable for your export expansion</li>
      <li><strong>Extensive network</strong> in construction, distribution, and retail sectors</li>
      <li><strong>Proven export sales track record</strong> and international business development</li>
      <li><strong>AI & automation expertise</strong> — built automated pipelines on GitHub Actions, using Claude, Windsurf, Devin for productivity</li>
      <li><strong>E-commerce experience</strong> — running own online stores (plitki-spb.ru, cersanit-spb.ru) with 3,000+ contact database</li>
      <li><strong>Sales team training</strong> and trade show management experience</li>
      <li><strong>Fluent in Russian and English</strong> (C1), conversational Spanish</li>
      <li><strong>Already living in Spain</strong> — no relocation needed, available immediately</li>
    </ul>
  </div>

  <p>I am particularly interested in export-oriented companies in:</p>
  <ul>
    <li>Ceramic tiles and construction materials</li>
    <li>Food and agricultural products (citrus, fruits, wine)</li>
    <li>Furniture, textiles, and footwear</li>
    <li>Automotive parts and industrial machinery</li>
    <li>Chemicals, plastics, and metal products</li>
  </ul>

  <p>I am available for a personal interview at your convenience and can start immediately.</p>

  <p>Please find my detailed CV attached. I would welcome the opportunity to discuss how my experience can benefit your organization's export growth.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Sales Manager — 20+ years B2B Export Sales<br><br>
    📞 <a href="tel:+79052050900" style="color:#e87722">+7 (905) 205-09-00</a><br>
    📧 <a href="mailto:novorom@mail.ru" style="color:#e87722">novorom@mail.ru</a><br>
    � <a href="https://www.linkedin.com/in/roman-novozhilov-b956b780/" style="color:#e87722">LinkedIn Profile</a><br>
    � Benicàssim, Spain (available immediately)
  </div>

  <hr style="margin:32px 0;border:0;border-top:1px solid #ddd">

  <p style="font-size:12px;color:#666;margin-bottom:8px">— Versión en español / Spanish version below —</p>

  <p>Estimado/a Responsable de Selección de Personal,</p>

  <p>Me dirijo a usted para solicitar el puesto de <strong>Jefe de Ventas</strong>, <strong>Jefe de Exportaciones</strong> o <strong>Director Comercial</strong> en su empresa.</p>

  <p>Con <strong>más de 20 años de experiencia probada en ventas mayoristas B2B y exportación</strong>, actualmente resido en Benicàssim, España, y estoy listo para contribuir al crecimiento internacional de su empresa de inmediato.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Por qué puedo aportar valor a su empresa:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>20+ años en ventas mayoristas B2B</strong> con importantes fabricantes rusos (azulejos cerámicos, porcelánico)</li>
      <li><strong>Título de ingeniero en Sistemas Electrónicos</strong> — entiendo el producto, no solo el discurso de ventas</li>
      <li><strong>Conocimiento profundo de los mercados de Rusia/CIS</strong> — valioso para su expansión de exportación</li>
      <li><strong>Red extensa</strong> en los sectores de construcción, distribución y retail</li>
      <li><strong>Historial comprobado en ventas de exportación</strong> y desarrollo de negocios internacionales</li>
      <li><strong>Experto en IA y automatización</strong> — construí pipelines automatizados en GitHub Actions, usando Claude, Windsurf, Devin para productividad</li>
      <li><strong>Experiencia en e-commerce</strong> — gestiono mis propias tiendas online (plitki-spb.ru, cersanit-spb.ru) con base de datos de 3,000+ contactos</li>
      <li><strong>Experiencia entrenando equipos de ventas</strong> y gestión de ferias comerciales</li>
      <li><strong>Fluido en ruso e inglés</strong> (C1), español conversacional</li>
      <li><strong>Ya vivo en España</strong> — no necesita reubicación, disponible inmediatamente</li>
    </ul>
  </div>

  <p>Estoy particularmente interesado en empresas orientadas a la exportación en:</p>
  <ul>
    <li>Azulejos cerámicos y materiales de construcción</li>
    <li>Alimentos y productos agrícolas (cítricos, frutas, vino)</li>
    <li>Muebles, textiles y calzado</li>
    <li>Repuestos automotrices y maquinaria industrial</li>
    <li>Químicos, plásticos y productos metálicos</li>
  </ul>

  <p>Estoy disponible para una entrevista personal en su conveniencia y puedo comenzar de inmediato.</p>

  <p>Adjunto mi CV detallado. Agradezco la oportunidad de discutir cómo mi experiencia puede beneficiar el crecimiento de exportación de su organización.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Jefe de Ventas — 20+ años Ventas B2B Exportación<br><br>
    📞 <a href="tel:+79052050900" style="color:#e87722">+7 (905) 205-09-00</a><br>
    📧 <a href="mailto:novorom@mail.ru" style="color:#e87722">novorom@mail.ru</a><br>
    🔗 <a href="https://www.linkedin.com/in/roman-novozhilov-b956b780/" style="color:#e87722">Perfil LinkedIn</a><br>
    📍 Benicàssim, España (disponible inmediatamente)
  </div>

  <p style="font-size:11px;color:#aaa;margin-top:16px">
    Si no está interesado en recibir solicitudes de empleo, responda "Remove".
  </p>

</body>
</html>
"""

EMAIL_BODY_TEXT = """\
Dear Hiring Manager,

I am writing to apply for Sales Manager, Export Sales Manager, or Commercial Director positions at your company.

With 20+ years of proven B2B wholesale sales and export experience, I am currently residing in Benicàssim, Spain, and ready to contribute to your company's international growth immediately.

Why I Can Add Value to Your Company:
- 20+ years in B2B wholesale sales with major Russian manufacturers (ceramic tiles, porcelain)
- Engineering degree in Electronic Systems — I understand the product, not just the sales pitch
- Deep knowledge of Russian/CIS markets — valuable for your export expansion
- Extensive network in construction, distribution, and retail sectors
- Proven export sales track record and international business development
- AI & automation expertise — built automated pipelines on GitHub Actions, using Claude, Windsurf, Devin for productivity
- E-commerce experience — running own online stores (plitki-spb.ru, cersanit-spb.ru) with 3,000+ contact database
- Sales team training and trade show management experience
- Fluent in Russian and English (C1), conversational Spanish
- Already living in Spain — no relocation needed, available immediately

I am particularly interested in export-oriented companies in:
- Ceramic tiles and construction materials
- Food and agricultural products (citrus, fruits, wine)
- Furniture, textiles, and footwear
- Automotive parts and industrial machinery
- Chemicals, plastics, and metal products

I am available for a personal interview at your convenience and can start immediately.

Please find my detailed CV attached. I would welcome the opportunity to discuss how my experience can benefit your organization's export growth.

Roman Novozhilov
Sales Manager — 20+ years B2B Export Sales
+7 (905) 205-09-00
novorom@mail.ru
LinkedIn: https://www.linkedin.com/in/roman-novozhilov-b956b780/
Benicàssim, Spain (available immediately)

— Versión en español / Spanish version below —

Estimado/a Responsable de Selección de Personal,

Me dirijo a usted para solicitar el puesto de Jefe de Ventas, Jefe de Exportaciones o Director Comercial en su empresa.

Con más de 20 años de experiencia probada en ventas mayoristas B2B y exportación, actualmente resido en Benicàssim, España, y estoy listo para contribuir al crecimiento internacional de su empresa de inmediato.

Por qué puedo aportar valor a su empresa:
- 20+ años en ventas mayoristas B2B con importantes fabricantes rusos (azulejos cerámicos, porcelánico)
- Título de ingeniero en Sistemas Electrónicos — entiendo el producto, no solo el discurso de ventas
- Conocimiento profundo de los mercados de Rusia/CIS — valioso para su expansión de exportación
- Red extensa en los sectores de construcción, distribución y retail
- Historial comprobado en ventas de exportación y desarrollo de negocios internacionales
- Experto en IA y automatización — construí pipelines automatizados en GitHub Actions, usando Claude, Windsurf, Devin para productividad
- Experiencia en e-commerce — gestiono mis propias tiendas online (plitki-spb.ru, cersanit-spb.ru) con base de datos de 3,000+ contactos
- Experiencia entrenando equipos de ventas y gestión de ferias comerciales
- Fluido en ruso e inglés (C1), español conversacional
- Ya vivo en España — no necesita reubicación, disponible inmediatamente

Estoy particularmente interesado en empresas orientadas a la exportación en:
- Azulejos cerámicos y materiales de construcción
- Alimentos y productos agrícolas (cítricos, frutas, vino)
- Muebles, textiles y calzado
- Repuestos automotrices y maquinaria industrial
- Químicos, plásticos y productos metálicos

Estoy disponible para una entrevista personal en su conveniencia y puedo comenzar de inmediato.

Adjunto mi CV detallado. Agradezco la oportunidad de discutir cómo mi experiencia puede beneficiar el crecimiento de exportación de su organización.

Roman Novozhilov
Jefe de Ventas — 20+ años Ventas B2B Exportación
+7 (905) 205-09-00
novorom@mail.ru
LinkedIn: https://www.linkedin.com/in/roman-novozhilov-b956b780/
Benicàssim, España (disponible inmediatamente)

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
