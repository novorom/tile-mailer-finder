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

SENDER_EMAIL = 'kadimborzota@gmail.com'
SENDER_NAME  = 'Roman Novozhilov — Sales Manager'
REPLY_TO     = 'novorom@gmail.com'

SHEET_ID   = os.environ.get('SPAIN_SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')

SEND_HOUR_FROM = 9
SEND_HOUR_TO   = 18
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

EMAIL_SUBJECT = "Sales Manager / Export Sales — Roman Novozhilov (20+ años de experiencia B2B)"

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.6;max-width:650px;margin:0 auto;padding:20px">

  <p>Estimado/a Responsable de Selección de Personal,</p>

  <p>Me dirijo a usted para expresar mi interés en oportunidades como <strong>Sales Manager, Export Sales Manager, Jefe de Ventas, Jefe de Exportaciones, Director Comercial o Ejecutivo de Ventas</strong> en su empresa.</p>

  <p>Con más de 20 años de experiencia demostrada en ventas mayoristas B2B y exportación, actualmente resido en Benicàssim, España, desde hace 4 años y estoy disponible para incorporarme de inmediato.</p>

  <p>Estoy abierto tanto a <strong>posiciones de Sales Manager / Ejecutivo de Ventas</strong>, donde pueda participar directamente en el desarrollo de clientes y las ventas, como a posiciones de responsabilidad acordes con mi experiencia. Para mí son especialmente importantes un producto interesante, un mercado con potencial y la posibilidad de generar resultados comerciales reales para la empresa.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Por qué puedo aportar valor a su empresa:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>Más de 20 años de experiencia en ventas mayoristas B2B</strong> con importantes fabricantes rusos (azulejos cerámicos y porcelánico)</li>
      <li><strong>Titulación de ingeniero en Sistemas Electrónicos</strong> — entiendo el producto y su componente técnico, no solo el proceso de ventas</li>
      <li><strong>Conocimiento profundo de los mercados de Rusia y la CEI</strong> — puedo aportar valor en el desarrollo de las ventas de exportación</li>
      <li><strong>Amplia red de contactos</strong> en los sectores de construcción, distribución y retail</li>
      <li><strong>Experiencia demostrada en ventas de exportación</strong> y desarrollo de negocios internacionales</li>
      <li><strong>Experiencia en IA y automatización</strong> — desarrollo proyectos automatizados utilizando herramientas modernas de IA. Entre ellos, he creado mi propio sistema de automatización para la búsqueda de empleo, completamente alojado en GitHub y ejecutado automáticamente mediante GitHub Actions. Esto demuestra mi capacidad para aprender rápidamente nuevas tecnologías, utilizarlas para resolver problemas comerciales prácticos y desarrollar proyectos complejos de forma autónoma</li>
      <li><strong>Experiencia en e-commerce</strong> — gestiono mis propias tiendas online (plitki-spb.ru, cersanit-spb.ru) con una base de datos de más de 3.000 contactos</li>
      <li><strong>Experiencia en formación de equipos comerciales</strong> y organización de ferias comerciales</li>
      <li><strong>Fluidez en ruso e inglés</strong> (C1), español básico</li>
      <li><strong>Vivo en España desde hace 4 años</strong> — no necesito reubicación y estoy disponible para incorporarme de inmediato</li>
    </ul>
  </div>

  <p><strong>Estoy especialmente interesado en empresas orientadas a la exportación en los siguientes sectores:</strong></p>
  <ul>
    <li>Azulejos cerámicos y materiales de construcción</li>
    <li>Alimentación y productos agrícolas (cítricos, frutas, vino)</li>
    <li>Muebles, textiles y calzado</li>
    <li>Recambios de automoción y maquinaria industrial</li>
    <li>Productos químicos, plásticos y productos metálicos</li>
  </ul>

  <p>Estoy disponible para una entrevista presencial o por videoconferencia en el momento que le resulte conveniente y puedo incorporarme de inmediato.</p>

  <p>Puede consultar mi perfil de LinkedIn para conocer mi CV y experiencia profesional con más detalle. Estaré encantado de conversar sobre cómo mi experiencia y conocimientos pueden contribuir al crecimiento de las ventas y del negocio de exportación de su empresa.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Sales Manager / Export Sales — 20+ años de experiencia en ventas B2B y exportación<br><br>
    📞 <a href="tel:+34605650553" style="color:#e87722">+34 605 650 553</a><br>
    📧 <a href="mailto:novorom@gmail.com" style="color:#e87722">novorom@gmail.com</a><br>
    🔗 <a href="https://www.linkedin.com/in/roman-novozhilov-b956b780/?locale=es-ES" style="color:#e87722">LinkedIn</a><br>
    📍 Benicàssim, España — disponible inmediatamente
  </div>

  <hr style="margin:32px 0;border:0;border-top:1px solid #ddd">

  <p style="font-size:12px;color:#666;margin-bottom:8px">— English version below / Versión en inglés abajo —</p>

  <p>Dear Hiring Manager,</p>

  <p>I am writing to express my strong interest in opportunities as <strong>Sales Manager, Export Sales Manager, Sales Executive, Commercial Director, or Business Development Manager</strong> at your company.</p>

  <p>With 20+ years of proven B2B wholesale sales and export experience, I have been residing in Benicàssim, Spain for 4 years, and ready to contribute to your company's international growth immediately.</p>

  <p>I am open to both <strong>Sales Manager / Sales Executive</strong> positions where I can participate directly in client development and sales, as well as management positions appropriate to my experience. For me, an interesting product, a market with potential, and the opportunity to generate real commercial results for the company are especially important.</p>

  <div style="background:#f5f5f5;padding:16px 20px;border-left:4px solid #e87722;margin:20px 0">
    <h3 style="margin:0 0 12px 0;color:#1a1a2e">Why I Can Add Value to Your Company:</h3>
    <ul style="margin:0;padding-left:20px">
      <li><strong>20+ years in B2B wholesale sales</strong> with major Russian manufacturers (ceramic tiles, porcelain)</li>
      <li><strong>Engineering degree in Electronic Systems</strong> — I understand the product, not just the sales pitch</li>
      <li><strong>Deep knowledge of Russian/CIS markets</strong> — valuable for your export expansion</li>
      <li><strong>Extensive network</strong> in construction, distribution, and retail sectors</li>
      <li><strong>Proven export sales track record</strong> and international business development</li>
      <li><strong>AI & automation expertise</strong> — I build automated projects using AI assistants like Cascade (Devin IDE). This email you're reading is part of my job search automation project that lives entirely on GitHub and runs automatically via GitHub Actions. I wrote this automation system with AI assistance, demonstrating my ability to leverage cutting-edge AI tools for practical business solutions and showing proactivity in solving complex problems</li>
      <li><strong>E-commerce experience</strong> — running own online stores (plitki-spb.ru, cersanit-spb.ru) with 3,000+ contact database</li>
      <li><strong>Sales team training</strong> and trade show management experience</li>
      <li><strong>Fluent in Russian and English</strong> (C1), basic Spanish</li>
      <li><strong>Already living in Spain for 4 years</strong> — no relocation needed, available immediately</li>
    </ul>
  </div>

  <p><strong>I am particularly interested in export-oriented companies in the following sectors:</strong></p>
  <ul>
    <li>Ceramic tiles and construction materials</li>
    <li>Food and agricultural products (citrus, fruits, wine)</li>
    <li>Furniture, textiles, and footwear</li>
    <li>Automotive parts and industrial machinery</li>
    <li>Chemicals, plastics, and metal products</li>
  </ul>

  <p>I am available for an in-person or video interview at your convenience and can start immediately.</p>

  <p>You can consult my LinkedIn profile to learn more about my CV and professional experience. I would be delighted to discuss how my experience and knowledge can contribute to the growth of sales and export business of your company.</p>

  <div style="background:#1a1a2e;color:#fff;padding:16px 20px;border-radius:8px;margin-top:24px">
    <strong>Roman Novozhilov</strong><br>
    Sales Manager / Export Sales — 20+ years B2B Export Sales<br><br>
    📞 <a href="tel:+34605650553" style="color:#e87722">+34 605 650 553</a><br>
    📧 <a href="mailto:novorom@gmail.com" style="color:#e87722">novorom@gmail.com</a><br>
    🔗 <a href="https://www.linkedin.com/in/roman-novozhilov-b956b780/?locale=es-ES" style="color:#e87722">LinkedIn</a><br>
    📍 Benicàssim, Spain — available immediately
  </div>

  <p style="font-size:11px;color:#aaa;margin-top:16px">
    If you are not interested in receiving job applications, please reply "Remove".
  </p>

</body>
</html>
"""

EMAIL_BODY_TEXT = """\
Estimado/a Responsable de Selección de Personal:

Me dirijo a usted para expresar mi interés en oportunidades como Sales Manager, Export Sales Manager, Jefe de Ventas, Jefe de Exportaciones, Director Comercial o Ejecutivo de Ventas en su empresa.

Con más de 20 años de experiencia demostrada en ventas mayoristas B2B y exportación, actualmente resido en Benicàssim, España, desde hace 4 años y estoy disponible para incorporarme de inmediato.

Estoy abierto tanto a posiciones de Sales Manager / Ejecutivo de Ventas, donde pueda participar directamente en el desarrollo de clientes y las ventas, como a posiciones de responsabilidad acordes con mi experiencia. Para mí son especialmente importantes un producto interesante, un mercado con potencial y la posibilidad de generar resultados comerciales reales para la empresa.

Por qué puedo aportar valor a su empresa:
- Más de 20 años de experiencia en ventas mayoristas B2B con importantes fabricantes rusos (azulejos cerámicos y porcelánico)
- Titulación de ingeniero en Sistemas Electrónicos — entiendo el producto y su componente técnico, no solo el proceso de ventas
- Conocimiento profundo de los mercados de Rusia y la CEI — puedo aportar valor en el desarrollo de las ventas de exportación
- Amplia red de contactos en los sectores de construcción, distribución y retail
- Experiencia demostrada en ventas de exportación y desarrollo de negocios internacionales
- Experiencia en IA y automatización — desarrollo proyectos automatizados utilizando herramientas modernas de IA. Entre ellos, he creado mi propio sistema de automatización para la búsqueda de empleo, completamente alojado en GitHub y ejecutado automáticamente mediante GitHub Actions. Esto demuestra mi capacidad para aprender rápidamente nuevas tecnologías, utilizarlas para resolver problemas comerciales prácticos y desarrollar proyectos complejos de forma autónoma
- Experiencia en e-commerce — gestiono mis propias tiendas online (plitki-spb.ru, cersanit-spb.ru) con una base de datos de más de 3.000 contactos
- Experiencia en formación de equipos comerciales y organización de ferias comerciales
- Fluidez en ruso e inglés (C1), español básico
- Vivo en España desde hace 4 años — no necesito reubicación y estoy disponible para incorporarme de inmediato

Estoy especialmente interesado en empresas orientadas a la exportación en los siguientes sectores:
- Azulejos cerámicos y materiales de construcción
- Alimentación y productos agrícolas (cítricos, frutas, vino)
- Muebles, textiles y calzado
- Recambios de automoción y maquinaria industrial
- Productos químicos, plásticos y productos metálicos

Estoy disponible para una entrevista presencial o por videoconferencia en el momento que le resulte conveniente y puedo incorporarme de inmediato.

Puede consultar mi perfil de LinkedIn para conocer mi CV y experiencia profesional con más detalle. Estaré encantado de conversar sobre cómo mi experiencia y conocimientos pueden contribuir al crecimiento de las ventas y del negocio de exportación de su empresa.

Roman Novozhilov
Sales Manager / Export Sales — 20+ años de experiencia en ventas B2B y exportación
+34 605 650 553
novorom@gmail.com
LinkedIn: https://www.linkedin.com/in/roman-novozhilov-b956b780/?locale=es-ES
Benicàssim, España — disponible inmediatamente

— English version below / Versión en inglés abajo —

Dear Hiring Manager,

I am writing to express my strong interest in opportunities as Sales Manager, Export Sales Manager, Sales Executive, Commercial Director, or Business Development Manager at your company.

With 20+ years of proven B2B wholesale sales and export experience, I have been residing in Benicàssim, Spain for 4 years, and ready to contribute to your company's international growth immediately.

I am open to both Sales Manager / Sales Executive positions where I can participate directly in client development and sales, as well as management positions appropriate to my experience. For me, an interesting product, a market with potential, and the opportunity to generate real commercial results for the company are especially important.

Why I Can Add Value to Your Company:
- 20+ years in B2B wholesale sales with major Russian manufacturers (ceramic tiles, porcelain)
- Engineering degree in Electronic Systems — I understand the product, not just the sales pitch
- Deep knowledge of Russian/CIS markets — valuable for your export expansion
- Extensive network in construction, distribution, and retail sectors
- Proven export sales track record and international business development
- AI & automation expertise — I build automated projects using AI assistants like Cascade (Devin IDE). This email you're reading is part of my job search automation project that lives entirely on GitHub and runs automatically via GitHub Actions. I wrote this automation system with AI assistance, demonstrating my ability to leverage cutting-edge AI tools for practical business solutions and showing proactivity in solving complex problems
- E-commerce experience — running own online stores (plitki-spb.ru, cersanit-spb.ru) with 3,000+ contact database
- Sales team training and trade show management experience
- Fluent in Russian and English (C1), basic Spanish
- Already living in Spain for 4 years — no relocation needed, available immediately

I am particularly interested in export-oriented companies in the following sectors:
- Ceramic tiles and construction materials
- Food and agricultural products (citrus, fruits, wine)
- Furniture, textiles, and footwear
- Automotive parts and industrial machinery
- Chemicals, plastics, and metal products

I am available for an in-person or video interview at your convenience and can start immediately.

You can consult my LinkedIn profile to learn more about my CV and professional experience. I would be delighted to discuss how my experience and knowledge can contribute to the growth of sales and export business of your company.

Roman Novozhilov
Sales Manager / Export Sales — 20+ years B2B Export Sales
+34 605 650 553
novorom@gmail.com
LinkedIn: https://www.linkedin.com/in/roman-novozhilov-b956b780/?locale=es-ES
Benicàssim, Spain — available immediately

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
            log.warning(f"Ошибка Google Sheets API [{code}]: {str(ex)}. Попытка {attempt+1}/{max_retries} через {sleep_time:.2f} сек...")
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
        try:
            retry_gspread_call(sheet.update_cell, row_num, 3, month_str)
            log.info(f'✓ Отмечено как отправлено: {email}')
        except Exception as ex:
            log.warning(f'✗ Ошибка отметки отправки ({email}): {ex}')

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
    msg = MIMEMultipart('alternative')
    msg['Subject'] = EMAIL_SUBJECT
    msg['From'] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
    msg['To'] = to_email
    msg['Reply-To'] = REPLY_TO
    msg['Bcc'] = REPLY_TO  # Отправляем копию на REPLY_TO для контроля

    # Текстовая версия
    text_part = MIMEText(EMAIL_BODY_TEXT, 'plain', 'utf-8')
    msg.attach(text_part)

    # HTML версия
    html_part = MIMEText(EMAIL_BODY_HTML, 'html', 'utf-8')
    msg.attach(html_part)

    # Отправка через SMTP
    try:
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT, timeout=15) as server:
            server.starttls()
            server.login(BREVO_USER, BREVO_PASS)
            # Отправляем только на основной адрес (без BCC из-за ограничений Brevo)
            recipients = [to_email]
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        log.info(f'✓ Письмо отправлено: {to_email}')
        return 'ok', ''
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
        if meta['status'] == 'active' and not meta['sent'] and (meta['row'] % TOTAL_MAILERS) == (MAILER_INDEX % TOTAL_MAILERS)
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
            mark_sent(sheet, email, 'sent')
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
