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

EMAIL_SUBJECT = "Плитка и керамогранит от 400 ₽/м² — спеццены для строительных организаций"

EMAIL_BODY_TEXT = """\
Добрый день!

Распродаём остатки облицовочной плитки и керамогранита по специальным ценам, ниже обычных.
Подборка на этот месяц для строительных организаций и подрядчиков.

ОБЛИЦОВОЧНАЯ ПЛИТКА (Нефрит-Керамика):
Риф бежевый — 600 × 200 × 9 мм, Стандарт — 400 ₽/м²
Гермес коричневый — 400 × 250 × 8 мм, Стандарт — 450 ₽/м²
Нарни серый — 600 × 200 × 9 мм, Стандарт — 450 ₽/м²
Лия бежевый — 600 × 300 × 9 мм, Стандарт — 450 ₽/м²

КЕРАМОГРАНИТ:
Astaria Ice белый — 450 × 450 × 8 мм, ГОСТ (М-квадрат) — 600 ₽/м²
«Соль-перец», светло-серый, матовый — 300 × 300 × 7 мм (Квадро Декор) — 610 ₽/м²
Matera бежевый — 597 × 597 × 10 мм, ГОСТ (М-квадрат) — 950 ₽/м²

Цены указаны с НДС. Количество ограничено, остатки по каждой позиции уточняйте у менеджера.

Преимущества:
• Работаем по счёту — оплата безналом для организаций
• Расчёт количества — посчитаем нужный объём под ваш объект бесплатно
• Фото и сертификаты — пришлём по любой позиции по запросу

Наш склад:
Ленинградская область, Тосненский район, Тельмановское городское поселение, посёлок Войскорово, 14В
Пн–Пт, с 08:00 до 18:00

С уважением,
Роман Новожилов
Менеджер по продажам, ООО «ТФ Керамика»
+7 905 205-09-00
novorom@mail.ru
"""

EMAIL_BODY_HTML = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<title>Плитка и керамогранит от 400 ₽/м² — спеццены для строительных организаций</title>
<style>
  body { margin:0; padding:0; background:#dcd9d2; }
  table { border-collapse:collapse; }
  a { color:#23272b; }
  @media (max-width:640px) {
    .wrap { width:100% !important; }
    .px { padding-left:18px !important; padding-right:18px !important; }
    .h1 { font-size:29px !important; line-height:34px !important; }
    .benefit { display:block !important; width:100% !important; padding:0 0 14px 0 !important; }
    .btn-cell { display:block !important; width:100% !important; padding:0 0 10px 0 !important; }
    .tag-price { font-size:26px !important; }
  }
</style>
</head>
<body style="margin:0;padding:0;background:#dcd9d2;">

<!-- Прехедер: текст, который видно в списке писем -->
<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:#dcd9d2;font-size:1px;line-height:1px;">
  Облицовочная плитка от 400 ₽/м², керамогранит от 600 ₽/м². Остатки со склада в Ленинградской области, работаем по счёту.
</div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#dcd9d2">
<tr><td align="center" style="padding:24px 12px;">

<table role="presentation" class="wrap" width="640" cellpadding="0" cellspacing="0" border="0" style="width:640px;max-width:640px;background:#f4f2ee;">

  <!-- ШАПКА -->
  <tr>
    <td class="px" bgcolor="#23272b" style="background:#23272b;padding:22px 32px;font-family:Georgia,'Times New Roman',serif;font-size:22px;line-height:26px;font-weight:bold;color:#ffffff;">
      ТФ Керамика
      <div style="font-family:Arial,Helvetica,sans-serif;font-size:13px;line-height:18px;font-weight:normal;color:#b9b6ae;padding-top:4px;">
        Ежемесячное предложение для строительных организаций
      </div>
    </td>
  </tr>

  <!-- ПОЛОСА-«ПЛИТКА»: образцы цветов из подборки -->
  <tr>
    <td style="padding:0;font-size:0;line-height:0;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td width="16.66%" height="14" bgcolor="#d8c6a5" style="height:14px;line-height:14px;font-size:0;">&nbsp;</td>
          <td width="16.66%" height="14" bgcolor="#8d9297" style="height:14px;line-height:14px;font-size:0;">&nbsp;</td>
          <td width="16.66%" height="14" bgcolor="#f7f5ef" style="height:14px;line-height:14px;font-size:0;">&nbsp;</td>
          <td width="16.66%" height="14" bgcolor="#6a4a36" style="height:14px;line-height:14px;font-size:0;">&nbsp;</td>
          <td width="16.66%" height="14" bgcolor="#b8b3a9" style="height:14px;line-height:14px;font-size:0;">&nbsp;</td>
          <td width="16.7%" height="14" bgcolor="#d8c6a5" style="height:14px;line-height:14px;font-size:0;">&nbsp;</td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ГЛАВНЫЙ БЛОК -->
  <tr>
    <td class="px" style="padding:36px 32px 8px 32px;font-family:Arial,Helvetica,sans-serif;">
      <div class="h1" style="font-family:Georgia,'Times New Roman',serif;font-size:36px;line-height:42px;font-weight:bold;color:#23272b;">
        Плитка и керамогранит от&nbsp;400&nbsp;₽/м²
      </div>
      <div style="font-size:16px;line-height:24px;color:#4a4f55;padding-top:14px;">
        Добрый день! Распродаём остатки облицовочной плитки и керамогранита по специальным ценам, ниже обычных.
        Подборка на этот месяц для строительных организаций и подрядчиков.
      </div>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:18px;">
        <tr>
          <td bgcolor="#f2b705" style="background:#f2b705;padding:9px 14px;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:18px;font-weight:bold;color:#23272b;">
            Цены действуют до 31 октября или до окончания остатков
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ОБЛИЦОВОЧНАЯ ПЛИТКА -->
  <tr>
    <td class="px" style="padding:30px 32px 10px 32px;font-family:Georgia,'Times New Roman',serif;font-size:22px;line-height:28px;font-weight:bold;color:#23272b;">
      Облицовочная плитка
      <div style="font-family:Arial,Helvetica,sans-serif;font-size:13px;line-height:18px;font-weight:normal;color:#6b7075;padding-top:2px;">Производитель: Нефрит-Керамика</div>
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:0 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#ffffff" style="background:#ffffff;">

        <tr>
          <td style="padding:16px 18px;border-bottom:1px solid #e4e1da;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">Риф бежевый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">600 × 200 × 9 мм, Стандарт</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;border-bottom:1px solid #ffffff;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">400</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

        <tr>
          <td style="padding:16px 18px;border-bottom:1px solid #e4e1da;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">Гермес коричневый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">400 × 250 × 8 мм, Стандарт</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;border-bottom:1px solid #ffffff;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">450</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

        <tr>
          <td style="padding:16px 18px;border-bottom:1px solid #e4e1da;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">Нарни серый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">600 × 200 × 9 мм, Стандарт</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;border-bottom:1px solid #ffffff;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">450</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

        <tr>
          <td style="padding:16px 18px;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">Лия бежевый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">600 × 300 × 9 мм, Стандарт</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">450</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

      </table>
    </td>
  </tr>

  <!-- КЕРАМОГРАНИТ -->
  <tr>
    <td class="px" style="padding:32px 32px 10px 32px;font-family:Georgia,'Times New Roman',serif;font-size:22px;line-height:28px;font-weight:bold;color:#23272b;">
      Керамогранит
    </td>
  </tr>
  <tr>
    <td class="px" style="padding:0 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#ffffff" style="background:#ffffff;">

        <tr>
          <td style="padding:16px 18px;border-bottom:1px solid #e4e1da;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">Astaria Ice белый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">450 × 450 × 8 мм, ГОСТ</div>
            <div style="font-size:13px;line-height:18px;color:#6b7075;">М-квадрат (ProGRES Ceramica)</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;border-bottom:1px solid #ffffff;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">600</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

        <tr>
          <td style="padding:16px 18px;border-bottom:1px solid #e4e1da;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">«Соль-перец», светло-серый, матовый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">Технический, 300 × 300 × 7 мм</div>
            <div style="font-size:13px;line-height:18px;color:#6b7075;">Квадро Декор</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;border-bottom:1px solid #ffffff;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">610</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

        <tr>
          <td style="padding:16px 18px;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-size:17px;line-height:22px;font-weight:bold;color:#23272b;">Matera бежевый</div>
            <div style="font-size:14px;line-height:20px;color:#6b7075;padding-top:2px;">597 × 597 × 10 мм, ГОСТ</div>
            <div style="font-size:13px;line-height:18px;color:#6b7075;">М-квадрат (ProGRES Ceramica)</div>
          </td>
          <td width="118" align="center" bgcolor="#f2b705" style="width:118px;background:#f2b705;padding:12px 8px;font-family:Arial,Helvetica,sans-serif;color:#23272b;">
            <div class="tag-price" style="font-size:30px;line-height:32px;font-weight:bold;">950</div>
            <div style="font-size:12px;line-height:16px;">₽ за м²</div>
          </td>
        </tr>

      </table>
    </td>
  </tr>

  <!-- ПРИМЕЧАНИЕ -->
  <tr>
    <td class="px" style="padding:14px 32px 0 32px;font-family:Arial,Helvetica,sans-serif;font-size:13px;line-height:19px;color:#6b7075;">
      Цены указаны с НДС. Количество ограничено, остатки по каждой позиции уточняйте у менеджера.
    </td>
  </tr>

  <!-- ПРЕИМУЩЕСТВА -->
  <tr>
    <td class="px" style="padding:30px 32px 6px 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td class="benefit" width="33%" valign="top" style="padding-right:12px;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:20px;color:#4a4f55;">
            <div style="font-size:15px;line-height:20px;font-weight:bold;color:#23272b;padding-bottom:3px;border-top:3px solid #23272b;padding-top:9px;">Работаем по счёту</div>
            Оплата безналом для организаций
          </td>
          <td class="benefit" width="33%" valign="top" style="padding:0 6px;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:20px;color:#4a4f55;">
            <div style="font-size:15px;line-height:20px;font-weight:bold;color:#23272b;padding-bottom:3px;border-top:3px solid #23272b;padding-top:9px;">Расчёт количества</div>
            Посчитаем нужный объём под ваш объект бесплатно
          </td>
          <td class="benefit" width="33%" valign="top" style="padding-left:12px;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:20px;color:#4a4f55;">
            <div style="font-size:15px;line-height:20px;font-weight:bold;color:#23272b;padding-bottom:3px;border-top:3px solid #23272b;padding-top:9px;">Фото и сертификаты</div>
            Пришлём по любой позиции по запросу
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ПРИЗЫВ К ДЕЙСТВИЮ -->
  <tr>
    <td class="px" style="padding:24px 32px 8px 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#23272b" style="background:#23272b;">
        <tr>
          <td style="padding:26px 26px 24px 26px;font-family:Arial,Helvetica,sans-serif;">
            <div style="font-family:Georgia,'Times New Roman',serif;font-size:22px;line-height:28px;font-weight:bold;color:#ffffff;">
              Нужна плитка на объект?
            </div>
            <div style="font-size:15px;line-height:22px;color:#c9c6be;padding:8px 0 18px 0;">
              Назовите объём, подберём, зарезервируем и выставим счёт.
            </div>
            <table role="presentation" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td class="btn-cell" style="padding-right:10px;">
                  <table role="presentation" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                      <td bgcolor="#f2b705" style="background:#f2b705;">
                        <a href="tel:+79052050900" style="display:block;padding:13px 22px;font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:20px;font-weight:bold;color:#23272b;text-decoration:none;text-align:center;">Позвонить +7 905 205-09-00</a>
                      </td>
                    </tr>
                  </table>
                </td>
                <td class="btn-cell">
                  <table role="presentation" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                      <td style="border:2px solid #f2b705;">
                        <a href="https://t.me/flyroman" style="display:block;padding:11px 20px;font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:20px;font-weight:bold;color:#f2b705;text-decoration:none;text-align:center;">Написать в Telegram</a>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- СКЛАД -->
  <tr>
    <td class="px" style="padding:26px 32px 6px 32px;font-family:Arial,Helvetica,sans-serif;">
      <div style="font-family:Georgia,'Times New Roman',serif;font-size:20px;line-height:26px;font-weight:bold;color:#23272b;padding-bottom:8px;">Наш склад</div>
      <div style="font-size:15px;line-height:23px;color:#4a4f55;">
        Ленинградская область, Тосненский район, Тельмановское городское поселение, посёлок Войскорово, 14В<br>
        Пн–Пт, с 08:00 до 18:00
      </div>
      <div style="padding-top:10px;font-size:15px;line-height:22px;">
        <a href="https://yandex.ru/maps/?text=%D0%9B%D0%B5%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C%2C%20%D0%92%D0%BE%D0%B9%D1%81%D0%BA%D0%BE%D1%80%D0%BE%D0%B2%D0%BE%2C%2014%D0%92" style="color:#23272b;font-weight:bold;">Открыть на карте</a>
      </div>
    </td>
  </tr>

  <!-- ПОДПИСЬ -->
  <tr>
    <td class="px" style="padding:26px 32px 30px 32px;font-family:Arial,Helvetica,sans-serif;font-size:15px;line-height:22px;color:#4a4f55;">
      С уважением,<br>
      <b style="color:#23272b;">Роман Новожилов</b><br>
      Менеджер по продажам, ООО «ТФ Керамика»<br>
      <a href="tel:+79052050900" style="color:#23272b;text-decoration:none;">+7 905 205-09-00</a>,
      <a href="mailto:novorom@mail.ru" style="color:#23272b;">novorom@mail.ru</a>
    </td>
  </tr>

  <!-- ПОДВАЛ -->
  <tr>
    <td class="px" bgcolor="#e9e6df" style="background:#e9e6df;padding:18px 32px;font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:18px;color:#6b7075;">
      Вы получили это письмо как клиент ООО «ТФ Керамика». Цены и наличие актуальны на дату рассылки.<br>
      Если не хотите получать наши предложения, <a href="#unsubscribe" style="color:#6b7075;">отпишитесь здесь</a> или ответьте на письмо словом «Отписаться».
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
