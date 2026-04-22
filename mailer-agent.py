#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tile Mailer Agent — Плитка & Керамогранит СПб
─────────────────────────────────────────────
- Ищет новые email строителей/дизайнеров/проектировщиков СПб и ЛО
- Добавляет новые адреса в Google Sheets
- Удаляет мёртвые (bounced) email из базы
- Рассылает письмо через Brevo SMTP (бесплатно, база без ограничений)
- Отправка ТОЛЬКО по будням с 12:00 до 16:00 МСК
- 300 писем/день — каждый день продолжает с того места где остановился
- 1-го числа месяца — сброс прогресса, новая рассылка
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
