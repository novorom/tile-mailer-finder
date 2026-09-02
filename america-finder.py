#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
America Remote Job Finder — Поиск удаленной работы в США
═══════════════════════════════════════════════════════════════
• Поиск удаленных позиций Sales Manager / Export Sales Manager в США
• B2B продажи, экспорт, бизнес развитие
• Керамическая промышленность, строительство, материалы
• Парсинг emails с сайтов компаний
• Сохранение в Google Sheets (столбцы D, E, F)
"""

import requests
import re
import os
import time
import logging
import json as json_module
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import google.generativeai as genai
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════

SHEET_ID = os.environ.get('SPAIN_SHEET_ID', '')
CREDS_JSON = os.environ.get('GOOGLE_CREDS', '')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')
GOOGLE_API_KEY_2 = os.environ.get('GOOGLE_API_KEY_2', '')  # Альтернативный ключ
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID', '')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
GEMINI_API_KEY_2 = os.environ.get('GEMINI_API_KEY_2', '')  # Альтернативный ключ
GEMINI_API_KEY_6 = os.environ.get('GEMINI_API_KEY_6', '')  # Ключ 6
HUNTER_API_KEY = os.environ.get('HUNTER_API_KEY', '')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Категории для поиска удаленной работы в США
REMOTE_JOB_CATEGORIES = [
    # Remote Sales Manager positions
    'remote sales manager USA',
    'remote export sales manager',
    'remote B2B sales representative',
    'remote account manager',
    'remote business development manager',
    'remote sales director',
    'work from home sales positions',
    'remote sales jobs USA',
    
    # Industry-specific remote sales
    'remote sales manager construction materials',
    'remote sales manager ceramic industry',
    'remote sales manager building materials',
    'remote export sales manager USA',
    'remote international sales manager',
    
    # Remote commercial roles
    'remote commercial director',
    'remote business development director',
    'remote account executive',
    'remote sales representative',
    
    # Location-specific remote work
    'remote sales jobs California',
    'remote sales jobs New York',
    'remote sales jobs Texas',
    'remote sales jobs Florida',
    
    # Company types
    'remote sales manufacturing companies',
    'remote sales distribution companies',
    'remote sales wholesale companies',
    'remote sales export companies',
    
    # Startups and AI companies
    'remote sales manager startup',
    'remote business development startup',
    'remote sales AI company',
    'remote sales tech startup',
    'remote sales SaaS company',
    'remote sales software startup',
    'remote sales fintech startup',
    'remote sales healthtech startup',
    'remote sales edtech startup',
    'remote sales proptech startup',
    
    # Technology companies
    'remote sales manager software company',
    'remote sales manager cloud services',
    'remote sales manager cybersecurity',
    'remote sales manager data analytics',
    'remote sales manager machine learning',
    'remote sales manager AI platform',
    'remote sales manager tech company',
    
    # AI and Machine Learning
    'remote sales manager artificial intelligence',
    'remote sales manager machine learning',
    'remote sales manager deep learning',
    'remote sales manager NLP',
    'remote sales manager computer vision',
    
    # SaaS and Cloud
    'remote sales manager SaaS',
    'remote sales manager cloud computing',
    'remote sales manager CRM software',
    'remote sales manager ERP software',
    'remote sales manager collaboration tools',
    
    # Fintech
    'remote sales manager fintech',
    'remote sales manager payments',
    'remote sales manager blockchain',
    'remote sales manager cryptocurrency',
    'remote sales manager financial software',
    
    # Healthtech
    'remote sales manager healthtech',
    'remote sales manager telemedicine',
    'remote sales manager medical software',
    'remote sales manager healthcare IT',
    
    # Edtech
    'remote sales manager edtech',
    'remote sales manager online learning',
    'remote sales manager educational software',
    'remote sales manager LMS platform',
    
    # Proptech
    'remote sales manager proptech',
    'remote sales manager real estate tech',
    'remote sales manager property management',
    'remote sales manager construction tech',
    
    # QA и тестирование ПО
    'remote QA engineer USA',
    'remote software tester USA',
    'remote quality assurance engineer',
    'remote test automation engineer',
    'remote manual tester',
    'remote QA analyst',
    'remote software testing jobs',
    'remote QA lead',
    'remote test manager',
    
    # Разработка ПО
    'remote software engineer USA',
    'remote full stack developer',
    'remote frontend developer',
    'remote backend developer',
    'remote web developer',
    'remote mobile developer',
    'remote DevOps engineer',
    'remote site reliability engineer',
    'remote software architect',
    'remote technical lead',
    
    # Product Management
    'remote product manager USA',
    'remote product owner',
    'remote technical product manager',
    'remote product analyst',
    'remote UX researcher',
    'remote UI designer',
    'remote product designer',
    
    # Data Science и Analytics
    'remote data scientist USA',
    'remote data analyst',
    'remote machine learning engineer',
    'remote AI engineer',
    'remote data engineer',
    'remote business intelligence analyst',
    'remote research scientist',
    
    # IT Project Management
    'remote project manager USA',
    'remote scrum master',
    'remote agile coach',
    'remote program manager',
    'remote delivery manager',
    
    # DevOps и Infrastructure
    'remote DevOps engineer USA',
    'remote cloud engineer',
    'remote infrastructure engineer',
    'remote platform engineer',
    'remote site reliability engineer',
    'remote Kubernetes engineer',
    'remote AWS engineer',
    'remote Azure engineer',
    'remote GCP engineer',
    
    # Cybersecurity
    'remote security engineer USA',
    'remote penetration tester',
    'remote security analyst',
    'remote incident responder',
    'remote security architect',
    'remote compliance analyst',
    
    # Database и Backend
    'remote database administrator',
    'remote backend engineer',
    'remote API developer',
    'remote microservices engineer',
    'remote system architect',
    
    # Mobile Development
    'remote iOS developer',
    'remote Android developer',
    'remote React Native developer',
    'remote Flutter developer',
    'remote mobile app developer',
    
    # Frontend Development
    'remote React developer',
    'remote Angular developer',
    'remote Vue.js developer',
    'remote JavaScript developer',
    'remote TypeScript developer',
    'remote CSS developer',
    
    # Backend Development
    'remote Python developer',
    'remote Java developer',
    'remote Node.js developer',
    'remote Go developer',
    'remote Ruby developer',
    'remote PHP developer',
    'remote C# developer',
    'remote C++ developer',
    
    # Technical Writing и Documentation
    'remote technical writer',
    'remote documentation specialist',
    'remote API documentation writer',
    'remote developer advocate',
    'remote developer relations',
    
    # Customer Success и Support
    'remote customer success manager',
    'remote technical support engineer',
    'remote solutions architect',
    'remote implementation manager',
    'remote customer onboarding specialist',
    
    # Sales Engineering
    'remote sales engineer',
    'remote solutions consultant',
    'remote pre-sales engineer',
    'remote technical account manager',
    'remote customer success engineer',
]

# Статический список американских компаний для удаленной работы (fallback)
# Выбраны компании среднего размера, где email адреса более доступны
STATIC_AMERICAN_COMPANIES = [
    # Керамическая промышленность (средние компании)
    {'name': 'Crossville Inc', 'website': 'https://www.crossvilleinc.com'},
    {'name': 'Florida Tile', 'website': 'https://www.floridatile.com'},
    {'name': 'Emser Tile', 'website': 'https://www.emser.com'},
    {'name': 'StonePeak Ceramics', 'website': 'https://www.stonepeakceramics.com'},
    {'name': 'MSI Surfaces', 'website': 'https://www.msinternational.com'},
    {'name': 'Marazzi USA', 'website': 'https://www.marazziusa.com'},
    {'name': 'American Olean', 'website': 'https://www.americanolean.com'},
    {'name': 'Daltile', 'website': 'https://www.daltile.com'},
    
    # Строительные материалы (региональные дистрибьюторы)
    {'name': 'Floor & Decor', 'website': 'https://www.flooranddecor.com'},
    {'name': 'Tile Shop', 'website': 'https://www.tileshop.com'},
    {'name': 'Lumber Liquidators', 'website': 'https://www.lumberliquidators.com'},
    {'name': 'Builders FirstSource', 'website': 'https://www.bldr.com'},
    {'name': '84 Lumber', 'website': 'https://www.84lumber.com'},
    {'name': 'Menards', 'website': 'https://www.menards.com'},
    {'name': 'Do It Best', 'website': 'https://www.doitbest.com'},
    {'name': 'Ace Hardware', 'website': 'https://www.acehardware.com'},
    
    # Оптовые дистрибьюторы (региональные)
    {'name': 'Anixter', 'website': 'https://www.anixter.com'},
    {'name': 'WESCO International', 'website': 'https://www.wesco.com'},
    {'name': 'Graybar Electric', 'website': 'https://www.graybar.com'},
    {'name': 'Rexel USA', 'website': 'https://www.rexelusa.com'},
    {'name': 'MSC Industrial Supply', 'website': 'https://www.mscdirect.com'},
    {'name': 'Fastenal', 'website': 'https://www.fastenal.com'},
    {'name': 'Sonepar', 'website': 'https://www.sonepar.com'},
    
    # Логистика и экспорт (средние компании)
    {'name': 'Flexport', 'website': 'https://www.flexport.com'},
    {'name': 'XPO Logistics', 'website': 'https://www.xpo.com'},
    {'name': 'J.B. Hunt Transport', 'website': 'https://www.jbhunt.com'},
    {'name': 'C.H. Robinson', 'website': 'https://www.chrobinson.com'},
    {'name': 'Expeditors International', 'website': 'https://www.expeditors.com'},
    
    # Производство (средние компании)
    {'name': 'USG Corporation', 'website': 'https://www.usg.com'},
    {'name': 'Owens Corning', 'website': 'https://www.owenscorning.com'},
    {'name': 'Johns Manville', 'website': 'https://www.jm.com'},
    {'name': 'CertainTeed', 'website': 'https://www.certainteed.com'},
    {'name': 'PPG Industries', 'website': 'https://www.ppg.com'},
    
    # Строительные компании (региональные)
    {'name': 'Beacon Roofing Supply', 'website': 'https://www.beaconroofingsupply.com'},
    {'name': 'ABC Supply', 'website': 'https://www.abcsupply.com'},
    {'name': 'SRS Distribution', 'website': 'https://www.srsdistribution.com'},
    {'name': 'Carter Lumber', 'website': 'https://www.carterlumber.com'},
    {'name': 'Stock Building Supply', 'website': 'https://www.stockbuilding.com'},
    
    # Технологии (средние компании)
    {'name': 'Twilio', 'website': 'https://www.twilio.com'},
    {'name': 'Zendesk', 'website': 'https://www.zendesk.com'},
    {'name': 'Atlassian', 'website': 'https://www.atlassian.com'},
    {'name': 'Shopify', 'website': 'https://www.shopify.com'},
    {'name': 'Square', 'website': 'https://www.squareup.com'},
    
    # Фармацевтика (средние компании)
    {'name': 'Abbott Laboratories', 'website': 'https://www.abbott.com'},
    {'name': 'Medtronic', 'website': 'https://www.medtronic.com'},
    {'name': 'Baxter International', 'website': 'https://www.baxter.com'},
    {'name': 'Boston Scientific', 'website': 'https://www.bostonscientific.com'},
    {'name': 'Stryker Corporation', 'website': 'https://www.stryker.com'},
    
    # Промышленное оборудование (средние компании)
    {'name': 'Emerson Electric', 'website': 'https://www.emerson.com'},
    {'name': 'Rockwell Automation', 'website': 'https://www.rockwellautomation.com'},
    {'name': 'Danaher Corporation', 'website': 'https://www.danaher.com'},
    {'name': 'Illinois Tool Works', 'website': 'https://www.itw.com'},
    {'name': 'Parker Hannifin', 'website': 'https://www.parker.com'},
    
    # Энергетика (средние компании)
    {'name': 'NextEra Energy', 'website': 'https://www.nexteraenergy.com'},
    {'name': 'Duke Energy', 'website': 'https://www.duke-energy.com'},
    {'name': 'Southern Company', 'website': 'https://www.southerncompany.com'},
    {'name': 'American Electric Power', 'website': 'https://www.aep.com'},
    {'name': 'Xcel Energy', 'website': 'https://www.xcelenergy.com'},
    
    # Химическая промышленность (средние компании)
    {'name': 'Eastman Chemical', 'website': 'https://www.eastman.com'},
    {'name': 'LyondellBasell', 'website': 'https://www.lyondellbasell.com'},
    {'name': 'Celanese', 'website': 'https://www.celanese.com'},
    {'name': 'Ashland Global', 'website': 'https://www.ashland.com'},
    {'name': 'Huntsman Corporation', 'website': 'https://www.huntsman.com'},
    
    # Автомобильные поставщики (средние компании)
    {'name': 'BorgWarner', 'website': 'https://www.borgwarner.com'},
    {'name': 'Delphi Technologies', 'website': 'https://www.delphi.com'},
    {'name': 'Visteon Corporation', 'website': 'https://www.visteon.com'},
    {'name': 'Lear Corporation', 'website': 'https://www.lear.com'},
    {'name': 'Adient', 'website': 'https://www.adient.com'},
    
    # Финансовые услуги (региональные банки)
    {'name': 'US Bancorp', 'website': 'https://www.usbank.com'},
    {'name': 'PNC Financial Services', 'website': 'https://www.pnc.com'},
    {'name': 'Capital One Financial', 'website': 'https://www.capitalone.com'},
    {'name': 'TD Bank', 'website': 'https://www.tdbank.com'},
    {'name': 'Regions Financial', 'website': 'https://www.regions.com'},
    
    # Пищевая промышленность (средние компании)
    {'name': 'General Mills', 'website': 'https://www.generalmills.com'},
    {'name': 'Campbell Soup Company', 'website': 'https://www.campbellsoupcompany.com'},
    {'name': 'J.M. Smucker', 'website': 'https://www.jmsmucker.com'},
    {'name': 'Hormel Foods', 'website': 'https://www.hormel.com'},
    {'name': 'Conagra Brands', 'website': 'https://www.conagrabrands.com'},
    
    # Ритейл (региональные сети)
    {'name': 'Kroger', 'website': 'https://www.kroger.com'},
    {'name': 'Publix', 'website': 'https://www.publix.com'},
    {'name': 'Albertsons', 'website': 'https://www.albertsons.com'},
    {'name': 'Safeway', 'website': 'https://www.safeway.com'},
    {'name': 'Meijer', 'website': 'https://www.meijer.com'},
    
    # Телекоммуникации (региональные)
    {'name': 'Verizon Business', 'website': 'https://www.verizon.com/business'},
    {'name': 'AT&T Business', 'website': 'https://www.att.com/business'},
    {'name': 'Comcast Business', 'website': 'https://www.comcastbusiness.com'},
    {'name': 'Charter Business', 'website': 'https://www.spectrum.com/business'},
    {'name': 'Lumen Technologies', 'website': 'https://www.lumen.com'},
    
    # Startups и технологические компании (где email более доступны)
    {'name': 'Notion', 'website': 'https://www.notion.so'},
    {'name': 'Figma', 'website': 'https://www.figma.com'},
    {'name': 'Canva', 'website': 'https://www.canva.com'},
    {'name': 'Slack', 'website': 'https://slack.com'},
    {'name': 'Zoom', 'website': 'https://zoom.us'},
    {'name': 'Airtable', 'website': 'https://airtable.com'},
    {'name': 'Monday.com', 'website': 'https://monday.com'},
    {'name': 'Asana', 'website': 'https://asana.com'},
    {'name': 'Trello', 'website': 'https://trello.com'},
    {'name': 'Basecamp', 'website': 'https://basecamp.com'},
    
    # AI и Machine Learning стартапы
    {'name': 'OpenAI', 'website': 'https://openai.com'},
    {'name': 'Anthropic', 'website': 'https://www.anthropic.com'},
    {'name': 'Hugging Face', 'website': 'https://huggingface.co'},
    {'name': 'Stability AI', 'website': 'https://stability.ai'},
    {'name': 'Midjourney', 'website': 'https://www.midjourney.com'},
    {'name': 'Runway ML', 'website': 'https://runwayml.com'},
    {'name': 'Jasper AI', 'website': 'https://www.jasper.ai'},
    {'name': 'Copy.ai', 'website': 'https://copy.ai'},
    {'name': 'Writesonic', 'website': 'https://writesonic.com'},
    {'name': 'Grammarly', 'website': 'https://www.grammarly.com'},
    
    # SaaS стартапы
    {'name': 'HubSpot', 'website': 'https://www.hubspot.com'},
    {'name': 'Salesforce', 'website': 'https://www.salesforce.com'},
    {'name': 'Stripe', 'website': 'https://stripe.com'},
    {'name': 'Shopify', 'website': 'https://www.shopify.com'},
    {'name': 'Square', 'website': 'https://www.squareup.com'},
    {'name': 'Toast', 'website': 'https://pos.toasttab.com'},
    {'name': 'Deel', 'website': 'https://www.deel.com'},
    {'name': 'Remote', 'website': 'https://remote.com'},
    {'name': 'Pilot', 'website': 'https://pilot.com'},
    {'name': 'Oyster', 'website': 'https://www.oysterhr.com'},
    
    # Fintech стартапы
    {'name': 'Plaid', 'website': 'https://plaid.com'},
    {'name': 'Coinbase', 'website': 'https://www.coinbase.com'},
    {'name': 'Robinhood', 'website': 'https://robinhood.com'},
    {'name': 'Chime', 'website': 'https://www.chime.com'},
    {'name': 'Nubank', 'website': 'https://nubank.com'},
    {'name': 'Revolut', 'website': 'https://www.revolut.com'},
    {'name': 'Wise', 'website': 'https://wise.com'},
    {'name': 'Brex', 'website': 'https://www.brex.com'},
    {'name': 'Ramp', 'website': 'https://ramp.com'},
    {'name': 'Mercury', 'website': 'https://mercury.com'},
    
    # Healthtech стартапы
    {'name': 'Teladoc', 'website': 'https://www.teladochealth.com'},
    {'name': 'Ro', 'website': 'https://ro.co'},
    {'name': 'Hims & Hers', 'website': 'https://www.hims.com'},
    {'name': 'Cerebral', 'website': 'https://cerebral.com'},
    {'name': 'BetterHelp', 'website': 'https://www.betterhelp.com'},
    {'name': 'Talkspace', 'website': 'https://www.talkspace.com'},
    {'name': 'Headspace', 'website': 'https://www.headspace.com'},
    {'name': 'Calm', 'website': 'https://www.calm.com'},
    {'name': 'Noom', 'website': 'https://www.noom.com'},
    {'name': 'Oura', 'website': 'https://ouraring.com'},
    
    # Edtech стартапы
    {'name': 'Coursera', 'website': 'https://www.coursera.org'},
    {'name': 'Udemy', 'website': 'https://www.udemy.com'},
    {'name': 'edX', 'website': 'https://www.edx.org'},
    {'name': 'Khan Academy', 'website': 'https://www.khanacademy.org'},
    {'name': 'Duolingo', 'website': 'https://www.duolingo.com'},
    {'name': 'Quizlet', 'website': 'https://quizlet.com'},
    {'name': 'Chegg', 'website': 'https://www.chegg.com'},
    {'name': 'MasterClass', 'website': 'https://www.masterclass.com'},
    {'name': 'Skillshare', 'website': 'https://www.skillshare.com'},
    {'name': 'Pluralsight', 'website': 'https://www.pluralsight.com'},
    
    # Proptech стартапы
    {'name': 'Zillow', 'website': 'https://www.zillow.com'},
    {'name': 'Redfin', 'website': 'https://www.redfin.com'},
    {'name': 'Opendoor', 'website': 'https://www.opendoor.com'},
    {'name': 'Better.com', 'website': 'https://www.better.com'},
    {'name': 'Knock', 'website': 'https://www.knock.com'},
    {'name': 'Compass', 'website': 'https://www.compass.com'},
    {'name': 'Offerpad', 'website': 'https://www.offerpad.com'},
    {'name': 'Reali', 'website': 'https://www.reali.com'},
    {'name': 'ZeroDown', 'website': 'https://www.zerodown.com'},
    {'name': 'Flyhomes', 'website': 'https://www.flyhomes.com'},
    
    # Cybersecurity стартапы (средние компании)
    {'name': 'Tenable', 'website': 'https://www.tenable.com'},
    {'name': 'Rapid7', 'website': 'https://www.rapid7.com'},
    {'name': 'Darktrace', 'website': 'https://www.darktrace.com'},
    {'name': 'Cylance', 'website': 'https://www.cylance.com'},
    {'name': 'Carbon Black', 'website': 'https://www.carbonblack.com'},
    {'name': 'SentinelOne', 'website': 'https://www.sentinelone.com'},
    {'name': 'CyberArk', 'website': 'https://www.cyberark.com'},
    {'name': 'Varonis', 'website': 'https://www.varonis.com'},
    {'name': 'Exabeam', 'website': 'https://www.exabeam.com'},
    {'name': 'LogRhythm', 'website': 'https://logrhythm.com'},
    
    # Data Analytics стартапы (средние компании)
    {'name': 'Looker', 'website': 'https://looker.com'},
    {'name': 'Tableau', 'website': 'https://www.tableau.com'},
    {'name': 'Mode Analytics', 'website': 'https://mode.com'},
    {'name': 'Mixpanel', 'website': 'https://mixpanel.com'},
    {'name': 'Amplitude', 'website': 'https://amplitude.com'},
    {'name': 'Heap', 'website': 'https://heap.io'},
    {'name': 'Segment', 'website': 'https://segment.com'},
    {'name': 'Kissmetrics', 'website': 'https://kissmetrics.com'},
    {'name': 'FullStory', 'website': 'https://www.fullstory.com'},
    {'name': 'Hotjar', 'website': 'https://www.hotjar.com'},
    
    # Дополнительные технологические компании среднего размера
    {'name': 'GitLab', 'website': 'https://about.gitlab.com'},
    {'name': 'Atlassian', 'website': 'https://www.atlassian.com'},
    {'name': 'Jira', 'website': 'https://www.atlassian.com/software/jira'},
    {'name': 'Confluence', 'website': 'https://www.atlassian.com/software/confluence'},
    {'name': 'Trello', 'website': 'https://trello.com'},
    {'name': 'Miro', 'website': 'https://miro.com'},
    {'name': 'Figma', 'website': 'https://www.figma.com'},
    {'name': 'Sketch', 'website': 'https://www.sketch.com'},
    {'name': 'InVision', 'website': 'https://www.invisionapp.com'},
    {'name': 'Adobe Creative Cloud', 'website': 'https://www.adobe.com/creativecloud'},
    
    # Облачные сервисы среднего размера
    {'name': 'DigitalOcean', 'website': 'https://www.digitalocean.com'},
    {'name': 'Linode', 'website': 'https://www.linode.com'},
    {'name': 'Vultr', 'website': 'https://www.vultr.com'},
    {'name': 'Hetzner', 'website': 'https://www.hetzner.com'},
    {'name': 'OVHcloud', 'website': 'https://www.ovhcloud.com'},
    
    # DevOps инструменты среднего размера
    {'name': 'CircleCI', 'website': 'https://circleci.com'},
    {'name': 'Travis CI', 'website': 'https://travis-ci.org'},
    {'name': 'Jenkins', 'website': 'https://www.jenkins.io'},
    {'name': 'GitLab CI', 'website': 'https://about.gitlab.com/stages-devops-lifecycle/continuous-integration'},
    {'name': 'Bamboo', 'website': 'https://www.atlassian.com/software/bamboo'},
    
    # Monitoring и Observability
    {'name': 'Datadog', 'website': 'https://www.datadoghq.com'},
    {'name': 'New Relic', 'website': 'https://newrelic.com'},
    {'name': 'AppDynamics', 'website': 'https://www.appdynamics.com'},
    {'name': 'Splunk', 'website': 'https://www.splunk.com'},
    {'name': 'Sumo Logic', 'website': 'https://www.sumologic.com'},
    
    # API и Integration
    {'name': 'Postman', 'website': 'https://www.postman.com'},
    {'name': 'Swagger', 'website': 'https://swagger.io'},
    {'name': 'MuleSoft', 'website': 'https://www.mulesoft.com'},
    {'name': 'Zapier', 'website': 'https://zapier.com'},
    {'name': 'IFTTT', 'website': 'https://ifttt.com'},
    
    # E-commerce платформы среднего размера
    {'name': 'BigCommerce', 'website': 'https://www.bigcommerce.com'},
    {'name': 'Magento', 'website': 'https://magento.com'},
    {'name': 'WooCommerce', 'website': 'https://woocommerce.com'},
    {'name': 'Shopify Plus', 'website': 'https://www.shopify.com/plus'},
    {'name': 'Salesforce Commerce Cloud', 'website': 'https://www.salesforce.com/products/commerce-cloud/'},
]

# ══════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ══════════════════════════════════════════════════════

def get_sheet():
    creds_data = json_module.loads(CREDS_JSON)
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json_module.dump(creds_data, f)
        creds_file = f.name
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(creds_file, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1
    return sheet

def add_company_to_sheet(sheet, company_name, email, website, job_title):
    """Добавляет компанию в столбцы D, E, F (company, email, website, job_title)"""
    try:
        # Находим первую пустую строку в столбце D
        all_values = sheet.col_values(4)  # Столбец D
        next_row = len(all_values) + 1
        
        # Добавляем данные в столбцы D, E, F
        sheet.update_cell(next_row, 4, company_name)  # D
        sheet.update_cell(next_row, 5, email)  # E
        sheet.update_cell(next_row, 6, website)  # F
        sheet.update_cell(next_row, 7, job_title)  # G (job_title)
        
        log.info(f'  ➕ Добавлено: {company_name} | {email}')
        return True
    except Exception as ex:
        log.warning(f'Ошибка добавления в таблицу: {ex}')
        return False

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ DUCKDUCKGO (альтернатива Google)
# ══════════════════════════════════════════════════════

def search_duckduckgo_web(query):
    """Поиск через DuckDuckGo как альтернатива Google"""
    try:
        url = f'https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}'
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        results = []
        # DuckDuckGo возвращает результаты в <a class="result__a">
        for link in soup.find_all('a', class_='result__a'):
            href = link.get('href', '')
            title = link.get_text(strip=True)
            
            # DuckDuckGo использует редиректы, извлекаем реальный URL
            if 'uddg=' in href:
                import urllib.parse
                parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                real_url = parsed.get('uddg', [href])[0]
            else:
                real_url = href
            
            if real_url and 'duckduckgo' not in real_url:
                results.append({
                    'title': title,
                    'link': real_url,
                    'snippet': ''
                })
        
        if results:
            log.info(f'     [DuckDuckGo] найдено: {len(results)}')
            return results[:10]  # Берем первые 10
            
    except Exception as ex:
        log.warning(f'DuckDuckGo error: {ex}')
    
    return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ GOOGLE CUSTOM SEARCH
# ══════════════════════════════════════════════════════

def search_google_web(query):
    """Поиск через Google Custom Search с альтернативным ключом и round-robin"""
    api_keys = [GOOGLE_API_KEY, GOOGLE_API_KEY_2]
    valid_keys = [k for k in api_keys if k and GOOGLE_CSE_ID]
    
    if not valid_keys:
        return []
    
    # Round-robin: выбираем ключ по очереди для распределения нагрузки
    import random
    api_key = random.choice(valid_keys)
    
    try:
        url = f'https://www.googleapis.com/customsearch/v1?key={api_key}&cx={GOOGLE_CSE_ID}&q={query}&num=10'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'items' in data:
            results = []
            for item in data['items']:
                results.append({
                    'title': item.get('title', ''),
                    'link': item.get('link', ''),
                    'snippet': item.get('snippet', '')
                })
            log.info(f'     [Google Search] найдено: {len(results)}')
            return results
    except Exception as ex:
        log.warning(f'Google Search error (key {api_key[:10]}...): {ex}')
        # Пробуем второй ключ если первый не сработал
        for fallback_key in valid_keys:
            if fallback_key != api_key:
                try:
                    url = f'https://www.googleapis.com/customsearch/v1?key={fallback_key}&cx={GOOGLE_CSE_ID}&q={query}&num=10'
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'items' in data:
                        results = []
                        for item in data['items']:
                            results.append({
                                'title': item.get('title', ''),
                                'link': item.get('link', ''),
                                'snippet': item.get('snippet', '')
                            })
                        log.info(f'     [Google Search fallback] найдено: {len(results)}')
                        return results
                except Exception as ex2:
                    log.warning(f'Google Search fallback error: {ex2}')
                    continue
    
    return []

# ══════════════════════════════════════════════════════
#  ПАРСИНГ EMAIL
# ══════════════════════════════════════════════════════

def extract_emails_from_url(url):
    """Извлекает emails с веб-страницы"""
    log.info(f"       Парсинг: {url}")
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        emails = set()
        
        # Поиск в mailto ссылках
        for mailto in soup.select('a[href^="mailto:"]'):
            email = mailto['href'].replace('mailto:', '').split('?')[0].strip()
            if '@' in email:
                emails.add(email.lower())
        
        # Поиск в тексте (регулярка) - без исключения цифр перед @
        text = soup.get_text()
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        found_emails = re.findall(email_pattern, text)
        for email in found_emails:
            emails.add(email.lower())
        
        # Поиск в data-email атрибутах
        for elem in soup.find_all(attrs={'data-email': True}):
            email = elem['data-email']
            if '@' in email:
                emails.add(email.lower())
        
        # Поиск в meta тегах
        for meta in soup.find_all('meta'):
            if meta.get('name') in ['email', 'contact-email', 'reply-to']:
                content = meta.get('content', '')
                if '@' in content:
                    emails.add(content.lower())
        
        # Поиск в href атрибутах (иногда email там)
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '@' in href and 'mailto:' not in href:
                # Извлекаем email из href
                email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', href)
                if email_match:
                    emails.add(email_match.group(0).lower())
        
        # Менее строгая фильтрация
        valid_emails = []
        skip_patterns = ['example', 'test', 'noreply', 'no-reply', 'donotreply', 
                        'spam', 'devnull', 'null', 'localhost']
        
        for email in emails:
            email_lower = email.lower()
            if not any(pattern in email_lower for pattern in skip_patterns):
                if len(email) < 80:
                    # Проверяем базовую валидность
                    if email.count('@') == 1 and '.' in email.split('@')[1]:
                        valid_emails.append(email)
        
        if valid_emails:
            log.info(f"       Найдено emails: {len(valid_emails)}")
        else:
            log.info(f"       Emails не найдены")
        
        return valid_emails
    except Exception as ex:
        log.warning(f'Ошибка парсинга {url}: {ex}')
        return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ GEMINI AI
# ══════════════════════════════════════════════════════

def search_gemini_leads(query):
    """Поиск компаний через Gemini AI с 3 альтернативными ключами и round-robin"""
    api_keys = [GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_6]
    valid_keys = [k for k in api_keys if k]
    
    if not valid_keys:
        return []
    
    # Round-robin: выбираем ключ по очереди для распределения нагрузки
    import random
    api_key = random.choice(valid_keys)
    
    try:
        genai.configure(api_key=api_key)
        models_to_try = ['gemini-3.1-flash-lite', 'gemini-1.5-flash', 'gemini-pro']
        
        for model_name in models_to_try:
            try:
                model = genai.GenerativeModel(model_name)
                # Быстрая проверка доступности
                model.generate_content("test", generation_config={"max_output_tokens": 1})
                log.info(f'     [Gemini AI] модель: {model_name}')
                
                prompt = f"""
                Find 5-10 US companies that are hiring for: {query}
                
                Return ONLY a JSON array with this format:
                [
                    {{
                        "name": "Company Name",
                        "website": "https://example.com"
                    }}
                ]
                
                Focus on companies in: startups, AI, technology, software development, QA, DevOps.
                """
                
                response = model.generate_content(prompt, generation_config={"max_output_tokens": 2000})
                text = response.text
                
                # Извлечение JSON из ответа
                json_match = re.search(r'\[.*\]', text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    companies = json_module.loads(json_str)
                    return companies
                
            except Exception as ex:
                log.warning(f'Gemini {model_name} error: {ex}')
                continue
                
    except Exception as ex:
        log.warning(f'Gemini API key error: {ex}')
        # Пробуем остальные ключи если первый не сработал
        for fallback_key in valid_keys:
            if fallback_key != api_key:
                try:
                    genai.configure(api_key=fallback_key)
                    models_to_try = ['gemini-3.1-flash-lite', 'gemini-1.5-flash', 'gemini-pro']
                    
                    for model_name in models_to_try:
                        try:
                            model = genai.GenerativeModel(model_name)
                            model.generate_content("test", generation_config={"max_output_tokens": 1})
                            log.info(f'     [Gemini AI fallback] модель: {model_name}')
                            
                            prompt = f"""
                            Find 5-10 US companies that are hiring for: {query}
                            
                            Return ONLY a JSON array with this format:
                            [
                                {{
                                    "name": "Company Name",
                                    "website": "https://example.com"
                                }}
                            ]
                            
                            Focus on companies in: startups, AI, technology, software development, QA, DevOps.
                            """
                            
                            response = model.generate_content(prompt, generation_config={"max_output_tokens": 2000})
                            text = response.text
                            
                            json_match = re.search(r'\[.*\]', text, re.DOTALL)
                            if json_match:
                                json_str = json_match.group(0)
                                companies = json_module.loads(json_str)
                                return companies
                            
                        except Exception as ex2:
                            log.warning(f'Gemini fallback {model_name} error: {ex2}')
                            continue
                            
                except Exception as ex2:
                    log.warning(f'Gemini fallback key error: {ex2}')
                    continue
    
    return []

# ══════════════════════════════════════════════════════
#  ПОИСК ЧЕРЕЗ HUNTER.IO
# ══════════════════════════════════════════════════════

def search_hunter_emails(domain):
    """Поиск emails через Hunter.io с усиленным fallback"""
    if not HUNTER_API_KEY:
        return []
    
    try:
        url = f'https://api.hunter.io/v2/email-finder?domain={domain}&api_key={HUNTER_API_KEY}'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('data', {}).get('email'):
            email = data['data']['email']
            log.info(f'       [Hunter.io] найден: {email}')
            return [email]
        
        # Если email-finder не сработал, пробуем domain search
        url = f'https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}&limit=10'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        emails = []
        if data.get('data', {}).get('emails'):
            for email_data in data['data']['emails']:
                if email_data.get('value'):
                    emails.append(email_data['value'])
        
        if emails:
            log.info(f'       [Hunter.io] найдено: {len(emails)}')
            return emails[:5]  # Берем первые 5
            
    except Exception as ex:
        log.warning(f'Hunter.io error: {ex}')
    
    return []

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════

def main():
    log.info('═══════════════════════════════════════════')
    log.info(' America Remote Job Finder — запуск')
    log.info('═══════════════════════════════════════════')
    
    sheet = get_sheet()
    
    # Загружаем существующие emails из столбца E
    try:
        existing_emails = set()
        col_e_values = sheet.col_values(5)  # Столбец E
        for email in col_e_values:
            if email and '@' in email:
                existing_emails.add(email.lower())
        log.info(f'Существующих email в базе: {len(existing_emails)}')
    except Exception as e:
        log.warning(f'Ошибка загрузки существующих email: {e}')
        existing_emails = set()
    
    total_added = 0
    processed_domains = set()
    
    # Сначала обрабатываем статический список компаний
    log.info(f'Статических компаний: {len(STATIC_AMERICAN_COMPANIES)}')
    for company in STATIC_AMERICAN_COMPANIES:
        domain = urlparse(company['website']).netloc.lower().replace('www.', '')
        if domain not in processed_domains:
            processed_domains.add(domain)
            log.info(f'   » {company["name"]} (Static)')
            
            # Проверяем существование сайта
            if not check_site_exists(company['website']):
                log.info(f'     [!] Сайт недоступен')
                continue
            
            # Парсим emails с сайта
            emails = extract_emails_from_url(company['website'])
            
            # Если не нашли emails, пробуем Hunter.io
            if not emails and HUNTER_API_KEY:
                try:
                    domain = urlparse(company['website']).netloc.lower().replace('www.', '')
                    hunter_emails = search_hunter_emails(domain)
                    if hunter_emails:
                        emails.extend(hunter_emails)
                        log.info(f'       Hunter.io нашел: {len(hunter_emails)} emails')
                except Exception as e:
                    log.warning(f'Hunter.io error: {e}')
            
            for email in emails:
                if email.lower() not in existing_emails:
                    if add_company_to_sheet(sheet, company['name'], email, company['website'], 'Static'):
                        existing_emails.add(email.lower())
                        total_added += 1
                        log.info(f'     [OK] Email: {email}')
                else:
                    log.info(f'     [!] Email уже есть в базе')
            
            time.sleep(2)
    
    # Затем пробуем поиск через категории (если Google Search работает или используем DuckDuckGo)
    google_search_works = False
    try:
        test_search = search_google_web('test')
        if test_search:
            google_search_works = True
            log.info('Google Search доступен')
        else:
            log.warning('Google Search недоступен, пробуем DuckDuckGo')
    except Exception as e:
        log.warning(f'Google Search test failed: {e}')
    
    if not google_search_works:
        log.info('Используем DuckDuckGo для динамического поиска')
    
    # Всегда пробуем динамический поиск (Google или DuckDuckGo)
    for category in REMOTE_JOB_CATEGORIES[:30]:  # Ограничиваем 30 категорий для скорости
        log.info(f'\n🔍 Категория: {category}')
        
        candidates = []
        
        # Google Search (основной источник)
        if google_search_works:
            try:
                google_results = search_google_web(category)
                for result in google_results[:5]:
                    website = result.get('link', '')
                    if website:
                        domain = urlparse(website).netloc.lower().replace('www.', '')
                        company_name = domain.split('.')[0].capitalize()
                        candidates.append({
                            'name': company_name,
                            'website': website,
                            'source': 'Google Search'
                        })
            except Exception as e:
                log.warning(f'Google Search error: {e}')
        
        # DuckDuckGo (альтернатива)
        if not candidates or not google_search_works:
            try:
                ddg_results = search_duckduckgo_web(category)
                for result in ddg_results[:5]:
                    website = result.get('link', '')
                    if website:
                        domain = urlparse(website).netloc.lower().replace('www.', '')
                        company_name = domain.split('.')[0].capitalize()
                        candidates.append({
                            'name': company_name,
                            'website': website,
                            'source': 'DuckDuckGo'
                        })
            except Exception as e:
                log.warning(f'DuckDuckGo error: {e}')
            
            # Gemini fallback (с альтернативным ключом)
            try:
                gemini_results = search_gemini_leads(category)
                for company in gemini_results:
                    if 'name' in company and 'website' in company:
                        candidates.append({
                            'name': company['name'],
                            'website': company['website'],
                            'source': 'Gemini AI'
                        })
            except Exception as e:
                log.warning(f'Gemini error: {e}')
            
            # Уникализация по домену
            unique = {}
            for c in candidates:
                domain = None
                if c.get('website'):
                    try:
                        domain = urlparse(c['website']).netloc.lower().replace('www.', '')
                    except:
                        pass
                
                if domain and domain not in unique:
                    unique[domain] = c
            
            log.info(f'   Уникальных компаний: {len(unique)}')
            
            for domain, company in unique.items():
                if domain in processed_domains:
                    log.info(f'     [!] Домен {domain} уже обрабатывался')
                    continue
                
                processed_domains.add(domain)
                
                website = company.get('website')
                company_name = company.get('name', domain)
                
                log.info(f'   » {company_name} ({company.get("source")})')
                
                # Проверяем существование сайта
                if not check_site_exists(website):
                    log.info(f'     [!] Сайт недоступен')
                    continue
                
                # Парсим emails с сайта
                emails = extract_emails_from_url(website)
                
                # Если не нашли emails, пробуем Hunter.io
                if not emails and HUNTER_API_KEY:
                    try:
                        hunter_emails = search_hunter_emails(domain)
                        if hunter_emails:
                            emails.extend(hunter_emails)
                            log.info(f'       Hunter.io нашел: {len(hunter_emails)} emails')
                    except Exception as e:
                        log.warning(f'Hunter.io error: {e}')
                
                for email in emails:
                    if email.lower() not in existing_emails:
                        if add_company_to_sheet(sheet, company_name, email, website, category):
                            existing_emails.add(email.lower())
                            total_added += 1
                            log.info(f'     [OK] Email: {email}')
                    else:
                        log.info(f'     [!] Email уже есть в базе')
                
                time.sleep(2)  # Задержка между запросами
            
            time.sleep(3)  # Задержка между категориями
    
    log.info('═══════════════════════════════════════════')
    log.info(f'ИТОГ: добавлено компаний: {total_added}')
    log.info('═══════════════════════════════════════════')

def check_site_exists(url):
    """Проверяет доступность сайта"""
    try:
        response = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        return response.status_code < 400
    except:
        return False

if __name__ == '__main__':
    main()
