import requests
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

sites = {
    'yell': 'https://www.yell.ru/spb/search/?q=плитка',
    'zoon': 'https://zoon.ru/spb/search/?q=плитка',
    'orgpage': 'https://www.orgpage.ru/санкт-петербург/?search=плитка',
    'flamp': 'https://spb.flamp.ru/search/плитка',
}

for name, url in sites.items():
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        print(f"{name}: {res.status_code}, len: {len(res.text)}")
    except Exception as e:
        print(f"{name}: error {e}")
