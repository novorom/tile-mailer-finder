# Tile Mailer Finder — Setup Guide 🚀

## ШАГ 1️⃣ — Создай новый репозиторий на GitHub

```
https://github.com/new
```

- Имя: `tile-mailer-finder`
- Описание: "Автоматический поиск и парсинг emails компаний СПб"
- Public репозиторий
- Инициализировать с README

---

## ШАГ 2️⃣ — Зарегистрируйся в API сервисах

### A) 2GIS API (основной поиск компаний)

**Что это:** Каталог компаний России с контактами, сайтами, телефонами

1. Перейди: https://dev.2gis.com
2. Нажми "Получить ключ"
3. Заполни форму (email, компания)
4. Подтверди email
5. В личном кабинете скопируй **API ключ**
6. Бесплатно: 10,000 запросов/месяц (достаточно!)

**Сохрани ключ: `TWOGIS_API_KEY`**

---

### B) Hunter.io API (парсинг корпоративных emails)

**Что это:** Поиск корпоративных email адресов по домену и компании

1. Перейди: https://hunter.io
2. Нажми "Sign up"
3. Создай аккаунт (email + пароль)
4. Подтверди email
5. В настройках → API в личном кабинете скопируй **API ключ**
6. Бесплатно: 50 запросов/день (можно расширить)

**Сохрани ключ: `HUNTER_API_KEY`**

---

### C) Google Sheets & Service Account (как раньше)

Если ещё не сделал — повтори как в старом проекте:

1. Google Cloud Console
2. Создай Service Account
3. Скопируй JSON ключ (GOOGLE_CREDS)
4. Создай новую Google Sheets таблицу (SHEET_ID)
5. Дай access Service Account email на эту таблицу

---

## ШАГ 3️⃣ — Структура проекта

Скопируй файлы в новый репозиторий:

```
tile-mailer-finder/
├── finder-agent.py          (основной скрипт поиска)
├── requirements.txt         (зависимости)
├── .github/
│   └── workflows/
│       └── schedule.yml     (GitHub Actions)
└── README.md
```

---

## ШАГ 4️⃣ — requirements.txt

```
gspread==6.1.2
google-auth==2.29.0
requests==2.31.0
beautifulsoup4==4.12.3
lxml==5.2.1
```

---

## ШАГ 5️⃣ — GitHub Secrets

Заходишь в репозиторий:
Settings → Secrets and variables → Actions → New repository secret

Добавляешь:

| Secret Name | Значение | Где взять |
|------------|----------|----------|
| `SHEET_ID` | ID твоей Google Sheets таблицы | URL таблицы (между /d/ и /edit) |
| `GOOGLE_CREDS` | JSON service account | Google Cloud Console |
| `TWOGIS_API_KEY` | API ключ 2GIS | https://dev.2gis.com |
| `HUNTER_API_KEY` | API ключ Hunter.io | https://hunter.io → Settings → API |

---

## ШАГ 6️⃣ — Google Sheets структура

Создай новую таблицу с колонками:

```
A: Название компании
B: Email
C: Сайт
D: Источник (2GIS / Hunter / Сайт)
E: Категория (строит / дизайн / ремонт)
F: Дата добавления
G: Статус (active / invalid / unsubscribed)
H: Примечания
```

---

## ШАГ 7️⃣ — Тестирование

1. Скопируй файл `finder-agent.py` в репозиторий
2. Добавь в `.github/workflows/schedule.yml`
3. Git push
4. В GitHub → Actions → Нажми "Run workflow" вручную
5. Смотри логи

---

## 📊 Как это работает:

1. **Каждый день в 02:00 МСК** (ночью) запускается скрипт
2. **2GIS API** ищет компании по категориям (строит, дизайн, ремонт и т.д.)
3. Для каждой компании:
   - Парсим сайт (ищем email в тексте и mailto)
   - Если не нашли → ищем через Hunter.io API
4. **Все найденные emails добавляются в Google Sheets**
5. Второй проект (tile-mailer) берёт emails из этой таблицы и рассылает письма

---

## 💰 Стоимость:

- **2GIS API:** Бесплатно 10,000 запросов/месяц ✅
- **Hunter.io API:** Бесплатно 50 запросов/день (платно: $99/месяц) ✅
- **Google Sheets:** Бесплатно ✅
- **GitHub Actions:** Бесплатно (3000 минут/месяц) ✅

**ИТОГО: 0 рублей! 🎉**

---

## 🚀 Когда будет готово:

Просто дай мне знать когда зарегистрировался в API сервисах, и я помогу с финальной настройкой!
