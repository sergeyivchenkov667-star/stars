# Court Audio Pipeline

Проект на **FastAPI**, реализующий пайплайн обработки аудио для судебных дел с использованием Celery и Redis.

---

## 🛠 Функционал

- Загрузка и обработка аудио файлов
- Пайплайн задач с очередями GPU и CPU через Celery
- Временные результаты сохраняются в `app/api/tmp/`
- Swagger UI для тестирования API

---

## 📦 Установка и запуск

### 1. Клонируем репозиторий

```bash
git clone https://gitlab.atsaero.ru/sivchenko/court-audio-pipeline.git
cd court-audio-pipeline
```
создать папки "data/audio" в директории "court-audio-pipeline/app/api" и поместить туда файлы: Адвокат.wav, Прокурор.wav и т.д
создать папку tmp в директории "court-audio-pipeline/app/api"
```bash
git checkout main
```
### 2. Настройка Python окружения
```bash
python3 --version          # Проверка версии Python
python3 -m venv .venv      # Создание виртуального окружения
source .venv/bin/activate  # Активация venv (Linux / WSL)
pip install --upgrade pip
pip install -r requirements.txt  # Установка зависимостей
```

## 🗄 Настройка PostgreSQL

Перед использованием проекта необходимо подготовить базу данных PostgreSQL.

---

### 1️⃣ Запуск PostgreSQL

Запустите сервер PostgreSQL:

```bash
### Старт
sudo service postgresql start


### Проверка статуса
sudo service postgresql status

### Вход в консоль PostgreSQL
sudo -u postgres psql

### Создание базы данных
CREATE DATABASE mydb;

### Создание пользователя и выдача прав
CREATE USER postgres WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE mydb TO postgres;
```

### 3. Поднимаем Docker сервисы
```bash
docker-compose up -d
```

### 4. Запуск Redis (WSL)
```bash
sudo service redis-server start
redis-cli ping  # Должно вернуть PONG
```

### 5. Запуск Celery и FastAPI
Необходимо открыть 3 терминала:

#### Терминал 1 — CPU воркер
celery -A app.celery_app worker -Q gpu -c 1 -P solo -n gpu@%h -l info
```bash
celery -A app.celery_app worker -Q cpu -c 4 -P threads -n cpu@%h -l info
```

#### Терминал 2 — GPU воркер
```bash
celery -A app.celery_app worker -Q gpu -c 1 -P solo -n gpu@%h -l info
```

#### Терминал 3 — FastAPI сервер
```bash
uvicorn app.pipeline.shag.Test_API:app --reload --host 0.0.0.0 --port 8000
```

### 6. Проверка API через Swagger
```bash
http://localhost:8000/docs#/
```