# Dockerfile для сервиса синхронизации MySQL <-> Google Sheets

FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY sync_service/ ./sync_service/
COPY config.env.example ./
COPY sync_service/config/table_mapping.yaml ./sync_service/config/

# Создаем директории для логов и credentials
RUN mkdir -p logs
RUN mkdir -p credentials

# Устанавливаем переменные окружения
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Порт для FastAPI
EXPOSE 8000

# Создаем пользователя для безопасности
RUN useradd -m -u 1000 syncuser && chown -R syncuser:syncuser /app
USER syncuser

# Команда запуска
CMD ["python", "-m", "sync_service.main"]
