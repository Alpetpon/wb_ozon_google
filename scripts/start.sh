#!/bin/bash

# Скрипт запуска сервиса синхронизации

set -e

echo "🚀 Запуск сервиса синхронизации MySQL <-> Google Sheets"

# Проверяем наличие виртуального окружения
if [ ! -d "venv" ]; then
    echo "❌ Виртуальное окружение не найдено. Запустите ./scripts/install.sh"
    exit 1
fi

# Активируем виртуальное окружение
echo "🔧 Активация виртуального окружения..."
source venv/bin/activate

# Проверяем наличие config.env
if [ ! -f "config.env" ]; then
    echo "❌ Файл config.env не найден. Скопируйте config.env.example и настройте его."
    exit 1
fi

# Проверяем наличие credentials.json
if [ ! -f "credentials/credentials.json" ]; then
    echo "❌ Файл credentials/credentials.json не найден."
    echo "Поместите ваш файл Google Service Account credentials в папку credentials/"
    exit 1
fi

# Проверяем наличие table_mapping.yaml
if [ ! -f "sync_service/config/table_mapping.yaml" ]; then
    echo "❌ Файл sync_service/config/table_mapping.yaml не найден."
    echo "Создайте файл конфигурации маппинга таблиц."
    exit 1
fi


# Загружаем переменные окружения
export $(cat config.env | grep -v '^#' | xargs)

# Создаем директорию для логов если её нет
mkdir -p logs

echo "✅ Предварительные проверки пройдены"
echo "🌐 Запуск сервиса на http://${API_HOST:-0.0.0.0}:${API_PORT:-8000}"
echo "📋 API документация: http://${API_HOST:-0.0.0.0}:${API_PORT:-8000}/docs"
echo "❤️  Статус здоровья: http://${API_HOST:-0.0.0.0}:${API_PORT:-8000}/health"
echo ""

# Запускаем сервис
python -m sync_service.main
