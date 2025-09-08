#!/bin/bash

# Скрипт установки и настройки сервиса синхронизации MySQL <-> Google Sheets

set -e

echo "🚀 Установка сервиса синхронизации MySQL <-> Google Sheets"

# Проверяем наличие Python 3.11+
echo "📋 Проверка Python..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не найден. Установите Python 3.11 или выше."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
if [[ $(echo "$PYTHON_VERSION 3.11" | awk '{print ($1 >= $2)}') == 0 ]]; then
    echo "❌ Требуется Python 3.11 или выше. Текущая версия: $PYTHON_VERSION"
    exit 1
fi

echo "✅ Python $PYTHON_VERSION найден"

# Создаем виртуальное окружение
echo "🔧 Создание виртуального окружения..."
python3 -m venv venv
source venv/bin/activate

# Обновляем pip
pip install --upgrade pip

# Устанавливаем зависимости
echo "📦 Установка зависимостей..."
pip install -r requirements.txt

# Создаем необходимые директории
echo "📁 Создание директорий..."
mkdir -p logs
mkdir -p credentials
mkdir -p sync_service/config

# Копируем пример конфигурации
if [ ! -f config.env ]; then
    echo "📄 Создание файла конфигурации..."
    cp config.env.example config.env
    echo "⚠️  Отредактируйте файл config.env с вашими настройками"
fi

# Проверяем наличие конфигурации маппинга
if [ ! -f sync_service/config/table_mapping.yaml ]; then
    echo "❌ Файл table_mapping.yaml не найден"
    echo "📄 Создан пример конфигурации в sync_service/config/table_mapping.yaml"
fi

echo ""
echo "🎉 Установка завершена!"
echo ""
echo "📝 Следующие шаги:"
echo "1. Отредактируйте config.env с вашими настройками MySQL и Google Sheets"
echo "2. Поместите google.json от Google в папку credentials/"
echo "3. Проверьте и настройте sync_service/config/table_mapping.yaml"
echo "4. Установите триггеры MySQL:"
echo "   mysql -u USER -p DATABASE < sync_service/triggers/change_log_table.sql"
echo "   mysql -u USER -p DATABASE < sync_service/triggers/create_triggers.sql"
echo "5. Запустите сервис: ./scripts/start.sh"
echo ""
echo "📚 Документация: https://developers.google.com/sheets/api/quickstart/python"
