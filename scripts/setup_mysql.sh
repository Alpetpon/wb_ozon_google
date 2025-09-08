#!/bin/bash

# Скрипт настройки MySQL триггеров и таблицы change_log

set -e

echo "🗄️ Настройка MySQL для сервиса синхронизации"

# Проверяем переменные окружения
if [ -f "config.env" ]; then
    export $(cat config.env | grep -v '^#' | xargs)
fi

# Получаем параметры подключения
MYSQL_HOST=${MYSQL_HOST:-localhost}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_DATABASE=${MYSQL_DATABASE:-sync_test}

echo "📡 Подключение к MySQL: $MYSQL_USER@$MYSQL_HOST/$MYSQL_DATABASE"

# Проверяем подключение к MySQL
if ! mysql -h"$MYSQL_HOST" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "USE $MYSQL_DATABASE;" 2>/dev/null; then
    echo "❌ Не удается подключиться к MySQL. Проверьте настройки в config.env"
    exit 1
fi

echo "✅ Подключение к MySQL успешно"

# Создаем таблицу change_log
echo "📋 Создание таблицы change_log..."
mysql -h"$MYSQL_HOST" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" < sync_service/triggers/change_log_table.sql

if [ $? -eq 0 ]; then
    echo "✅ Таблица change_log создана"
else
    echo "❌ Ошибка при создании таблицы change_log"
    exit 1
fi

# Создаем триггеры
echo "🔧 Создание триггеров..."
mysql -h"$MYSQL_HOST" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" < sync_service/triggers/create_triggers.sql

if [ $? -eq 0 ]; then
    echo "✅ Триггеры созданы"
else
    echo "❌ Ошибка при создании триггеров"
    exit 1
fi

# Проверяем созданные триггеры
echo "📊 Проверка созданных триггеров..."
TRIGGER_COUNT=$(mysql -h"$MYSQL_HOST" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -sN -e "
SELECT COUNT(*) FROM information_schema.TRIGGERS 
WHERE TRIGGER_SCHEMA = '$MYSQL_DATABASE' 
AND TRIGGER_NAME LIKE 'tr_%'
")

echo "✅ Создано триггеров: $TRIGGER_COUNT"

# Проверяем таблицу change_log
echo "📋 Проверка таблицы change_log..."
CHANGE_LOG_EXISTS=$(mysql -h"$MYSQL_HOST" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -sN -e "
SELECT COUNT(*) FROM information_schema.tables 
WHERE table_schema = '$MYSQL_DATABASE' 
AND table_name = 'change_log'
")

if [ "$CHANGE_LOG_EXISTS" -eq 1 ]; then
    echo "✅ Таблица change_log найдена"
else
    echo "❌ Таблица change_log не найдена"
    exit 1
fi

echo ""
echo "🎉 Настройка MySQL завершена!"
echo ""
echo "📝 Созданные объекты:"
echo "   - Таблица: change_log"
echo "   - Триггеры: $TRIGGER_COUNT штук"
echo ""
echo "⚡ Теперь все изменения в отслеживаемых таблицах будут записываться в change_log"
