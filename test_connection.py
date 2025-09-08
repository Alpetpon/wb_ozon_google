#!/usr/bin/env python3
"""
Тестовый скрипт для проверки подключения к MySQL и Google Sheets
Запуск: python test_connection.py
"""

import asyncio
import os
import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.append(str(Path(__file__).parent))

from sync_service.clients.mysql_client import MySQLClient
from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader

async def test_mysql_connection():
    """Тест подключения к MySQL"""
    print("🔍 Тестирование подключения к MySQL...")
    
    try:
        # Загружаем конфигурацию
        config_loader = ConfigLoader()
        env_config = config_loader.load_env_config()
        mysql_config = env_config['mysql']
        
        # Создаем клиент
        mysql_client = MySQLClient(
            host=mysql_config.host,
            user=mysql_config.user,
            password=mysql_config.password,
            database=mysql_config.database,
            port=mysql_config.port
        )
        
        # Тестируем подключение
        await mysql_client.create_pool(minsize=1, maxsize=2)
        
        # Проверяем базовый запрос
        result = await mysql_client.fetch_one("SELECT 1 as test, NOW() as server_time")
        print(f"✅ MySQL подключение успешно!")
        print(f"   Сервер время: {result['server_time']}")
        
        # Проверяем таблицы
        tables_query = """
        SELECT TABLE_NAME, TABLE_ROWS 
        FROM information_schema.tables 
        WHERE table_schema = %s 
        AND TABLE_NAME IN ('tovar', 'tovar_wb', 'voronka_wb', 'detaliz_wb', 'change_log')
        ORDER BY TABLE_NAME
        """
        tables = await mysql_client.fetch_all(tables_query, (mysql_config.database,))
        
        print(f"📊 Найденные таблицы:")
        for table in tables:
            print(f"   - {table['TABLE_NAME']}: {table['TABLE_ROWS']} записей")
        
        # Проверяем change_log
        if any(t['TABLE_NAME'] == 'change_log' for t in tables):
            change_count = await mysql_client.fetch_one(
                "SELECT COUNT(*) as count FROM change_log WHERE status = 'PENDING'"
            )
            print(f"📋 change_log: {change_count['count']} ожидающих синхронизации")
        else:
            print("⚠️  Таблица change_log не найдена - запустите scripts/setup_mysql.sh")
        
        await mysql_client.close_pool()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка MySQL: {e}")
        return False

async def test_google_sheets_connection():
    """Тест подключения к Google Sheets"""
    print("\n🔍 Тестирование подключения к Google Sheets...")
    
    try:
        # Загружаем конфигурацию
        config_loader = ConfigLoader()
        env_config = config_loader.load_env_config()
        sheets_config = env_config['google_sheets']
        
        # Проверяем файл credentials
        if not Path(sheets_config.credentials_file).exists():
            print(f"❌ Файл credentials не найден: {sheets_config.credentials_file}")
            print("   Поместите google.json в папку credentials/")
            return False
        
        # Создаем клиент
        sheets_client = GoogleSheetsClient(
            credentials_file=sheets_config.credentials_file,
            spreadsheet_id=sheets_config.spreadsheet_id
        )
        
        # Тестируем аутентификацию
        await sheets_client.authenticate()
        print("✅ Google Sheets аутентификация успешна!")
        
        # Получаем информацию о таблице
        sheet_info = await sheets_client.get_sheet_info()
        title = sheet_info['properties']['title']
        print(f"📊 Подключена таблица: '{title}'")
        
        # Проверяем листы
        sheets = sheet_info.get('sheets', [])
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        print(f"📄 Найденные листы ({len(sheet_names)}):")
        
        # Проверяем наличие управляющих листов
        required_sheets = ['tech', 'list', 'настройки', 'настройки 2']
        for sheet_name in required_sheets:
            if sheet_name in sheet_names:
                print(f"   ✅ {sheet_name}")
            else:
                print(f"   ❌ {sheet_name} - отсутствует")
        
        # Проверяем листы данных
        data_sheets = [name for name in sheet_names if name.startswith(('wb_', 'ozon_'))]
        if data_sheets:
            print(f"📈 Листы данных ({len(data_sheets)}):")
            for sheet_name in data_sheets[:5]:  # Показываем первые 5
                print(f"   - {sheet_name}")
            if len(data_sheets) > 5:
                print(f"   ... и еще {len(data_sheets) - 5}")
        
        # Тестируем чтение листа tech
        if 'tech' in sheet_names:
            tech_data = await sheets_client.get_sheet_data('tech')
            if tech_data and len(tech_data) > 1:
                print(f"📋 Лист 'tech': {len(tech_data)-1} записей синхронизации")
                # Показываем заголовки
                headers = tech_data[0] if tech_data else []
                if headers:
                    print(f"   Заголовки: {', '.join(headers[:3])}...")
            else:
                print("⚠️  Лист 'tech' пуст или отсутствует")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка Google Sheets: {e}")
        return False

async def test_configuration():
    """Тест конфигурации"""
    print("\n🔍 Тестирование конфигурации...")
    
    try:
        config_loader = ConfigLoader()
        
        # Проверяем валидацию конфигурации
        is_valid = config_loader.validate_config()
        if is_valid:
            print("✅ Конфигурация валидна")
        else:
            print("❌ Ошибки в конфигурации")
            return False
        
        # Получаем настроенные таблицы
        enabled_tables = config_loader.get_enabled_tables()
        print(f"📊 Настроенных таблиц: {len(enabled_tables)}")
        
        # Группируем по типу
        wb_tables = [name for name in enabled_tables.keys() if name.endswith('_wb') or 'wb' in name]
        ozon_tables = [name for name in enabled_tables.keys() if not name.endswith('_wb') and 'wb' not in name]
        
        print(f"   🟢 WB таблиц: {len(wb_tables)}")
        print(f"   🔵 OZON таблиц: {len(ozon_tables)}")
        
        # Показываем первые несколько
        for name, config in list(enabled_tables.items())[:3]:
            print(f"   - {name} → '{config.sheet_name}' ({config.sync_direction})")
        
        if len(enabled_tables) > 3:
            print(f"   ... и еще {len(enabled_tables) - 3}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка конфигурации: {e}")
        return False

async def main():
    """Основная функция тестирования"""
    print("🚀 Тестирование системы синхронизации MySQL ↔ Google Sheets")
    print("=" * 60)
    
    # Проверяем переменные окружения
    print("🔧 Проверка переменных окружения...")
    config_file = Path("config.env")
    if config_file.exists():
        print("✅ Файл config.env найден")
        # Загружаем переменные
        with open(config_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    else:
        print("⚠️  Файл config.env не найден - используем config.env.example")
    
    # Запускаем тесты
    tests = [
        ("Конфигурация", test_configuration),
        ("MySQL", test_mysql_connection),
        ("Google Sheets", test_google_sheets_connection),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Критическая ошибка в тесте {test_name}: {e}")
            results.append((test_name, False))
    
    # Итоги
    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    
    passed = 0
    for test_name, result in results:
        if result:
            print(f"✅ {test_name}: ПРОЙДЕН")
            passed += 1
        else:
            print(f"❌ {test_name}: ОШИБКА")
    
    print(f"\n🎯 Итого: {passed}/{len(results)} тестов пройдено")
    
    if passed == len(results):
        print("\n🎉 Все тесты пройдены! Система готова к работе.")
        print("\n📝 Следующие шаги:")
        print("1. Запустите сервис: ./scripts/start.sh")
        print("2. Откройте API документацию: http://localhost:8000/docs")
        print("3. Проверьте статус: http://localhost:8000/health")
    else:
        print("\n⚠️  Есть ошибки. Исправьте их перед запуском системы.")
        print("\n📚 Справка:")
        print("- Настройка Google API: docs/google_setup.md")
        print("- Настройка MySQL: ./scripts/setup_mysql.sh")
        print("- Конфигурация: config.env")
    
    return passed == len(results)

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Тестирование прервано пользователем")
        exit(1)
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        exit(1)
