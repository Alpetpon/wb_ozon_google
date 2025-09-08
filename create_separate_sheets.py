#!/usr/bin/env python3
"""
Создание отдельных Google Sheets для больших таблиц
"""

import asyncio
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sync_service.clients.mysql_client import MySQLClient
from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader

# Пороговое значение для создания отдельной таблицы
LARGE_TABLE_THRESHOLD = 100000  # 100k записей

async def create_separate_sheets_for_large_tables():
    """Создает отдельные Google Sheets для больших таблиц"""
    
    print("📊 Создание отдельных Google Sheets для больших таблиц...")
    print("=" * 70)
    
    # Загружаем конфигурацию
    config_loader = ConfigLoader()
    config = config_loader.load_env_config()
    table_mapping = config_loader.load_table_mapping()
    table_configs = table_mapping['tables']
    
    # Создаем MySQL клиент
    mysql_client = MySQLClient(
        host=config['mysql'].host,
        user=config['mysql'].user,
        password=config['mysql'].password,
        database=config['mysql'].database,
        port=config['mysql'].port
    )
    
    # Создаем Google Sheets клиент
    sheets_client = GoogleSheetsClient(
        credentials_file=config['google_sheets'].credentials_file,
        spreadsheet_id=config['google_sheets'].spreadsheet_id
    )
    
    try:
        await mysql_client.create_pool(minsize=1, maxsize=2)
        await sheets_client.authenticate()
        
        print("✅ Подключения установлены")
        
        # Проверяем размеры всех таблиц
        large_tables = []
        
        for table_name, table_config in table_configs.items():
            if table_config.get('enabled', False) and table_config.get('sync_direction') == "mysql_to_sheets":
                try:
                    count = await mysql_client.get_table_count(table_name)
                    print(f"📋 {table_name}: {count:,} записей", end="")
                    
                    if count > LARGE_TABLE_THRESHOLD:
                        print(" 🔴 БОЛЬШАЯ ТАБЛИЦА - создадим отдельную Google Sheets")
                        large_tables.append({
                            'mysql_name': table_name,
                            'sheet_name': table_config.get('sheet_name', table_name),
                            'count': count,
                            'config': table_config
                        })
                    else:
                        print(" 🟢 малая таблица - остается в клиентских таблицах")
                        
                except Exception as e:
                    print(f"   ❌ Ошибка получения размера: {e}")
        
        if not large_tables:
            print("\n🎉 Больших таблиц не найдено! Все данные помещаются в существующие таблицы.")
            return
        
        print(f"\n📊 Найдено больших таблиц: {len(large_tables)}")
        
        # Создаем отдельные Google Sheets для больших таблиц
        created_sheets = {}
        
        for table_info in large_tables:
            table_name = table_info['mysql_name']
            sheet_name = table_info['sheet_name']
            count = table_info['count']
            
            print(f"\n🔧 Создание отдельной Google Sheets для '{table_name}' ({count:,} записей)...")
            
            try:
                # Создаем новую Google Sheets
                new_spreadsheet = await create_new_spreadsheet(sheets_client, f"BigData_{sheet_name}")
                
                if new_spreadsheet:
                    spreadsheet_id = new_spreadsheet['spreadsheetId']
                    created_sheets[table_name] = {
                        'spreadsheet_id': spreadsheet_id,
                        'sheet_name': sheet_name,
                        'url': f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}",
                        'count': count
                    }
                    print(f"   ✅ Создана Google Sheets: {spreadsheet_id}")
                    print(f"   🔗 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
                
            except Exception as e:
                print(f"   ❌ Ошибка создания Google Sheets для {table_name}: {e}")
        
        # Сохраняем информацию о созданных таблицах
        if created_sheets:
            await save_large_tables_config(created_sheets)
            
            print(f"\n🎉 Создано {len(created_sheets)} отдельных Google Sheets!")
            print("\n📋 СОЗДАННЫЕ ТАБЛИЦЫ:")
            for table_name, info in created_sheets.items():
                print(f"   📊 {table_name} → {info['spreadsheet_id']} ({info['count']:,} записей)")
                print(f"      🔗 {info['url']}")
            
            print(f"\n📝 Информация сохранена в файле: large_tables_mapping.json")
            print("📝 Теперь можно запустить синхронизацию больших таблиц отдельно")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await mysql_client.close_pool()

async def create_new_spreadsheet(sheets_client, title):
    """Создает новую Google Sheets таблицу"""
    try:
        # Используем Google Drive API для создания новой таблицы
        body = {
            'properties': {
                'title': title
            }
        }
        
        # Создаем новую таблицу через Sheets API
        response = await sheets_client._make_request(
            sheets_client.service.spreadsheets().create,
            body=body
        )
        
        return response
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы {title}: {e}")
        return None

async def save_large_tables_config(created_sheets):
    """Сохраняет конфигурацию больших таблиц в файл"""
    import json
    
    config_file = "/Users/alex/Downloads/Work/Разработка/Бот/large_tables_mapping.json"
    
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(created_sheets, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Конфигурация сохранена в: {config_file}")

if __name__ == "__main__":
    asyncio.run(create_separate_sheets_for_large_tables())
