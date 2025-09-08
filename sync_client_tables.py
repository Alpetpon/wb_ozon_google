#!/usr/bin/env python3
"""
Синхронизация данных для всех клиентов согласно листу "tech"
"""

import asyncio
import sys
import os
from datetime import datetime

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sync_service.clients.mysql_client import MySQLClient
from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader
from sync_service.sync.mysql_to_sheets import MySQLToSheetsSync

def convert_table_name(sheet_name):
    """Преобразует имя таблицы из формата листа в формат конфигурации"""
    # Маппинг имен таблиц
    name_mapping = {
        'wb_voronka': 'voronka_wb',
        'wb_detaliz': 'detaliz_wb', 
        'wb_price': 'price_wb',
        'wb_tovar': 'tovar_wb',
        'wb_sklad': 'stock_wb',
        'wb_priem': 'priem_wb',
        'wb_reklam': 'rekl_wb',
        'ozon_detaliz': 'detaliz',
        'ozon_reklam': 'stat_rk',
        'ozon_tovar': 'tovar',
        'ozon_price': 'prices',
        'ozon_voronka': 'voronka',
        'ozon_zakfbo': 'zakaz_fbo',
        'ozon_prodfbo': 'report_fbo',
        'ozon_zakfbs': 'zakaz_fbs',
        'ozon_prodfbs': 'report_fbs'
    }
    return name_mapping.get(sheet_name, sheet_name)

async def sync_all_client_tables():
    """Синхронизация данных для всех клиентов"""
    
    print("🔄 Синхронизация данных для всех клиентов...")
    print("=" * 60)
    
    # Загружаем конфигурацию
    config_loader = ConfigLoader()
    config = config_loader.load_env_config()
    
    # Создаем клиенты
    mysql_client = MySQLClient(
        host=config['mysql'].host,
        user=config['mysql'].user,
        password=config['mysql'].password,
        database=config['mysql'].database,
        port=config['mysql'].port
    )
    
    try:
        await mysql_client.create_pool(minsize=1, maxsize=2)
        
        # Подключаемся к управляющей таблице
        admin_sheets_client = GoogleSheetsClient(
            credentials_file=config['google_sheets'].credentials_file,
            spreadsheet_id=config['google_sheets'].spreadsheet_id
        )
        await admin_sheets_client.authenticate()
        
        print("✅ Подключения установлены")
        
        # Читаем лист "tech" для получения списка клиентов
        print("\n📋 Чтение управляющей таблицы 'tech'...")
        tech_data = await admin_sheets_client.get_sheet_data('tech')
        
        if len(tech_data) < 2:
            print("❌ Лист 'tech' пуст или не содержит данных")
            return
        
        # Парсим заголовки
        headers = tech_data[0]
        print(f"📄 Заголовки: {', '.join(headers)}")
        
        # Обрабатываем каждую строку (кроме заголовка)
        clients = {}
        for row in tech_data[1:]:
            if len(row) >= 7:  # Проверяем что строка полная
                client_id = row[0]  # Код клиента
                table_id = row[1]   # ID таблицы
                table_name = row[2] # Таблица загрузки
                start_date = row[3] # Начало периода
                end_date = row[4]   # Конец периода
                loaded_to_db = row[5] # Выгружено в базу
                loaded_from_db = row[6] # Выгружено из базы
                
                if client_id and table_id and table_name:
                    if client_id not in clients:
                        clients[client_id] = {
                            'table_id': table_id,
                            'tables': []
                        }
                    clients[client_id]['tables'].append({
                        'name': table_name,
                        'start_date': start_date,
                        'end_date': end_date,
                        'loaded_to_db': loaded_to_db,
                        'loaded_from_db': loaded_from_db
                    })
        
        print(f"\n👥 Найдено клиентов: {len(clients)}")
        for client_id, client_data in clients.items():
            print(f"   📊 Клиент {client_id}: {len(client_data['tables'])} таблиц → {client_data['table_id']}")
        
        # Синхронизируем данные для каждого клиента
        total_synced = 0
        for client_id, client_data in clients.items():
            print(f"\n🔄 Синхронизация клиента {client_id}...")
            
            # Создаем клиент для Google Sheets клиента
            client_sheets = GoogleSheetsClient(
                credentials_file=config['google_sheets'].credentials_file,
                spreadsheet_id=client_data['table_id']
            )
            await client_sheets.authenticate()
            
            # Создаем синхронизатор
            sync = MySQLToSheetsSync(mysql_client, client_sheets, config['sync'])
            
            # Синхронизируем каждую таблицу клиента
            for table_info in client_data['tables']:
                table_name = table_info['name']
                print(f"   📋 Синхронизация таблицы '{table_name}'...")
                
                # Преобразуем имя таблицы из формата листа в формат конфигурации
                config_table_name = convert_table_name(table_name)
                
                # Получаем конфигурацию таблицы
                table_config = config_loader.get_table_config(config_table_name)
                if not table_config:
                    print(f"      ❌ Конфигурация для '{config_table_name}' (из '{table_name}') не найдена")
                    continue
                
                try:
                    # Синхронизируем таблицу (используем имя из конфигурации для MySQL)
                    result = await sync.sync_table(config_table_name, table_config)
                    
                    if result.get('status') == 'completed':
                        synced_count = result.get('synced', 0)
                        total_synced += synced_count
                        print(f"      ✅ Синхронизировано {synced_count} записей")
                        
                        # Обновляем статус в листе "tech"
                        await update_tech_status(admin_sheets_client, client_id, table_name, True)
                    else:
                        print(f"      ❌ Ошибка синхронизации: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    print(f"      ❌ Ошибка: {e}")
        
        print(f"\n🎉 Синхронизация завершена!")
        print(f"📊 Всего синхронизировано записей: {total_synced}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await mysql_client.close_pool()

async def update_tech_status(sheets_client, client_id, table_name, status):
    """Обновляет статус в листе tech"""
    try:
        # Читаем текущие данные
        tech_data = await sheets_client.get_sheet_data('tech')
        
        # Находим строку для обновления
        for i, row in enumerate(tech_data[1:], start=2):  # Пропускаем заголовок
            if len(row) >= 7 and row[0] == client_id and row[2] == table_name:
                # Обновляем статус "Выгружено из базы"
                range_name = f"G{i}"  # Колонка G (7-я колонка)
                await sheets_client.update_sheet_data('tech', range_name, [[str(status)]])
                print(f"      📝 Статус обновлен в листе 'tech'")
                break
    except Exception as e:
        print(f"      ⚠️ Не удалось обновить статус: {e}")

if __name__ == "__main__":
    asyncio.run(sync_all_client_tables())
