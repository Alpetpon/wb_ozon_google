#!/usr/bin/env python3
"""
Очистка старых данных и синхронизация с ограничениями
"""

import asyncio
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sync_service.clients.mysql_client import MySQLClient
from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader

# Максимальное количество записей для каждой таблицы
MAX_RECORDS_PER_TABLE = 50000  # 50k записей = ~1-2 млн ячеек (при 20-40 колонках)

def convert_table_name(sheet_name):
    """Преобразует имя таблицы из формата листа в формат конфигурации"""
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

async def cleanup_and_sync():
    """Очистка старых данных и синхронизация новых с ограничениями"""
    
    print("🧹 Очистка и синхронизация данных с ограничениями по размеру")
    print("=" * 70)
    print(f"📊 Максимум записей на таблицу: {MAX_RECORDS_PER_TABLE:,}")
    print("=" * 70)
    
    # Загружаем конфигурацию
    config_loader = ConfigLoader()
    config = config_loader.load_env_config()
    
    # Создаем MySQL клиент
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
                
                if client_id and table_id and table_name:
                    if client_id not in clients:
                        clients[client_id] = {
                            'table_id': table_id,
                            'tables': []
                        }
                    clients[client_id]['tables'].append(table_name)
        
        print(f"\n👥 Найдено клиентов: {len(clients)}")
        
        # Анализируем размеры таблиц
        print(f"\n📊 Анализ размеров таблиц в MySQL:")
        table_mapping = config_loader.load_table_mapping()
        table_sizes = {}
        
        for table_name, table_config in table_mapping['tables'].items():
            if table_config.get('enabled', False) and table_config.get('sync_direction') == "mysql_to_sheets":
                try:
                    count = await mysql_client.get_table_count(table_name)
                    table_sizes[table_name] = count
                    
                    if count > MAX_RECORDS_PER_TABLE:
                        print(f"   🔴 {table_name}: {count:,} → будет синхронизировано {MAX_RECORDS_PER_TABLE:,} (ПОСЛЕДНИЕ)")
                    else:
                        print(f"   🟢 {table_name}: {count:,} → будет синхронизировано {count:,} (ВСЕ)")
                        
                except Exception as e:
                    print(f"   ❌ {table_name}: ошибка получения размера - {e}")
                    table_sizes[table_name] = 0
        
        # Обрабатываем каждого клиента
        total_synced = 0
        for client_id, client_data in clients.items():
            print(f"\n🔄 Обработка клиента {client_id}...")
            print(f"   📋 Таблиц для синхронизации: {len(client_data['tables'])}")
            
            # Создаем клиент для Google Sheets клиента
            client_sheets = GoogleSheetsClient(
                credentials_file=config['google_sheets'].credentials_file,
                spreadsheet_id=client_data['table_id']
            )
            await client_sheets.authenticate()
            
            # Получаем информацию о всех листах
            print(f"   📊 Проверка существующих листов...")
            sheet_info = await client_sheets.get_sheet_info()
            existing_sheets = []
            total_cells_used = 0
            
            for sheet in sheet_info.get('sheets', []):
                sheet_name = sheet['properties']['title']
                grid_props = sheet['properties']['gridProperties']
                rows = grid_props.get('rowCount', 0)
                cols = grid_props.get('columnCount', 0)
                cells = rows * cols
                total_cells_used += cells
                existing_sheets.append({
                    'name': sheet_name,
                    'rows': rows,
                    'cols': cols,
                    'cells': cells
                })
            
            print(f"   📊 Существующие листы: {len(existing_sheets)}")
            print(f"   📊 Всего использовано ячеек: {total_cells_used:,} / 10,000,000")
            
            # Удаляем старые листы, которые нужно пересинхронизировать
            sheets_to_sync = set()
            for table_name in client_data['tables']:
                sheets_to_sync.add(table_name)
            
            print(f"   🧹 Удаление старых листов для пересинхронизации...")
            for sheet in existing_sheets:
                if sheet['name'] in sheets_to_sync:
                    try:
                        await client_sheets.delete_sheet(sheet['name'])
                        print(f"      ✅ Удален лист: {sheet['name']} ({sheet['cells']:,} ячеек)")
                        total_cells_used -= sheet['cells']
                    except Exception as e:
                        print(f"      ⚠️ Не удалось удалить лист {sheet['name']}: {e}")
            
            print(f"   📊 Ячеек после очистки: {total_cells_used:,} / 10,000,000")
            
            # Синхронизируем каждую таблицу
            client_synced = 0
            for table_name in client_data['tables']:
                print(f"   📋 Синхронизация таблицы '{table_name}'...")
                
                # Преобразуем имя таблицы
                config_table_name = convert_table_name(table_name)
                
                # Получаем конфигурацию таблицы
                table_config = config_loader.get_table_config(config_table_name)
                if not table_config:
                    print(f"      ❌ Конфигурация для '{config_table_name}' не найдена")
                    continue
                
                try:
                    # Определяем количество записей для синхронизации
                    total_records = table_sizes.get(config_table_name, 0)
                    if total_records == 0:
                        print(f"      📝 Таблица {config_table_name} пуста")
                        continue
                    
                    records_to_sync = min(total_records, MAX_RECORDS_PER_TABLE)
                    print(f"      📊 Будет синхронизировано {records_to_sync:,} из {total_records:,} записей")
                    
                    # Создаем лист
                    if not await client_sheets.sheet_exists(table_name):
                        await client_sheets.create_sheet(table_name)
                        print(f"      ✅ Создан лист: {table_name}")
                    
                    # Получаем данные из MySQL (последние записи)
                    mysql_data = await get_limited_data(mysql_client, config_table_name, table_config, records_to_sync)
                    
                    if mysql_data:
                        # Записываем данные
                        await client_sheets.append_sheet_data(table_name, mysql_data)
                        synced_count = len(mysql_data)
                        client_synced += synced_count
                        total_synced += synced_count
                        print(f"      ✅ Синхронизировано {synced_count:,} записей")
                        
                        # Обновляем статус в tech
                        await update_tech_status(admin_sheets_client, client_id, table_name, True)
                    else:
                        print(f"      ⚠️ Нет данных для синхронизации")
                        
                except Exception as e:
                    print(f"      ❌ Ошибка синхронизации {table_name}: {e}")
            
            print(f"   ✅ Клиент {client_id}: синхронизировано {client_synced:,} записей")
        
        print(f"\n🎉 Очистка и синхронизация завершена!")
        print(f"📊 Всего синхронизировано записей: {total_synced:,}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await mysql_client.close_pool()

async def get_limited_data(mysql_client, table_name, table_config, max_records):
    """Получает ограниченное количество данных из MySQL (самые новые записи)"""
    try:
        # Получаем поля для выборки
        fields = list(table_config.fields.keys())
        primary_key = table_config.primary_key
        
        # Строим запрос для получения последних записей
        query = f"""
        SELECT {', '.join(fields)}
        FROM `{table_name}`
        ORDER BY `{primary_key}` DESC
        LIMIT {max_records}
        """
        
        print(f"         🔍 Получение последних {max_records:,} записей...")
        rows = await mysql_client.fetch_all(query)
        
        if not rows:
            return []
        
        # Разворачиваем порядок (старые записи сверху)
        rows = list(reversed(rows))
        
        # Конвертируем в формат Google Sheets
        sheet_data = []
        
        # Добавляем заголовки
        headers = [table_config.fields.get(field, field) for field in fields]
        sheet_data.append(headers)
        
        # Добавляем данные
        for row in rows:
            sheet_row = []
            for field in fields:
                value = row.get(field, '')
                if value is None:
                    value = ''
                sheet_row.append(str(value))
            sheet_data.append(sheet_row)
        
        print(f"         📊 Подготовлено {len(sheet_data):,} строк (включая заголовки)")
        return sheet_data
        
    except Exception as e:
        print(f"         ❌ Ошибка получения данных из {table_name}: {e}")
        return []

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
    asyncio.run(cleanup_and_sync())
