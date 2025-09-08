#!/usr/bin/env python3
"""
Синхронизация больших таблиц с разбиением на части
"""

import asyncio
import sys
import os
import math

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sync_service.clients.mysql_client import MySQLClient
from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader

# Максимальное количество строк на лист (500K строк = ~10 млн ячеек при 20 колонках)
MAX_ROWS_PER_SHEET = 500000

async def sync_large_tables():
    """Синхронизирует большие таблицы с разбиением на части"""
    
    print("📊 Синхронизация больших таблиц с разбиением на части...")
    print("=" * 70)
    
    # Загружаем конфигурацию
    config_loader = ConfigLoader()
    config = config_loader.load_env_config()
    
    # Подключаемся к управляющей таблице
    admin_sheets_client = GoogleSheetsClient(
        credentials_file=config['google_sheets'].credentials_file,
        spreadsheet_id=config['google_sheets'].spreadsheet_id
    )
    await admin_sheets_client.authenticate()
    
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
        
        # Большие таблицы, которые нужно разбить
        large_tables_info = {
            'detaliz_wb': 'wb_detaliz',
            'detaliz': 'ozon_detaliz', 
            'prices': 'ozon_price',
            'voronka': 'ozon_voronka',
            'zakaz_fbo': 'ozon_zakfbo',
            'report_fbo': 'ozon_prodfbo'
        }
        
        # Обрабатываем каждую строку (кроме заголовка)
        clients = {}
        for row in tech_data[1:]:
            if len(row) >= 7:
                client_id = row[0]
                table_id = row[1]
                table_name = row[2]
                
                if client_id and table_id and table_name in large_tables_info.values():
                    if client_id not in clients:
                        clients[client_id] = {
                            'table_id': table_id,
                            'large_tables': []
                        }
                    
                    # Находим MySQL имя таблицы
                    mysql_table = None
                    for mysql_name, sheet_name in large_tables_info.items():
                        if sheet_name == table_name:
                            mysql_table = mysql_name
                            break
                    
                    if mysql_table:
                        clients[client_id]['large_tables'].append({
                            'mysql_name': mysql_table,
                            'sheet_name': table_name
                        })
        
        print(f"\n👥 Найдено клиентов с большими таблицами: {len(clients)}")
        
        # Синхронизируем большие таблицы для каждого клиента
        total_synced = 0
        for client_id, client_data in clients.items():
            if not client_data['large_tables']:
                continue
                
            print(f"\n🔄 Синхронизация больших таблиц клиента {client_id}...")
            
            # Создаем клиент для Google Sheets клиента
            client_sheets = GoogleSheetsClient(
                credentials_file=config['google_sheets'].credentials_file,
                spreadsheet_id=client_data['table_id']
            )
            await client_sheets.authenticate()
            
            # Синхронизируем каждую большую таблицу
            for table_info in client_data['large_tables']:
                mysql_table = table_info['mysql_name'] 
                sheet_name = table_info['sheet_name']
                
                print(f"   📋 Синхронизация большой таблицы '{mysql_table}' → '{sheet_name}'...")
                
                try:
                    # Получаем общее количество записей
                    total_count = await mysql_client.get_table_count(mysql_table)
                    print(f"      📊 Всего записей: {total_count:,}")
                    
                    if total_count == 0:
                        print(f"      ⏭️  Таблица {mysql_table} пуста, пропускаем")
                        continue
                    
                    # Рассчитываем количество частей
                    parts_needed = math.ceil(total_count / MAX_ROWS_PER_SHEET)
                    print(f"      📄 Нужно создать {parts_needed} частей")
                    
                    # Получаем конфигурацию таблицы
                    table_mapping = config_loader.load_table_mapping()
                    table_config = table_mapping['tables'].get(mysql_table)
                    
                    if not table_config:
                        print(f"      ❌ Конфигурация для '{mysql_table}' не найдена")
                        continue
                    
                    # Синхронизируем каждую часть
                    synced_count = 0
                    for part_num in range(1, parts_needed + 1):
                        offset = (part_num - 1) * MAX_ROWS_PER_SHEET
                        limit = min(MAX_ROWS_PER_SHEET, total_count - offset)
                        
                        part_sheet_name = f"{sheet_name}_part{part_num}"
                        print(f"      🔄 Часть {part_num}/{parts_needed}: {part_sheet_name} ({limit:,} записей)")
                        
                        # Создаем лист для части (если не существует)
                        if not await client_sheets.sheet_exists(part_sheet_name):
                            try:
                                await client_sheets.create_sheet(part_sheet_name)
                                print(f"         ✅ Создан лист: {part_sheet_name}")
                            except Exception as e:
                                print(f"         ❌ Ошибка создания листа: {e}")
                                continue
                        
                        # Получаем данные из MySQL
                        mysql_data = await get_table_data_with_pagination(
                            mysql_client, mysql_table, table_config, offset, limit
                        )
                        
                        if mysql_data:
                            # Очищаем лист перед записью
                            try:
                                await client_sheets.clear_sheet_data(part_sheet_name)
                            except:
                                pass  # Лист может быть пустым
                            
                            # Записываем данные
                            try:
                                await client_sheets.append_sheet_data(part_sheet_name, mysql_data)
                                synced_count += len(mysql_data)
                                print(f"         ✅ Синхронизировано {len(mysql_data):,} записей")
                            except Exception as e:
                                print(f"         ❌ Ошибка записи в лист: {e}")
                        else:
                            print(f"         ⚠️ Нет данных для синхронизации")
                    
                    total_synced += synced_count
                    print(f"      ✅ Всего синхронизировано для {mysql_table}: {synced_count:,} записей")
                    
                except Exception as e:
                    print(f"      ❌ Ошибка синхронизации {mysql_table}: {e}")
        
        print(f"\n🎉 Синхронизация больших таблиц завершена!")
        print(f"📊 Всего синхронизировано записей: {total_synced:,}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await mysql_client.close_pool()

async def get_table_data_with_pagination(mysql_client, table_name, table_config, offset, limit):
    """Получает данные из MySQL с пагинацией"""
    try:
        # Получаем поля для выборки
        fields = list(table_config.get('fields', {}).keys())
        if not fields:
            fields = ['*']
        
        query = f"""
        SELECT {', '.join(fields)}
        FROM `{table_name}`
        ORDER BY {table_config.get('primary_key', 'id')}
        LIMIT {limit} OFFSET {offset}
        """
        
        rows = await mysql_client.fetch_all(query)
        
        if not rows:
            return []
        
        # Конвертируем в формат для Google Sheets
        sheet_data = []
        
        # Добавляем заголовки (только для первой части)
        if offset == 0:
            headers = [table_config['fields'].get(field, field) for field in fields]
            sheet_data.append(headers)
        
        # Добавляем данные
        for row in rows:
            sheet_row = []
            for field in fields:
                value = row.get(field, '')
                # Преобразуем None в пустую строку
                if value is None:
                    value = ''
                # Преобразуем в строку для Google Sheets
                sheet_row.append(str(value))
            sheet_data.append(sheet_row)
        
        return sheet_data
        
    except Exception as e:
        print(f"❌ Ошибка получения данных из {table_name}: {e}")
        return []

if __name__ == "__main__":
    asyncio.run(sync_large_tables())
