#!/usr/bin/env python3
"""
Синхронизация с фильтрацией - только последние 1 млн записей для больших таблиц
"""

import asyncio
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sync_service.clients.mysql_client import MySQLClient
from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader
from sync_service.sync.mysql_to_sheets import MySQLToSheetsSync

# Пороговые значения
LARGE_TABLE_THRESHOLD = 100000  # 100k записей
MAX_RECORDS_FOR_LARGE_TABLES = 1000000  # 1 млн записей для больших таблиц

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

async def sync_with_filtering():
    """Синхронизация данных с фильтрацией для больших таблиц"""
    
    print("🔄 Синхронизация данных с фильтрацией (1 млн записей для больших таблиц)")
    print("=" * 80)
    
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
        
        # Анализируем размеры таблиц
        print(f"\n📊 Анализ размеров таблиц:")
        table_sizes = {}
        table_mapping = config_loader.load_table_mapping()
        
        for table_name, table_config in table_mapping['tables'].items():
            if table_config.get('enabled', False) and table_config.get('sync_direction') == "mysql_to_sheets":
                try:
                    count = await mysql_client.get_table_count(table_name)
                    table_sizes[table_name] = count
                    
                    if count > LARGE_TABLE_THRESHOLD:
                        limited_count = min(count, MAX_RECORDS_FOR_LARGE_TABLES)
                        print(f"   🔴 {table_name}: {count:,} записей → будет синхронизировано {limited_count:,} (ФИЛЬТР)")
                    else:
                        print(f"   🟢 {table_name}: {count:,} записей → будет синхронизировано {count:,} (ВСЕ)")
                        
                except Exception as e:
                    print(f"   ❌ {table_name}: ошибка получения размера - {e}")
                    table_sizes[table_name] = 0
        
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
            
            # Создаем кастомный синхронизатор с фильтрацией
            sync = FilteredMySQLToSheetsSync(mysql_client, client_sheets, config['sync'])
            
            # Синхронизируем каждую таблицу клиента
            for table_info in client_data['tables']:
                table_name = table_info['name']
                print(f"   📋 Синхронизация таблицы '{table_name}'...")
                
                # Преобразуем имя таблицы
                config_table_name = convert_table_name(table_name)
                
                # Получаем конфигурацию таблицы
                table_config = config_loader.get_table_config(config_table_name)
                if not table_config:
                    print(f"      ❌ Конфигурация для '{config_table_name}' (из '{table_name}') не найдена")
                    continue
                
                try:
                    # Определяем нужна ли фильтрация
                    total_records = table_sizes.get(config_table_name, 0)
                    use_filtering = total_records > LARGE_TABLE_THRESHOLD
                    
                    if use_filtering:
                        max_records = MAX_RECORDS_FOR_LARGE_TABLES
                        print(f"      🔍 Применяется фильтр: последние {max_records:,} из {total_records:,} записей")
                    else:
                        max_records = None
                        print(f"      📝 Синхронизируются все {total_records:,} записей")
                    
                    # Синхронизируем таблицу с фильтрацией
                    result = await sync.sync_table_with_filter(
                        config_table_name, 
                        table_config, 
                        max_records=max_records
                    )
                    
                    if result.get('status') == 'completed':
                        synced_count = result.get('synced', 0)
                        total_synced += synced_count
                        print(f"      ✅ Синхронизировано {synced_count:,} записей")
                        
                        # Обновляем статус в листе "tech"
                        await update_tech_status(admin_sheets_client, client_id, table_name, True)
                    else:
                        print(f"      ❌ Ошибка синхронизации: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    print(f"      ❌ Ошибка: {e}")
        
        print(f"\n🎉 Синхронизация завершена!")
        print(f"📊 Всего синхронизировано записей: {total_synced:,}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await mysql_client.close_pool()

class FilteredMySQLToSheetsSync(MySQLToSheetsSync):
    """Синхронизатор с поддержкой фильтрации данных"""
    
    async def sync_table_with_filter(self, table_name: str, table_config, max_records=None):
        """Синхронизация таблицы с ограничением количества записей"""
        try:
            print(f"      🔄 Начало синхронизации {table_name}...")
            
            # Проверяем направление синхронизации
            if table_config.sync_direction not in ['both', 'mysql_to_sheets']:
                return {"status": "skipped", "reason": "sync_direction"}
            
            # Получаем общее количество записей
            total_count = await self.mysql_client.get_table_count(table_name)
            if total_count == 0:
                print(f"      📝 Таблица {table_name} пуста")
                await self._clear_sheet_if_needed(table_config.sheet_name)
                return {"status": "completed", "synced": 0, "total": 0}
            
            # Определяем количество записей для синхронизации
            if max_records and total_count > max_records:
                records_to_sync = max_records
                print(f"      🔍 Фильтр применен: {records_to_sync:,} из {total_count:,} записей")
            else:
                records_to_sync = total_count
                print(f"      📊 Синхронизируются все {records_to_sync:,} записей")
            
            # Очищаем лист перед синхронизацией
            try:
                await self.sheets_client.clear_sheet_data(table_config.sheet_name)
                print(f"      🧹 Лист {table_config.sheet_name} очищен")
            except:
                pass
            
            # Получаем данные с фильтрацией (самые новые записи)
            mysql_data = await self._get_filtered_data(table_name, table_config, records_to_sync)
            
            if not mysql_data:
                print(f"      ⚠️ Нет данных для синхронизации")
                return {"status": "completed", "synced": 0, "total": total_count}
            
            # Конвертируем данные в формат Google Sheets
            sheet_data = self._convert_to_sheet_format(mysql_data, table_config)
            
            # Записываем данные в Google Sheets батчами
            batch_size = min(table_config.batch_size, 1000)  # Ограничиваем размер батча
            synced_records = 0
            
            for i in range(0, len(sheet_data), batch_size):
                batch = sheet_data[i:i + batch_size]
                try:
                    await self.sheets_client.append_sheet_data(table_config.sheet_name, batch)
                    synced_records += len(batch)
                    print(f"      📝 Записано {synced_records:,}/{len(sheet_data):,} записей")
                except Exception as e:
                    print(f"      ❌ Ошибка записи батча: {e}")
                    break
            
            print(f"      ✅ Синхронизация {table_name} завершена: {synced_records:,} записей")
            return {"status": "completed", "synced": synced_records, "total": total_count}
            
        except Exception as e:
            print(f"      ❌ Ошибка синхронизации {table_name}: {e}")
            return {"status": "error", "error": str(e)}
    
    async def _get_filtered_data(self, table_name: str, table_config, max_records: int):
        """Получает отфильтрованные данные из MySQL (самые новые записи)"""
        try:
            # Получаем поля для выборки
            fields = list(table_config.fields.keys())
            primary_key = table_config.primary_key
            
            # Строим запрос для получения последних записей
            # Сортируем по первичному ключу в убывающем порядке (самые новые сверху)
            query = f"""
            SELECT {', '.join(fields)}
            FROM `{table_name}`
            ORDER BY `{primary_key}` DESC
            LIMIT {max_records}
            """
            
            print(f"      🔍 Выполняется запрос: получение последних {max_records:,} записей...")
            rows = await self.mysql_client.fetch_all(query)
            
            if rows:
                print(f"      📊 Получено {len(rows):,} записей из MySQL")
                # Разворачиваем порядок, чтобы старые записи были сверху в Google Sheets
                return list(reversed(rows))
            else:
                print(f"      ⚠️ Нет данных в таблице {table_name}")
                return []
                
        except Exception as e:
            print(f"      ❌ Ошибка получения данных из {table_name}: {e}")
            return []
    
    def _convert_to_sheet_format(self, mysql_data, table_config):
        """Конвертирует данные MySQL в формат Google Sheets"""
        if not mysql_data:
            return []
        
        sheet_data = []
        
        # Добавляем заголовки
        headers = [table_config.fields.get(field, field) for field in table_config.fields.keys()]
        sheet_data.append(headers)
        
        # Добавляем данные
        for row in mysql_data:
            sheet_row = []
            for field in table_config.fields.keys():
                value = row.get(field, '')
                # Преобразуем None в пустую строку
                if value is None:
                    value = ''
                # Преобразуем в строку для Google Sheets
                sheet_row.append(str(value))
            sheet_data.append(sheet_row)
        
        return sheet_data

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
    asyncio.run(sync_with_filtering())
