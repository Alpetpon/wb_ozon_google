#!/usr/bin/env python3
"""
Создание листов в таблицах клиентов
"""

import asyncio
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sync_service.clients.sheets_client import GoogleSheetsClient
from sync_service.config.config_loader import ConfigLoader

async def create_client_sheets():
    """Создает листы в таблицах всех клиентов"""
    
    print("📊 Создание листов в таблицах клиентов...")
    print("=" * 60)
    
    # Загружаем конфигурацию
    config_loader = ConfigLoader()
    config = config_loader.load_env_config()
    
    # Подключаемся к управляющей таблице
    admin_sheets_client = GoogleSheetsClient(
        credentials_file=config['google_sheets'].credentials_file,
        spreadsheet_id=config['google_sheets'].spreadsheet_id
    )
    await admin_sheets_client.authenticate()
    
    print("✅ Подключение к управляющей таблице установлено")
    
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
                        'tables': set()  # Используем set для уникальности
                    }
                clients[client_id]['tables'].add(table_name)
    
    print(f"\n👥 Найдено клиентов: {len(clients)}")
    for client_id, client_data in clients.items():
        print(f"   📊 Клиент {client_id}: {len(client_data['tables'])} таблиц → {client_data['table_id']}")
    
    # Создаем листы для каждого клиента
    total_created = 0
    for client_id, client_data in clients.items():
        print(f"\n🔄 Создание листов для клиента {client_id}...")
        
        # Создаем клиент для Google Sheets клиента
        client_sheets = GoogleSheetsClient(
            credentials_file=config['google_sheets'].credentials_file,
            spreadsheet_id=client_data['table_id']
        )
        await client_sheets.authenticate()
        
        # Получаем информацию о существующих листах
        try:
            sheet_info = await client_sheets.get_sheet_info()
            existing_sheets = [sheet['properties']['title'] for sheet in sheet_info.get('sheets', [])]
            print(f"   📄 Существующие листы: {', '.join(existing_sheets)}")
        except Exception as e:
            print(f"   ⚠️ Не удалось получить информацию о листах: {e}")
            existing_sheets = []
        
        # Создаем недостающие листы
        created_count = 0
        for table_name in client_data['tables']:
            if table_name in existing_sheets:
                print(f"   ⏭️  {table_name} - уже существует")
            else:
                try:
                    await client_sheets.create_sheet(table_name)
                    print(f"   ✅ {table_name} - создан")
                    created_count += 1
                    total_created += 1
                except Exception as e:
                    print(f"   ❌ {table_name} - ошибка создания: {e}")
        
        print(f"   📊 Создано {created_count} новых листов для клиента {client_id}")
    
    print(f"\n🎉 Создание листов завершено!")
    print(f"📊 Всего создано {total_created} новых листов")
    print("📝 Теперь можно запускать синхронизацию данных")

if __name__ == "__main__":
    asyncio.run(create_client_sheets())
