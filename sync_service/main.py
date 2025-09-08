"""
Основной сервис синхронизации MySQL <-> Google Sheets
FastAPI приложение с фоновыми задачами
"""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from loguru import logger
import uvicorn

from .clients.mysql_client import MySQLClient
from .clients.sheets_client import GoogleSheetsClient
from .sync.mysql_to_sheets import MySQLToSheetsSync
from .sync.sheets_to_mysql import SheetsToMySQLSync
from .sync.change_processor import ChangeProcessor
from .config.config_loader import ConfigLoader


# Глобальные клиенты
mysql_client: Optional[MySQLClient] = None
sheets_client: Optional[GoogleSheetsClient] = None
mysql_to_sheets_sync: Optional[MySQLToSheetsSync] = None
sheets_to_mysql_sync: Optional[SheetsToMySQLSync] = None
change_processor: Optional[ChangeProcessor] = None
config_loader: Optional[ConfigLoader] = None

# Задача обработки изменений
change_processor_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    await startup_event()
    yield
    # Shutdown
    await shutdown_event()


# Создаем FastAPI приложение
app = FastAPI(
    title="MySQL-Google Sheets Sync Service",
    description="Сервис двусторонней синхронизации между MySQL и Google Sheets",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic модели для API
class SyncRequest(BaseModel):
    tables: Optional[List[str]] = None
    force_full_sync: bool = False
    direction: str = "both"  # both, mysql_to_sheets, sheets_to_mysql


class TableSyncRequest(BaseModel):
    table_name: str
    force_full_sync: bool = False
    direction: str = "both"


class SyncResponse(BaseModel):
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None


async def startup_event():
    """Инициализация при запуске"""
    global mysql_client, sheets_client, mysql_to_sheets_sync, sheets_to_mysql_sync
    global change_processor, config_loader, change_processor_task
    
    try:
        logger.info("Starting MySQL-Google Sheets Sync Service...")
        
        # Инициализируем загрузчик конфигурации
        config_loader = ConfigLoader()
        
        # Проверяем конфигурацию
        if not config_loader.validate_config():
            raise RuntimeError("Configuration validation failed")
        
        # Загружаем конфигурации
        env_config = config_loader.load_env_config()
        sync_config = config_loader.get_sync_settings()
        
        # Инициализируем MySQL клиент
        mysql_config = env_config['mysql']
        mysql_client = MySQLClient(
            host=mysql_config.host,
            user=mysql_config.user,
            password=mysql_config.password,
            database=mysql_config.database,
            port=mysql_config.port,
            charset=mysql_config.charset
        )
        await mysql_client.create_pool()
        
        # Инициализируем Google Sheets клиент
        sheets_config = env_config['google_sheets']
        sheets_client = GoogleSheetsClient(
            credentials_file=sheets_config.credentials_file,
            spreadsheet_id=sheets_config.spreadsheet_id
        )
        await sheets_client.authenticate()
        
        # Инициализируем синхронизаторы
        mysql_to_sheets_sync = MySQLToSheetsSync(mysql_client, sheets_client, sync_config)
        sheets_to_mysql_sync = SheetsToMySQLSync(mysql_client, sheets_client, sync_config)
        
        # Инициализируем обработчик изменений
        change_processor = ChangeProcessor(mysql_client, sheets_client, sync_config, config_loader)
        
        # Запускаем фоновую обработку изменений
        change_processor_task = asyncio.create_task(
            change_processor.start_processing(polling_interval=30)
        )
        
        # Запускаем автоматическую синхронизацию при старте
        logger.info("Starting automatic initial sync...")
        asyncio.create_task(perform_initial_sync())
        
        logger.info("Service started successfully")
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise


async def shutdown_event():
    """Очистка при завершении"""
    global mysql_client, change_processor, change_processor_task
    
    try:
        logger.info("Shutting down service...")
        
        # Останавливаем обработчик изменений
        if change_processor:
            change_processor.stop_processing()
        
        if change_processor_task:
            change_processor_task.cancel()
            try:
                await change_processor_task
            except asyncio.CancelledError:
                pass
        
        # Закрываем MySQL соединения
        if mysql_client:
            await mysql_client.close_pool()
        
        logger.info("Service shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


# Dependency для проверки инициализации сервисов
async def get_services():
    """Dependency для получения инициализированных сервисов"""
    if not all([mysql_client, sheets_client, mysql_to_sheets_sync, 
               sheets_to_mysql_sync, change_processor, config_loader]):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not properly initialized"
        )
    
    return {
        "mysql_client": mysql_client,
        "sheets_client": sheets_client,
        "mysql_to_sheets_sync": mysql_to_sheets_sync,
        "sheets_to_mysql_sync": sheets_to_mysql_sync,
        "change_processor": change_processor,
        "config_loader": config_loader
    }


@app.get("/", response_model=Dict[str, str])
async def root():
    """Корневой endpoint"""
    return {
        "service": "MySQL-Google Sheets Sync Service",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/health", response_model=Dict[str, Any])
async def health_check(services: dict = Depends(get_services)):
    """Проверка состояния сервиса"""
    try:
        # Проверяем MySQL подключение
        mysql_ok = await services["mysql_client"].fetch_one("SELECT 1 as test")
        mysql_status = "ok" if mysql_ok else "error"
        
        # Проверяем Google Sheets доступность
        try:
            await services["sheets_client"].get_sheet_info()
            sheets_status = "ok"
        except:
            sheets_status = "error"
        
        # Статистика change_log
        stats = await services["change_processor"].get_change_log_stats()
        
        return {
            "status": "healthy" if mysql_status == "ok" and sheets_status == "ok" else "unhealthy",
            "mysql": mysql_status,
            "google_sheets": sheets_status,
            "change_processor": "running" if change_processor.is_running else "stopped",
            "change_log_stats": stats
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


@app.get("/config/tables", response_model=Dict[str, Any])
async def get_tables_config(services: dict = Depends(get_services)):
    """Получение конфигурации таблиц"""
    try:
        enabled_tables = services["config_loader"].get_enabled_tables()
        return {
            "enabled_tables": {name: {
                "sheet_name": config.sheet_name,
                "sync_direction": config.sync_direction,
                "batch_size": config.batch_size,
                "fields_count": len(config.fields)
            } for name, config in enabled_tables.items()},
            "total_tables": len(enabled_tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/mysql-to-sheets", response_model=SyncResponse)
async def sync_mysql_to_sheets(
    request: SyncRequest,
    background_tasks: BackgroundTasks,
    services: dict = Depends(get_services)
):
    """Запуск синхронизации MySQL -> Google Sheets"""
    try:
        # Получаем конфигурации таблиц
        if request.tables:
            table_configs = {}
            for table_name in request.tables:
                config = services["config_loader"].get_table_config(table_name)
                if config:
                    table_configs[table_name] = config
        else:
            table_configs = services["config_loader"].get_enabled_tables()
        
        if not table_configs:
            raise HTTPException(status_code=400, detail="No tables configured for sync")
        
        # Запускаем синхронизацию в фоне
        background_tasks.add_task(
            sync_mysql_to_sheets_task,
            services["mysql_to_sheets_sync"],
            table_configs,
            request.force_full_sync
        )
        
        return SyncResponse(
            status="started",
            message=f"MySQL -> Sheets sync started for {len(table_configs)} tables"
        )
        
    except Exception as e:
        logger.error(f"Error starting MySQL -> Sheets sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/sheets-to-mysql", response_model=SyncResponse)
async def sync_sheets_to_mysql(
    request: SyncRequest,
    background_tasks: BackgroundTasks,
    services: dict = Depends(get_services)
):
    """Запуск синхронизации Google Sheets -> MySQL"""
    try:
        # Получаем конфигурации таблиц
        if request.tables:
            table_configs = {}
            for table_name in request.tables:
                config = services["config_loader"].get_table_config(table_name)
                if config and config.sync_direction in ['both', 'sheets_to_mysql']:
                    table_configs[table_name] = config
        else:
            enabled_tables = services["config_loader"].get_enabled_tables()
            table_configs = {
                name: config for name, config in enabled_tables.items()
                if config.sync_direction in ['both', 'sheets_to_mysql']
            }
        
        if not table_configs:
            raise HTTPException(status_code=400, detail="No tables configured for Sheets -> MySQL sync")
        
        # Запускаем синхронизацию в фоне
        background_tasks.add_task(
            sync_sheets_to_mysql_task,
            services["sheets_to_mysql_sync"],
            table_configs
        )
        
        return SyncResponse(
            status="started",
            message=f"Sheets -> MySQL sync started for {len(table_configs)} tables"
        )
        
    except Exception as e:
        logger.error(f"Error starting Sheets -> MySQL sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/bidirectional", response_model=SyncResponse)
async def sync_bidirectional(
    request: SyncRequest,
    background_tasks: BackgroundTasks,
    services: dict = Depends(get_services)
):
    """Запуск двусторонней синхронизации"""
    try:
        # Получаем конфигурации таблиц
        if request.tables:
            table_configs = {}
            for table_name in request.tables:
                config = services["config_loader"].get_table_config(table_name)
                if config:
                    table_configs[table_name] = config
        else:
            table_configs = services["config_loader"].get_enabled_tables()
        
        if not table_configs:
            raise HTTPException(status_code=400, detail="No tables configured for sync")
        
        # Запускаем обе синхронизации в фоне
        background_tasks.add_task(
            sync_bidirectional_task,
            services["mysql_to_sheets_sync"],
            services["sheets_to_mysql_sync"],
            table_configs,
            request.force_full_sync
        )
        
        return SyncResponse(
            status="started",
            message=f"Bidirectional sync started for {len(table_configs)} tables"
        )
        
    except Exception as e:
        logger.error(f"Error starting bidirectional sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sync/status", response_model=Dict[str, Any])
async def get_sync_status(services: dict = Depends(get_services)):
    """Получение статуса синхронизации"""
    try:
        stats = await services["change_processor"].get_change_log_stats()
        
        return {
            "change_processor_running": services["change_processor"].is_running,
            "change_log_stats": stats,
            "timestamp": stats.get("timestamp")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/change-log/process", response_model=SyncResponse)
async def process_change_log(services: dict = Depends(get_services)):
    """Ручная обработка очереди изменений"""
    try:
        result = await services["change_processor"].process_pending_changes()
        
        return SyncResponse(
            status="completed",
            message=f"Processed {result.get('processed', 0)} changes, {result.get('errors', 0)} errors",
            data=result
        )
        
    except Exception as e:
        logger.error(f"Error processing change log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/change-log/cleanup", response_model=SyncResponse)
async def cleanup_change_log(
    retention_days: int = 30,
    services: dict = Depends(get_services)
):
    """Очистка старых записей change_log"""
    try:
        cleaned_count = await services["change_processor"].cleanup_old_records(retention_days)
        
        return SyncResponse(
            status="completed",
            message=f"Cleaned up {cleaned_count} old records",
            data={"cleaned_records": cleaned_count, "retention_days": retention_days}
        )
        
    except Exception as e:
        logger.error(f"Error cleaning up change log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/change-log/stats", response_model=Dict[str, Any])
async def get_change_log_stats(services: dict = Depends(get_services)):
    """Получение статистики change_log"""
    try:
        return await services["change_processor"].get_change_log_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Фоновые задачи
async def sync_mysql_to_sheets_task(sync_service, table_configs, force_full_sync):
    """Фоновая задача синхронизации MySQL -> Sheets"""
    try:
        logger.info("Starting MySQL -> Sheets sync task")
        result = await sync_service.sync_multiple_tables(table_configs, force_full_sync)
        logger.info(f"MySQL -> Sheets sync completed: {result}")
    except Exception as e:
        logger.error(f"MySQL -> Sheets sync task failed: {e}")


async def sync_sheets_to_mysql_task(sync_service, table_configs):
    """Фоновая задача синхронизации Sheets -> MySQL"""
    try:
        logger.info("Starting Sheets -> MySQL sync task")
        result = await sync_service.sync_multiple_tables(table_configs)
        logger.info(f"Sheets -> MySQL sync completed: {result}")
    except Exception as e:
        logger.error(f"Sheets -> MySQL sync task failed: {e}")


async def sync_bidirectional_task(mysql_to_sheets_sync, sheets_to_mysql_sync, 
                                 table_configs, force_full_sync):
    """Фоновая задача двусторонней синхронизации"""
    try:
        logger.info("Starting bidirectional sync task")
        
        # Сначала MySQL -> Sheets
        mysql_to_sheets_tables = {
            name: config for name, config in table_configs.items()
            if config.sync_direction in ['both', 'mysql_to_sheets']
        }
        
        if mysql_to_sheets_tables:
            result1 = await mysql_to_sheets_sync.sync_multiple_tables(
                mysql_to_sheets_tables, force_full_sync
            )
            logger.info(f"MySQL -> Sheets sync completed: {result1}")
        
        # Затем Sheets -> MySQL
        sheets_to_mysql_tables = {
            name: config for name, config in table_configs.items()
            if config.sync_direction in ['both', 'sheets_to_mysql']
        }
        
        if sheets_to_mysql_tables:
            result2 = await sheets_to_mysql_sync.sync_multiple_tables(sheets_to_mysql_tables)
            logger.info(f"Sheets -> MySQL sync completed: {result2}")
        
        logger.info("Bidirectional sync task completed")
        
    except Exception as e:
        logger.error(f"Bidirectional sync task failed: {e}")


def main():
    """Запуск сервиса"""
    # Настройка логирования
    logger.add("logs/sync_service.log", rotation="1 day", retention="30 days")
    
    # Получаем конфигурацию из переменных окружения
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    
    logger.info(f"Starting sync service on {host}:{port}")
    
    uvicorn.run(
        "sync_service.main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )


async def perform_initial_sync():
    """Выполняет начальную синхронизацию при запуске сервиса с умной очисткой"""
    await asyncio.sleep(5)  # Даем время для полной инициализации
    
    try:
        logger.info("🔄 Starting automatic initial sync with intelligent cleanup...")
        
        # Ограничиваем количество записей для предотвращения переполнения
        MAX_RECORDS_PER_TABLE = 25000  # Уменьшено для избежания лимитов ячеек
        
        # Читаем управляющую таблицу для получения клиентов
        admin_sheets_client = sheets_client
        tech_data = await admin_sheets_client.get_sheet_data('tech')
        
        if len(tech_data) < 2:
            logger.warning("Tech sheet is empty or has no data")
            return
        
        # Парсим клиентов
        clients = {}
        for row in tech_data[1:]:
            if len(row) >= 7:
                client_id = row[0]
                table_id = row[1]
                table_name = row[2]
                
                if client_id and table_id and table_name:
                    if client_id not in clients:
                        clients[client_id] = {
                            'table_id': table_id,
                            'tables': []
                        }
                    clients[client_id]['tables'].append(table_name)
        
        logger.info(f"Found {len(clients)} clients for automatic sync")
        
        # Синхронизируем каждого клиента
        for client_id, client_data in clients.items():
            try:
                logger.info(f"🔄 Auto-syncing client {client_id} ({len(client_data['tables'])} tables)")
                
                # Создаем клиент для Google Sheets клиента
                client_sheets = GoogleSheetsClient(
                    credentials_file=config_loader.load_env_config()['google_sheets'].credentials_file,
                    spreadsheet_id=client_data['table_id']
                )
                await client_sheets.authenticate()
                
                # Проверяем использование ячеек
                sheet_info = await client_sheets.get_sheet_info()
                total_cells_used = 0
                existing_sheets = []
                
                for sheet in sheet_info.get('sheets', []):
                    grid_props = sheet['properties']['gridProperties']
                    cells = grid_props.get('rowCount', 0) * grid_props.get('columnCount', 0)
                    total_cells_used += cells
                    existing_sheets.append(sheet['properties']['title'])
                
                logger.info(f"   📊 Client {client_id} using {total_cells_used:,} / 10,000,000 cells")
                
                # Если таблица переполнена - очищаем ВСЕ нужные листы сразу
                if total_cells_used > 5000000:  # 50% от лимита для безопасности
                    logger.info(f"   🧹 Cleaning sheets for client {client_id} (cells: {total_cells_used:,})")
                    
                    # Сначала удаляем все листы для синхронизации
                    sheets_to_delete = []
                    for table_name in client_data['tables']:
                        if table_name in existing_sheets:
                            sheets_to_delete.append(table_name)
                    
                    if sheets_to_delete:
                        logger.info(f"   📋 Deleting {len(sheets_to_delete)} sheets to free up space...")
                        for table_name in sheets_to_delete:
                            try:
                                await client_sheets.delete_sheet(table_name)
                                logger.info(f"      ✅ Deleted sheet: {table_name}")
                                await asyncio.sleep(1)  # Небольшая пауза между операциями
                            except Exception as e:
                                logger.warning(f"      ⚠️ Could not delete sheet {table_name}: {e}")
                                # Если не можем удалить лист, хотя бы очистим и уменьшим его
                                try:
                                    await client_sheets.clear_sheet_data(table_name)
                                    await client_sheets.resize_sheet(table_name, rows=10, cols=5)
                                    logger.info(f"      🔧 Cleared and resized sheet: {table_name}")
                                except:
                                    pass
                        
                        # Ждем немного после массового удаления
                        logger.info("   ⏳ Waiting for cleanup to complete...")
                        await asyncio.sleep(5)
                
                # Синхронизируем таблицы клиента
                synced_count = 0
                for table_name in client_data['tables']:
                    try:
                        # Преобразуем имя таблицы
                        mysql_table = convert_table_name_for_sync(table_name)
                        
                        # Получаем конфигурацию таблицы
                        table_config = config_loader.get_table_config(mysql_table)
                        if not table_config:
                            continue
                        
                        # Создаем лист если не существует
                        if not await client_sheets.sheet_exists(table_name):
                            await client_sheets.create_sheet(table_name)
                        
                        # Получаем ограниченные данные из MySQL
                        total_records = await mysql_client.get_table_count(mysql_table)
                        records_to_sync = min(total_records, MAX_RECORDS_PER_TABLE)
                        
                        if records_to_sync > 0:
                            # Получаем данные из MySQL
                            mysql_data = await get_limited_mysql_data(mysql_table, table_config, records_to_sync)
                            
                            if mysql_data:
                                # Используем улучшенный метод синхронизации с обработкой лимитов
                                try:
                                    result = await client_sheets.sync_table_to_sheets(
                                        table_data=mysql_data,
                                        sheet_name=table_name,
                                        field_mapping=table_config.fields,
                                        clear_existing=True,
                                        max_rows=records_to_sync
                                    )
                                    synced_records = result.get("synced", 0)
                                    synced_count += synced_records
                                    
                                    if "error" in result:
                                        logger.warning(f"      ⚠️ {table_name}: {result['error']}")
                                    else:
                                        logger.info(f"      ✅ Synced {synced_records:,} records to {table_name}")
                                        # Обновляем статус в tech таблице
                                        await update_tech_sync_status(
                                            client_id=client_id,
                                            table_name=table_name,
                                            exported_from_db=True
                                        )
                                        
                                except Exception as e:
                                    if "limit" in str(e).lower():
                                        logger.warning(f"      ⚠️ {table_name}: Cell limit exceeded, skipping")
                                    else:
                                        raise
                    
                    except Exception as e:
                        logger.error(f"      ❌ Error syncing {table_name}: {e}")
                
                logger.info(f"   ✅ Client {client_id}: synced {synced_count:,} total records")
                
                # Обновляем статус "Выгружено в базу" для всех таблиц
                try:
                    logger.info(f"   📝 Updating 'exported to DB' status for client {client_id}")
                    
                    for table_name in client_data['tables']:
                        try:
                            # Обновляем статус "Выгружено в базу" = TRUE для каждой таблицы
                            await update_tech_sync_status(
                                client_id=client_id,
                                table_name=table_name,
                                exported_to_db=True
                            )
                        except Exception as e:
                            logger.warning(f"      ⚠️ Error updating status for {table_name}: {e}")
                    
                    logger.info(f"   ✅ Client {client_id}: updated 'exported to DB' status for all tables")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error updating 'exported to DB' status for client {client_id}: {e}")
                
            except Exception as e:
                logger.error(f"❌ Error processing client {client_id}: {e}")
        
        logger.info("🎉 Automatic initial sync completed")
        
    except Exception as e:
        logger.error(f"❌ Automatic initial sync failed: {e}")


def convert_table_name_for_sync(sheet_name):
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


async def get_limited_mysql_data(table_name, table_config, max_records):
    """Получает ограниченное количество данных из MySQL"""
    try:
        fields = list(table_config.fields.keys())
        primary_key = table_config.primary_key
        
        query = f"""
        SELECT {', '.join(fields)}
        FROM `{table_name}`
        ORDER BY `{primary_key}` DESC
        LIMIT {max_records}
        """
        
        rows = await mysql_client.fetch_all(query)
        if not rows:
            return []
        
        # Разворачиваем порядок и конвертируем в формат Google Sheets
        rows = list(reversed(rows))
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
        
        return sheet_data
        
    except Exception as e:
        logger.error(f"Error getting limited data from {table_name}: {e}")
        return []


async def update_tech_sync_status(client_id: str, table_name: str, 
                                 exported_from_db: bool = None, 
                                 exported_to_db: bool = None):
    """Обновляет статус синхронизации в tech таблице"""
    try:
        # Получаем текущие данные из tech таблицы
        tech_data = await sheets_client.get_sheet_data('tech')
        
        if len(tech_data) < 2:
            logger.warning("Tech sheet is empty or has no data")
            return False
        
        # Находим строку для обновления
        row_to_update = None
        row_index = None
        
        for i, row in enumerate(tech_data[1:], start=2):  # Начинаем с 2 (пропускаем заголовок)
            if len(row) >= 3 and row[0] == client_id and row[2] == table_name:
                row_to_update = row
                row_index = i
                break
        
        if not row_to_update:
            logger.warning(f"No row found in tech table for client {client_id}, table {table_name}")
            return False
        
        # Создаем обновленную строку
        updated_row = list(row_to_update)
        
        # Дополняем строку до нужной длины если необходимо
        while len(updated_row) < 7:
            updated_row.append("")
        
        # Обновляем статусы
        if exported_to_db is not None:
            updated_row[5] = "TRUE" if exported_to_db else "FALSE"  # Выгружено в базу
        if exported_from_db is not None:
            updated_row[6] = "TRUE" if exported_from_db else "FALSE"  # Выгружено из базы
        
        # Обновляем строку в Google Sheets
        range_name = f"A{row_index}:G{row_index}"
        await sheets_client.update_sheet_data('tech', range_name, [updated_row])
        
        logger.info(f"✅ Updated tech status for {client_id}/{table_name}: "
                   f"to_db={exported_to_db}, from_db={exported_from_db}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating tech sync status: {e}")
        return False


if __name__ == "__main__":
    main()
