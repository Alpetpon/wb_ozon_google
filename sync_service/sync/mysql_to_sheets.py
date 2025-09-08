"""
Модуль синхронизации данных MySQL -> Google Sheets
Обеспечивает постраничную выгрузку данных с контролем батчей
"""

import asyncio
from typing import Dict, List, Any, Optional
from loguru import logger
import math
from datetime import datetime

from ..clients.mysql_client import MySQLClient
from ..clients.sheets_client import GoogleSheetsClient
from ..config.config_loader import TableConfig, SyncConfig


class MySQLToSheetsSync:
    """Синхронизатор MySQL -> Google Sheets"""
    
    def __init__(self, mysql_client: MySQLClient, sheets_client: GoogleSheetsClient,
                 sync_config: SyncConfig):
        self.mysql_client = mysql_client
        self.sheets_client = sheets_client
        self.sync_config = sync_config
        
    async def sync_table(self, table_name: str, table_config: TableConfig,
                        force_full_sync: bool = False) -> Dict[str, Any]:
        """Синхронизация одной таблицы"""
        try:
            logger.info(f"Starting MySQL -> Sheets sync for table: {table_name}")
            start_time = datetime.now()
            
            # Проверяем направление синхронизации
            if table_config.sync_direction not in ['both', 'mysql_to_sheets']:
                logger.info(f"Skipping table {table_name}: sync direction is {table_config.sync_direction}")
                return {"status": "skipped", "reason": "sync_direction"}
            
            # Получаем общее количество записей
            total_count = await self.mysql_client.get_table_count(table_name)
            if total_count == 0:
                logger.info(f"Table {table_name} is empty")
                await self._clear_sheet_if_needed(table_config.sheet_name)
                return {"status": "completed", "synced": 0, "total": 0}
            
            logger.info(f"Table {table_name} has {total_count} records")
            
            # Рассчитываем количество батчей
            batch_size = min(table_config.batch_size, self.sync_config.max_batch_size)
            total_batches = math.ceil(total_count / batch_size)
            
            # Синхронизируем по батчам
            synced_count = 0
            first_batch = True
            
            for batch_num in range(total_batches):
                offset = batch_num * batch_size
                
                logger.info(f"Processing batch {batch_num + 1}/{total_batches} "
                           f"(offset: {offset}, limit: {batch_size})")
                
                # Получаем данные из MySQL
                mysql_data = await self.mysql_client.get_table_data(
                    table_name=table_name,
                    limit=batch_size,
                    offset=offset,
                    order_by=table_config.primary_key
                )
                
                if not mysql_data:
                    logger.warning(f"No data in batch {batch_num + 1}")
                    continue
                
                # Синхронизируем батч
                batch_result = await self._sync_batch_to_sheets(
                    mysql_data=mysql_data,
                    table_config=table_config,
                    clear_existing=(first_batch and force_full_sync),
                    append_mode=(not first_batch or not force_full_sync)
                )
                
                synced_count += batch_result.get("synced", 0)
                first_batch = False
                
                # Небольшая пауза между батчами для контроля нагрузки
                if batch_num < total_batches - 1:
                    await asyncio.sleep(1)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "completed",
                "table": table_name,
                "synced": synced_count,
                "total": total_count,
                "batches": total_batches,
                "duration_seconds": duration,
                "avg_speed": round(synced_count / duration, 2) if duration > 0 else 0
            }
            
            logger.info(f"Completed sync for {table_name}: {synced_count}/{total_count} records "
                       f"in {duration:.2f}s")
            
            # Если есть доступ к update_tech_sync_status, обновляем статус
            try:
                # Импортируем функцию динамически для избежания циклических импортов
                from sync_service.main import update_tech_sync_status, sheets_client
                
                # Ищем client_id в tech таблице по table_name  
                tech_data = await sheets_client.get_sheet_data('tech')
                for row in tech_data[1:]:
                    if len(row) >= 3 and row[2] == table_name:
                        client_id = row[0]
                        await update_tech_sync_status(
                            client_id=client_id,
                            table_name=table_name,
                            exported_from_db=True
                        )
                        break
            except Exception as e:
                # Не критично, если не можем обновить статус
                pass
            
            return result
            
        except Exception as e:
            logger.error(f"Error syncing table {table_name}: {e}")
            return {
                "status": "error",
                "table": table_name,
                "error": str(e)
            }
    
    async def _sync_batch_to_sheets(self, mysql_data: List[Dict[str, Any]],
                                   table_config: TableConfig, clear_existing: bool = False,
                                   append_mode: bool = True) -> Dict[str, Any]:
        """Синхронизация одного батча данных с проверкой лимитов"""
        try:
            # Конвертируем данные в формат Google Sheets
            sheet_data = self.sheets_client.convert_to_sheet_format(
                mysql_data, table_config.fields, include_headers=(not append_mode)
            )
            
            if not sheet_data:
                return {"synced": 0}
            
            # Проверяем лимит ячеек перед записью
            if not await self.sheets_client.can_fit_data(sheet_data):
                logger.warning(f"Cannot fit batch data for {table_config.sheet_name} - cell limit exceeded")
                return {"synced": 0, "error": "Cell limit exceeded"}
            
            if clear_existing:
                logger.info(f"Clearing existing data in sheet: {table_config.sheet_name}")
                await self.sheets_client.clear_sheet_data(table_config.sheet_name)
            
            # Записываем данные с улучшенной обработкой ошибок
            try:
                if append_mode and not clear_existing:
                    await self.sheets_client.append_sheet_data(
                        table_config.sheet_name, sheet_data
                    )
                else:
                    await self.sheets_client.update_sheet_data(
                        table_config.sheet_name, "A1", sheet_data
                    )
                
                return {"synced": len(mysql_data)}
                
            except Exception as e:
                error_msg = str(e).lower()
                if "limit" in error_msg or "quota" in error_msg:
                    logger.warning(f"Hit limit while syncing {table_config.sheet_name}: {e}")
                    return {"synced": 0, "error": "API or cell limit exceeded"}
                raise
            
        except Exception as e:
            logger.error(f"Error syncing batch to sheets: {e}")
            raise
    
    async def _clear_sheet_if_needed(self, sheet_name: str):
        """Очистка листа если он существует"""
        try:
            if await self.sheets_client.sheet_exists(sheet_name):
                await self.sheets_client.clear_sheet_data(sheet_name)
                logger.info(f"Cleared empty sheet: {sheet_name}")
        except Exception as e:
            logger.warning(f"Could not clear sheet {sheet_name}: {e}")
    
    async def sync_multiple_tables(self, table_configs: Dict[str, TableConfig],
                                  force_full_sync: bool = False) -> Dict[str, Any]:
        """Синхронизация нескольких таблиц"""
        logger.info(f"Starting sync for {len(table_configs)} tables")
        start_time = datetime.now()
        
        results = {}
        total_synced = 0
        successful_tables = 0
        failed_tables = 0
        
        for table_name, table_config in table_configs.items():
            try:
                if not table_config.enabled:
                    logger.info(f"Skipping disabled table: {table_name}")
                    results[table_name] = {"status": "disabled"}
                    continue
                
                # Синхронизируем таблицу
                result = await self.sync_table(table_name, table_config, force_full_sync)
                results[table_name] = result
                
                if result["status"] == "completed":
                    total_synced += result.get("synced", 0)
                    successful_tables += 1
                else:
                    failed_tables += 1
                
            except Exception as e:
                logger.error(f"Failed to sync table {table_name}: {e}")
                results[table_name] = {"status": "error", "error": str(e)}
                failed_tables += 1
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        summary = {
            "status": "completed",
            "total_tables": len(table_configs),
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "total_synced_records": total_synced,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "results": results
        }
        
        logger.info(f"Sync completed: {successful_tables}/{len(table_configs)} tables, "
                   f"{total_synced} records in {duration:.2f}s")
        
        return summary
    
    async def sync_table_incremental(self, table_name: str, table_config: TableConfig,
                                   last_sync_time: datetime = None) -> Dict[str, Any]:
        """Инкрементальная синхронизация таблицы"""
        try:
            logger.info(f"Starting incremental sync for table: {table_name}")
            
            where_clause = None
            if last_sync_time:
                # Предполагаем, что в таблице есть поле updated_at или created_at
                timestamp_fields = ['updated_at', 'created_at', 'dat', 'dF']
                
                # Проверяем структуру таблицы
                table_structure = await self.mysql_client.get_table_structure(table_name)
                existing_fields = [field['Field'] for field in table_structure]
                
                timestamp_field = None
                for field in timestamp_fields:
                    if field in existing_fields:
                        timestamp_field = field
                        break
                
                if timestamp_field:
                    where_clause = f"`{timestamp_field}` >= '{last_sync_time.strftime('%Y-%m-%d %H:%M:%S')}'"
                    logger.info(f"Using incremental sync with field: {timestamp_field}")
                else:
                    logger.warning(f"No timestamp field found in {table_name}, falling back to full sync")
            
            # Получаем количество записей для синхронизации
            count = await self.mysql_client.get_table_count(table_name, where_clause)
            
            if count == 0:
                logger.info(f"No new records to sync for table: {table_name}")
                return {"status": "completed", "synced": 0, "total": 0}
            
            # Получаем данные
            mysql_data = await self.mysql_client.get_table_data(
                table_name=table_name,
                where_clause=where_clause,
                order_by=table_config.primary_key
            )
            
            if not mysql_data:
                return {"status": "completed", "synced": 0, "total": 0}
            
            # Синхронизируем данные (добавляем в конец листа)
            result = await self._sync_batch_to_sheets(
                mysql_data=mysql_data,
                table_config=table_config,
                clear_existing=False,
                append_mode=True
            )
            
            logger.info(f"Incremental sync completed for {table_name}: {result.get('synced', 0)} records")
            
            return {
                "status": "completed",
                "table": table_name,
                "synced": result.get("synced", 0),
                "total": count,
                "sync_type": "incremental"
            }
            
        except Exception as e:
            logger.error(f"Error in incremental sync for {table_name}: {e}")
            return {
                "status": "error",
                "table": table_name,
                "error": str(e)
            }
    
    async def validate_sync(self, table_name: str, table_config: TableConfig) -> Dict[str, Any]:
        """Валидация синхронизации - сравнение количества записей"""
        try:
            # Получаем количество записей в MySQL
            mysql_count = await self.mysql_client.get_table_count(table_name)
            
            # Получаем количество записей в Google Sheets
            sheet_data = await self.sheets_client.get_sheet_data(table_config.sheet_name)
            # Вычитаем 1 для заголовка
            sheets_count = len(sheet_data) - 1 if sheet_data and len(sheet_data) > 0 else 0
            
            is_valid = mysql_count == sheets_count
            
            result = {
                "table": table_name,
                "mysql_count": mysql_count,
                "sheets_count": sheets_count,
                "is_valid": is_valid,
                "difference": mysql_count - sheets_count
            }
            
            if is_valid:
                logger.info(f"Sync validation passed for {table_name}: {mysql_count} records")
            else:
                logger.warning(f"Sync validation failed for {table_name}: "
                             f"MySQL={mysql_count}, Sheets={sheets_count}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating sync for {table_name}: {e}")
            return {
                "table": table_name,
                "error": str(e),
                "is_valid": False
            }
