"""
Обработчик очереди изменений
Читает записи из change_log и синхронизирует их с Google Sheets
"""

import asyncio
from typing import Dict, List, Any, Optional
from loguru import logger
from datetime import datetime, timedelta
import json

from ..clients.mysql_client import MySQLClient
from ..clients.sheets_client import GoogleSheetsClient
from ..config.config_loader import ConfigLoader, TableConfig, SyncConfig


class ChangeProcessor:
    """Обработчик изменений из change_log"""
    
    def __init__(self, mysql_client: MySQLClient, sheets_client: GoogleSheetsClient,
                 sync_config: SyncConfig, config_loader: ConfigLoader):
        self.mysql_client = mysql_client
        self.sheets_client = sheets_client
        self.sync_config = sync_config
        self.config_loader = config_loader
        self.is_running = False
        
    async def start_processing(self, polling_interval: int = 30):
        """Запуск обработчика в режиме демона"""
        self.is_running = True
        logger.info(f"Starting change processor with {polling_interval}s interval")
        
        while self.is_running:
            try:
                await self.process_pending_changes()
                await asyncio.sleep(polling_interval)
            except Exception as e:
                logger.error(f"Error in change processing loop: {e}")
                await asyncio.sleep(5)  # Короткая пауза при ошибке
    
    def stop_processing(self):
        """Остановка обработчика"""
        self.is_running = False
        logger.info("Change processor stopped")
    
    async def process_pending_changes(self) -> Dict[str, Any]:
        """Обработка всех ожидающих изменений"""
        try:
            # Получаем pending изменения
            pending_changes = await self._get_pending_changes()
            
            if not pending_changes:
                logger.debug("No pending changes found")
                return {"processed": 0, "errors": 0}
            
            logger.info(f"Processing {len(pending_changes)} pending changes")
            
            # Группируем изменения по таблицам
            changes_by_table = self._group_changes_by_table(pending_changes)
            
            processed_count = 0
            error_count = 0
            
            # Обрабатываем изменения для каждой таблицы
            for table_name, changes in changes_by_table.items():
                try:
                    table_config = self.config_loader.get_table_config(table_name)
                    if not table_config or not table_config.enabled:
                        logger.warning(f"Table {table_name} not configured or disabled")
                        await self._mark_changes_as_failed(
                            [c['id'] for c in changes],
                            "Table not configured or disabled"
                        )
                        error_count += len(changes)
                        continue
                    
                    # Обрабатываем изменения таблицы
                    result = await self._process_table_changes(table_name, changes, table_config)
                    processed_count += result.get("processed", 0)
                    error_count += result.get("errors", 0)
                    
                except Exception as e:
                    logger.error(f"Error processing changes for table {table_name}: {e}")
                    await self._mark_changes_as_failed(
                        [c['id'] for c in changes],
                        str(e)
                    )
                    error_count += len(changes)
            
            logger.info(f"Change processing completed: {processed_count} processed, {error_count} errors")
            
            return {
                "processed": processed_count,
                "errors": error_count,
                "total": len(pending_changes)
            }
            
        except Exception as e:
            logger.error(f"Error in process_pending_changes: {e}")
            return {"processed": 0, "errors": 0, "error": str(e)}
    
    async def _get_pending_changes(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Получение ожидающих обработки изменений"""
        query = """
        SELECT id, table_name, change_type, row_id, old_data, new_data, 
               created_at
        FROM change_log 
        WHERE status = 'PENDING' 
        ORDER BY created_at ASC 
        LIMIT %s
        """
        
        changes = await self.mysql_client.fetch_all(query, (limit,))
        
        # Парсим JSON поля
        for change in changes:
            if change['old_data']:
                change['old_data'] = json.loads(change['old_data'])
            if change['new_data']:
                change['new_data'] = json.loads(change['new_data'])
        
        return changes
    
    def _group_changes_by_table(self, changes: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Группировка изменений по таблицам"""
        grouped = {}
        for change in changes:
            table_name = change['table_name']
            if table_name not in grouped:
                grouped[table_name] = []
            grouped[table_name].append(change)
        
        return grouped
    
    async def _process_table_changes(self, table_name: str, changes: List[Dict[str, Any]], 
                                   table_config: TableConfig) -> Dict[str, Any]:
        """Обработка изменений для одной таблицы"""
        processed_count = 0
        error_count = 0
        
        # Отмечаем изменения как обрабатываемые
        change_ids = [c['id'] for c in changes]
        await self._mark_changes_as_processing(change_ids)
        
        try:
            # Применяем изменения в зависимости от типа операции
            for change in changes:
                try:
                    success = await self._apply_change_to_sheets(change, table_config)
                    if success:
                        await self._mark_change_as_completed(change['id'])
                        processed_count += 1
                    else:
                        await self._mark_change_as_failed(change['id'], "Failed to apply change")
                        error_count += 1
                        
                except Exception as e:
                    logger.error(f"Error applying change {change['id']}: {e}")
                    await self._mark_change_as_failed(change['id'], str(e))
                    error_count += 1
            
            return {"processed": processed_count, "errors": error_count}
            
        except Exception as e:
            logger.error(f"Error processing table changes for {table_name}: {e}")
            # Возвращаем все изменения в pending
            await self._mark_changes_as_pending(change_ids)
            return {"processed": 0, "errors": len(changes)}
    
    async def _apply_change_to_sheets(self, change: Dict[str, Any], 
                                     table_config: TableConfig) -> bool:
        """Применение конкретного изменения в Google Sheets"""
        try:
            change_type = change['change_type']
            
            if change_type == 'INSERT':
                return await self._handle_insert(change, table_config)
            elif change_type == 'UPDATE':
                return await self._handle_update(change, table_config)
            elif change_type == 'DELETE':
                return await self._handle_delete(change, table_config)
            else:
                logger.error(f"Unknown change type: {change_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error applying change to sheets: {e}")
            return False
    
    async def _handle_insert(self, change: Dict[str, Any], table_config: TableConfig) -> bool:
        """Обработка INSERT операции"""
        try:
            new_data = change['new_data']
            if not new_data:
                return False
            
            # Конвертируем данные в формат Sheets
            sheet_data = self.sheets_client.convert_to_sheet_format(
                [new_data], table_config.fields, include_headers=False
            )
            
            if not sheet_data:
                return False
            
            # Добавляем строку в конец листа
            await self.sheets_client.append_sheet_data(
                table_config.sheet_name, sheet_data
            )
            
            logger.debug(f"Inserted record {change['row_id']} to sheet {table_config.sheet_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error handling INSERT: {e}")
            return False
    
    async def _handle_update(self, change: Dict[str, Any], table_config: TableConfig) -> bool:
        """Обработка UPDATE операции"""
        try:
            new_data = change['new_data']
            if not new_data:
                return False
            
            # Для UPDATE нужно найти строку в Sheets и обновить её
            # Это более сложная операция, которая требует поиска по уникальным ключам
            
            # Получаем данные из листа
            sheet_data = await self.sheets_client.get_sheet_data(table_config.sheet_name)
            if not sheet_data or len(sheet_data) < 2:  # Нет данных кроме заголовка
                return False
            
            headers = sheet_data[0]
            rows = sheet_data[1:]
            
            # Создаем маппинг заголовков к индексам
            header_to_index = {header: idx for idx, header in enumerate(headers)}
            
            # Ищем строку для обновления по уникальным ключам
            target_row_index = None
            for row_idx, row in enumerate(rows):
                match = True
                for unique_key in table_config.unique_keys:
                    sheet_header = table_config.fields.get(unique_key)
                    if not sheet_header or sheet_header not in header_to_index:
                        continue
                    
                    col_idx = header_to_index[sheet_header]
                    if col_idx < len(row):
                        sheet_value = str(row[col_idx]).strip()
                        db_value = str(new_data.get(unique_key, "")).strip()
                        if sheet_value != db_value:
                            match = False
                            break
                
                if match:
                    target_row_index = row_idx + 2  # +2 because 1-indexed and skip header
                    break
            
            if target_row_index is None:
                # Строка не найдена, добавляем как новую
                return await self._handle_insert(change, table_config)
            
            # Обновляем найденную строку
            updated_row = self.sheets_client.convert_to_sheet_format(
                [new_data], table_config.fields, include_headers=False
            )
            
            if updated_row:
                range_name = f"A{target_row_index}:{chr(65 + len(updated_row[0]) - 1)}{target_row_index}"
                await self.sheets_client.update_sheet_data(
                    table_config.sheet_name, range_name, updated_row
                )
                
                logger.debug(f"Updated record {change['row_id']} in sheet {table_config.sheet_name}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error handling UPDATE: {e}")
            return False
    
    async def _handle_delete(self, change: Dict[str, Any], table_config: TableConfig) -> bool:
        """Обработка DELETE операции"""
        try:
            old_data = change['old_data']
            if not old_data:
                return False
            
            # Для DELETE нужно найти и удалить строку
            # Получаем данные из листа
            sheet_data = await self.sheets_client.get_sheet_data(table_config.sheet_name)
            if not sheet_data or len(sheet_data) < 2:
                return True  # Нет данных для удаления
            
            headers = sheet_data[0]
            rows = sheet_data[1:]
            
            # Создаем маппинг заголовков к индексам
            header_to_index = {header: idx for idx, header in enumerate(headers)}
            
            # Ищем строку для удаления
            target_row_index = None
            for row_idx, row in enumerate(rows):
                match = True
                for unique_key in table_config.unique_keys:
                    sheet_header = table_config.fields.get(unique_key)
                    if not sheet_header or sheet_header not in header_to_index:
                        continue
                    
                    col_idx = header_to_index[sheet_header]
                    if col_idx < len(row):
                        sheet_value = str(row[col_idx]).strip()
                        db_value = str(old_data.get(unique_key, "")).strip()
                        if sheet_value != db_value:
                            match = False
                            break
                
                if match:
                    target_row_index = row_idx + 2  # +2 because 1-indexed and skip header
                    break
            
            if target_row_index is None:
                return True  # Строка уже удалена или не найдена
            
            # Очищаем строку (удаление строк через API сложнее)
            empty_row = [[""] * len(headers)]
            range_name = f"A{target_row_index}:{chr(65 + len(headers) - 1)}{target_row_index}"
            
            await self.sheets_client.update_sheet_data(
                table_config.sheet_name, range_name, empty_row
            )
            
            logger.debug(f"Deleted record {change['row_id']} from sheet {table_config.sheet_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error handling DELETE: {e}")
            return False
    
    async def _mark_changes_as_processing(self, change_ids: List[int]):
        """Отметка изменений как обрабатываемых"""
        if not change_ids:
            return
        
        placeholders = ','.join(['%s'] * len(change_ids))
        query = f"""
        UPDATE change_log 
        SET status = 'PROCESSING', processed_at = NOW() 
        WHERE id IN ({placeholders})
        """
        await self.mysql_client.execute_query(query, tuple(change_ids))
    
    async def _mark_change_as_completed(self, change_id: int):
        """Отметка изменения как завершенного"""
        query = """
        UPDATE change_log 
        SET status = 'COMPLETED', processed_at = NOW(), error_message = NULL 
        WHERE id = %s
        """
        await self.mysql_client.execute_query(query, (change_id,))
    
    async def _mark_change_as_failed(self, change_id: int, error_message: str):
        """Отметка изменения как неудачного"""
        query = """
        UPDATE change_log 
        SET status = 'FAILED', processed_at = NOW(), error_message = %s 
        WHERE id = %s
        """
        await self.mysql_client.execute_query(query, (error_message, change_id))
    
    async def _mark_changes_as_failed(self, change_ids: List[int], error_message: str):
        """Отметка множественных изменений как неудачных"""
        if not change_ids:
            return
        
        placeholders = ','.join(['%s'] * len(change_ids))
        query = f"""
        UPDATE change_log 
        SET status = 'FAILED', processed_at = NOW(), error_message = %s 
        WHERE id IN ({placeholders})
        """
        await self.mysql_client.execute_query(query, (error_message,) + tuple(change_ids))
    
    async def _mark_change_as_pending(self, change_id: int):
        """Возврат изменения в статус pending"""
        query = """
        UPDATE change_log 
        SET status = 'PENDING', processed_at = NULL 
        WHERE id = %s
        """
        await self.mysql_client.execute_query(query, (change_id,))
    
    async def _mark_changes_as_pending(self, change_ids: List[int]):
        """Возврат множественных изменений в статус pending"""
        if not change_ids:
            return
        
        placeholders = ','.join(['%s'] * len(change_ids))
        query = f"""
        UPDATE change_log 
        SET status = 'PENDING', processed_at = NULL 
        WHERE id IN ({placeholders})
        """
        await self.mysql_client.execute_query(query, tuple(change_ids))
    
    
    async def get_change_log_stats(self) -> Dict[str, Any]:
        """Получение статистики change_log"""
        query = """
        SELECT 
            status,
            COUNT(*) as count,
            MIN(created_at) as oldest,
            MAX(created_at) as newest
        FROM change_log 
        GROUP BY status
        """
        
        stats = await self.mysql_client.fetch_all(query)
        
        # Получаем общую статистику
        total_query = "SELECT COUNT(*) as total FROM change_log"
        total_result = await self.mysql_client.fetch_one(total_query)
        total_count = total_result['total'] if total_result else 0
        
        return {
            "total_records": total_count,
            "by_status": stats,
            "timestamp": datetime.now().isoformat()
        }
    
    async def cleanup_old_records(self, retention_days: int = 30) -> int:
        """Очистка старых обработанных записей"""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        query = """
        DELETE FROM change_log 
        WHERE status IN ('COMPLETED', 'FAILED') 
        AND processed_at < %s
        """
        
        result = await self.mysql_client.execute_query(query, (cutoff_date,))
        logger.info(f"Cleaned up {result} old change_log records older than {retention_days} days")
        
        return result
