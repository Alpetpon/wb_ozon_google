"""
Модуль синхронизации данных Google Sheets -> MySQL
Обеспечивает транзакционный upsert и обработку конфликтов
"""

import asyncio
from typing import Dict, List, Any, Optional
from loguru import logger
from datetime import datetime

from ..clients.mysql_client import MySQLClient
from ..clients.sheets_client import GoogleSheetsClient
from ..config.config_loader import TableConfig, SyncConfig


class SheetsToMySQLSync:
    """Синхронизатор Google Sheets -> MySQL"""
    
    def __init__(self, mysql_client: MySQLClient, sheets_client: GoogleSheetsClient,
                 sync_config: SyncConfig):
        self.mysql_client = mysql_client
        self.sheets_client = sheets_client
        self.sync_config = sync_config
        
    async def sync_table(self, table_name: str, table_config: TableConfig,
                        validate_data: bool = True) -> Dict[str, Any]:
        """Синхронизация одной таблицы из Sheets в MySQL"""
        try:
            logger.info(f"Starting Sheets -> MySQL sync for table: {table_name}")
            start_time = datetime.now()
            
            # Проверяем направление синхронизации
            if table_config.sync_direction not in ['both', 'sheets_to_mysql']:
                logger.info(f"Skipping table {table_name}: sync direction is {table_config.sync_direction}")
                return {"status": "skipped", "reason": "sync_direction"}
            
            # Получаем данные из Google Sheets
            sheets_data = await self.sheets_client.sync_sheets_to_table(
                table_config.sheet_name, table_config.fields
            )
            
            if not sheets_data:
                logger.info(f"No data found in sheet: {table_config.sheet_name}")
                return {"status": "completed", "synced": 0, "total": 0}
            
            # Валидируем данные если требуется
            if validate_data:
                validation_result = await self._validate_sheet_data(
                    sheets_data, table_name, table_config
                )
                if not validation_result["is_valid"]:
                    return {
                        "status": "error",
                        "error": "Data validation failed",
                        "validation_errors": validation_result["errors"]
                    }
            
            # Разбиваем на батчи для обработки
            batch_size = min(table_config.batch_size, self.sync_config.max_batch_size)
            total_batches = (len(sheets_data) + batch_size - 1) // batch_size
            
            total_inserted = 0
            total_updated = 0
            errors = []
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(sheets_data))
                batch_data = sheets_data[start_idx:end_idx]
                
                logger.info(f"Processing batch {batch_num + 1}/{total_batches} "
                           f"({len(batch_data)} records)")
                
                # Обрабатываем батч
                batch_result = await self._process_batch(
                    batch_data, table_name, table_config
                )
                
                total_inserted += batch_result.get("inserted", 0)
                total_updated += batch_result.get("updated", 0)
                
                if batch_result.get("errors"):
                    errors.extend(batch_result["errors"])
                
                # Пауза между батчами
                if batch_num < total_batches - 1:
                    await asyncio.sleep(0.5)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "completed",
                "table": table_name,
                "total_records": len(sheets_data),
                "inserted": total_inserted,
                "updated": total_updated,
                "errors_count": len(errors),
                "duration_seconds": duration,
                "batches": total_batches
            }
            
            if errors:
                result["errors"] = errors[:10]  # Показываем первые 10 ошибок
            
            logger.info(f"Completed Sheets -> MySQL sync for {table_name}: "
                       f"{total_inserted} inserted, {total_updated} updated, "
                       f"{len(errors)} errors in {duration:.2f}s")
            
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
                            exported_to_db=True
                        )
                        break
            except Exception as e:
                # Не критично, если не можем обновить статус
                pass
            
            return result
            
        except Exception as e:
            logger.error(f"Error syncing table {table_name} from Sheets: {e}")
            return {
                "status": "error",
                "table": table_name,
                "error": str(e)
            }
    
    async def _process_batch(self, batch_data: List[Dict[str, Any]], 
                            table_name: str, table_config: TableConfig) -> Dict[str, Any]:
        """Обработка одного батча данных"""
        try:
            # Подготавливаем данные для upsert
            prepared_data = []
            errors = []
            
            for row_idx, row in enumerate(batch_data):
                try:
                    # Очищаем и подготавливаем данные
                    cleaned_row = self._clean_row_data(row, table_config)
                    if cleaned_row:
                        prepared_data.append(cleaned_row)
                except Exception as e:
                    errors.append(f"Row {row_idx}: {str(e)}")
            
            if not prepared_data:
                return {"inserted": 0, "updated": 0, "errors": errors}
            
            # Выполняем upsert операцию
            upsert_result = await self.mysql_client.upsert_data(
                table_name, prepared_data, table_config.unique_keys
            )
            
            return {
                "inserted": upsert_result.get("inserted", 0),
                "updated": upsert_result.get("updated", 0),
                "errors": errors
            }
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return {"inserted": 0, "updated": 0, "errors": [str(e)]}
    
    def _clean_row_data(self, row: Dict[str, Any], table_config: TableConfig) -> Dict[str, Any]:
        """Очистка и подготовка данных строки"""
        cleaned_row = {}
        
        for mysql_field, value in row.items():
            if mysql_field in table_config.fields:
                # Очищаем значение
                if value is None or value == "":
                    cleaned_value = None
                elif isinstance(value, str):
                    cleaned_value = value.strip()
                    if cleaned_value == "":
                        cleaned_value = None
                else:
                    cleaned_value = value
                
                # Специальная обработка для определенных типов полей
                if mysql_field == 'id' and cleaned_value is not None:
                    try:
                        cleaned_value = int(cleaned_value)
                    except (ValueError, TypeError):
                        # Если ID не число, пропускаем его (будет автоинкремент)
                        continue
                
                cleaned_row[mysql_field] = cleaned_value
        
        return cleaned_row
    
    async def _validate_sheet_data(self, sheets_data: List[Dict[str, Any]], 
                                  table_name: str, table_config: TableConfig) -> Dict[str, Any]:
        """Валидация данных из Sheets"""
        errors = []
        
        try:
            # Получаем структуру таблицы
            table_structure = await self.mysql_client.get_table_structure(table_name)
            table_fields = {field['Field']: field for field in table_structure}
            
            # Проверяем каждую строку
            for row_idx, row in enumerate(sheets_data):
                row_errors = []
                
                # Проверяем обязательные поля
                for mysql_field in table_config.fields.keys():
                    field_info = table_fields.get(mysql_field)
                    if not field_info:
                        continue
                    
                    value = row.get(mysql_field)
                    
                    # Проверяем NOT NULL поля (кроме автоинкремента)
                    if (field_info['Null'] == 'NO' and 
                        'auto_increment' not in field_info.get('Extra', '') and
                        mysql_field != table_config.primary_key and
                        (value is None or value == "")):
                        row_errors.append(f"Field '{mysql_field}' cannot be empty")
                    
                    # Проверяем длину строковых полей
                    if value and isinstance(value, str):
                        field_type = field_info.get('Type', '')
                        if 'varchar' in field_type:
                            try:
                                max_length = int(field_type.split('(')[1].split(')')[0])
                                if len(value) > max_length:
                                    row_errors.append(f"Field '{mysql_field}' too long: {len(value)} > {max_length}")
                            except:
                                pass
                
                if row_errors:
                    errors.append(f"Row {row_idx + 1}: {'; '.join(row_errors)}")
            
            is_valid = len(errors) == 0
            
            if not is_valid:
                logger.warning(f"Data validation found {len(errors)} errors")
            
            return {
                "is_valid": is_valid,
                "errors": errors[:50]  # Ограничиваем количество ошибок
            }
            
        except Exception as e:
            logger.error(f"Error during data validation: {e}")
            return {
                "is_valid": False,
                "errors": [f"Validation error: {str(e)}"]
            }
    
    async def sync_multiple_tables(self, table_configs: Dict[str, TableConfig],
                                  validate_data: bool = True) -> Dict[str, Any]:
        """Синхронизация нескольких таблиц"""
        logger.info(f"Starting Sheets -> MySQL sync for {len(table_configs)} tables")
        start_time = datetime.now()
        
        results = {}
        total_inserted = 0
        total_updated = 0
        successful_tables = 0
        failed_tables = 0
        
        for table_name, table_config in table_configs.items():
            try:
                if not table_config.enabled:
                    logger.info(f"Skipping disabled table: {table_name}")
                    results[table_name] = {"status": "disabled"}
                    continue
                
                # Синхронизируем таблицу
                result = await self.sync_table(table_name, table_config, validate_data)
                results[table_name] = result
                
                if result["status"] == "completed":
                    total_inserted += result.get("inserted", 0)
                    total_updated += result.get("updated", 0)
                    successful_tables += 1
                else:
                    failed_tables += 1
                
            except Exception as e:
                logger.error(f"Failed to sync table {table_name} from Sheets: {e}")
                results[table_name] = {"status": "error", "error": str(e)}
                failed_tables += 1
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        summary = {
            "status": "completed",
            "total_tables": len(table_configs),
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "results": results
        }
        
        logger.info(f"Sheets -> MySQL sync completed: {successful_tables}/{len(table_configs)} tables, "
                   f"{total_inserted} inserted, {total_updated} updated in {duration:.2f}s")
        
        return summary
    
    async def detect_conflicts(self, table_name: str, table_config: TableConfig) -> Dict[str, Any]:
        """Обнаружение конфликтов между Sheets и MySQL"""
        try:
            logger.info(f"Detecting conflicts for table: {table_name}")
            
            # Получаем данные из обоих источников
            mysql_data = await self.mysql_client.get_table_data(table_name)
            sheets_data = await self.sheets_client.sync_sheets_to_table(
                table_config.sheet_name, table_config.fields
            )
            
            # Создаем индексы по уникальным ключам
            mysql_index = {}
            sheets_index = {}
            
            for row in mysql_data:
                key = self._create_unique_key(row, table_config.unique_keys)
                if key:
                    mysql_index[key] = row
            
            for row in sheets_data:
                key = self._create_unique_key(row, table_config.unique_keys)
                if key:
                    sheets_index[key] = row
            
            conflicts = []
            only_in_mysql = []
            only_in_sheets = []
            
            # Ищем конфликты и различия
            all_keys = set(mysql_index.keys()) | set(sheets_index.keys())
            
            for key in all_keys:
                mysql_row = mysql_index.get(key)
                sheets_row = sheets_index.get(key)
                
                if mysql_row and sheets_row:
                    # Проверяем различия в данных
                    differences = self._compare_rows(mysql_row, sheets_row, table_config.fields)
                    if differences:
                        conflicts.append({
                            "key": key,
                            "differences": differences,
                            "mysql_data": mysql_row,
                            "sheets_data": sheets_row
                        })
                elif mysql_row and not sheets_row:
                    only_in_mysql.append(mysql_row)
                elif sheets_row and not mysql_row:
                    only_in_sheets.append(sheets_row)
            
            result = {
                "table": table_name,
                "conflicts_count": len(conflicts),
                "only_in_mysql_count": len(only_in_mysql),
                "only_in_sheets_count": len(only_in_sheets),
                "conflicts": conflicts[:10],  # Показываем первые 10 конфликтов
                "only_in_mysql": only_in_mysql[:10],
                "only_in_sheets": only_in_sheets[:10]
            }
            
            logger.info(f"Conflict detection completed for {table_name}: "
                       f"{len(conflicts)} conflicts, {len(only_in_mysql)} only in MySQL, "
                       f"{len(only_in_sheets)} only in Sheets")
            
            return result
            
        except Exception as e:
            logger.error(f"Error detecting conflicts for {table_name}: {e}")
            return {
                "table": table_name,
                "error": str(e)
            }
    
    def _create_unique_key(self, row: Dict[str, Any], unique_keys: List[str]) -> Optional[str]:
        """Создание уникального ключа из записи"""
        try:
            key_parts = []
            for field in unique_keys:
                value = row.get(field)
                if value is None:
                    return None
                key_parts.append(str(value))
            return "|".join(key_parts)
        except Exception:
            return None
    
    def _compare_rows(self, mysql_row: Dict[str, Any], sheets_row: Dict[str, Any], 
                     field_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """Сравнение двух записей на предмет различий"""
        differences = []
        
        for mysql_field in field_mapping.keys():
            mysql_value = mysql_row.get(mysql_field)
            sheets_value = sheets_row.get(mysql_field)
            
            # Нормализуем значения для сравнения
            mysql_normalized = str(mysql_value).strip() if mysql_value is not None else ""
            sheets_normalized = str(sheets_value).strip() if sheets_value is not None else ""
            
            if mysql_normalized != sheets_normalized:
                differences.append({
                    "field": mysql_field,
                    "mysql_value": mysql_value,
                    "sheets_value": sheets_value
                })
        
        return differences
