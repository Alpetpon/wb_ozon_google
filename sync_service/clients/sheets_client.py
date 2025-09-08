"""
Клиент для работы с Google Sheets API
Обеспечивает чтение, запись и пакетные операции с Google Таблицами
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from loguru import logger
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from asyncio_throttle import Throttler
import time


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets API"""
    
    # Права доступа для API
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    def __init__(self, credentials_file: str, spreadsheet_id: str):
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id
        self.service = None
        self.creds = None
        
        # Throttler для соблюдения лимитов API (100 запросов в 100 секунд)
        self.throttler = Throttler(rate_limit=90, period=100)
        
    async def authenticate(self):
        """Аутентификация с Google API"""
        try:
            if os.path.exists(self.credentials_file):
                # Используем service account credentials
                self.creds = ServiceAccountCredentials.from_service_account_file(
                    self.credentials_file, scopes=self.SCOPES
                )
                logger.info("Authenticated with service account")
            else:
                logger.error(f"Credentials file not found: {self.credentials_file}")
                raise FileNotFoundError(f"Credentials file not found: {self.credentials_file}")
            
            # Создаем сервис
            self.service = build('sheets', 'v4', credentials=self.creds)
            logger.info(f"Google Sheets service initialized for spreadsheet: {self.spreadsheet_id}")
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise
    
    async def _make_request(self, request_func, *args, **kwargs):
        """Выполнение запроса с соблюдением лимитов API и обработкой тайм-аутов"""
        max_retries = 3
        base_delay = 30
        
        async with self.throttler:
            for attempt in range(max_retries):
                try:
                    return request_func(*args, **kwargs).execute()
                except HttpError as e:
                    logger.error(f"Google Sheets API error (attempt {attempt + 1}): {e}")
                    
                    if e.resp.status == 429:  # Rate limit exceeded
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Rate limit exceeded, waiting {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    elif e.resp.status == 400 and "limit" in str(e):
                        # Превышение лимита ячеек - не повторяем
                        logger.error("Cell limit exceeded - not retrying")
                        raise
                    elif attempt < max_retries - 1:
                        # Для других ошибок пробуем еще раз
                        delay = base_delay * (attempt + 1)
                        logger.warning(f"HTTP error, retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    raise
                except Exception as e:
                    error_msg = str(e).lower()
                    if "timeout" in error_msg or "timed out" in error_msg:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Timeout error (attempt {attempt + 1}), retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.error("Max timeout retries exceeded")
                            raise
                    elif "socket" in error_msg or "connection" in error_msg:
                        if attempt < max_retries - 1:
                            delay = base_delay * (attempt + 1)
                            logger.warning(f"Connection error, retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            continue
                    
                    logger.error(f"Unexpected error in Google Sheets request: {e}")
                    raise
    
    async def get_sheet_data(self, sheet_name: str, range_name: str = None) -> List[List[str]]:
        """Получение данных из листа"""
        if not self.service:
            await self.authenticate()
        
        range_str = f"'{sheet_name}'"
        if range_name:
            range_str += f"!{range_name}"
        
        try:
            result = await self._make_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.spreadsheet_id,
                range=range_str
            )
            
            values = result.get('values', [])
            logger.info(f"Retrieved {len(values)} rows from {sheet_name}")
            return values
            
        except HttpError as e:
            if e.resp.status == 400 and "Unable to parse range" in str(e):
                logger.warning(f"Sheet '{sheet_name}' not found, creating...")
                await self.create_sheet(sheet_name)
                return []
            raise
    
    async def update_sheet_data(self, sheet_name: str, range_name: str, 
                               values: List[List[Any]], value_input_option: str = 'RAW') -> Dict:
        """Обновление данных в листе"""
        if not self.service:
            await self.authenticate()
        
        range_str = f"'{sheet_name}'"
        if range_name:
            range_str += f"!{range_name}"
        
        body = {
            'values': values
        }
        
        result = await self._make_request(
            self.service.spreadsheets().values().update,
            spreadsheetId=self.spreadsheet_id,
            range=range_str,
            valueInputOption=value_input_option,
            body=body
        )
        
        logger.info(f"Updated {len(values)} rows in {sheet_name}")
        return result
    
    async def append_sheet_data(self, sheet_name: str, values: List[List[Any]], 
                               range_name: str = None, value_input_option: str = 'RAW') -> Dict:
        """Добавление данных в конец листа"""
        if not self.service:
            await self.authenticate()
        
        range_str = f"'{sheet_name}'"
        if range_name:
            range_str += f"!{range_name}"
        
        body = {
            'values': values
        }
        
        result = await self._make_request(
            self.service.spreadsheets().values().append,
            spreadsheetId=self.spreadsheet_id,
            range=range_str,
            valueInputOption=value_input_option,
            insertDataOption='INSERT_ROWS',
            body=body
        )
        
        logger.info(f"Appended {len(values)} rows to {sheet_name}")
        return result
    
    async def clear_sheet_data(self, sheet_name: str, range_name: str = None) -> Dict:
        """Очистка данных в листе"""
        if not self.service:
            await self.authenticate()
        
        range_str = f"'{sheet_name}'"
        if range_name:
            range_str += f"!{range_name}"
        
        result = await self._make_request(
            self.service.spreadsheets().values().clear,
            spreadsheetId=self.spreadsheet_id,
            range=range_str,
            body={}
        )
        
        logger.info(f"Cleared data in {sheet_name}")
        return result

    async def resize_sheet(self, sheet_name: str, rows: int = 1000, cols: int = 26) -> Dict:
        """Изменение размера листа для экономии ячеек"""
        if not self.service:
            await self.authenticate()
        
        try:
            # Получаем ID листа
            sheet_info = await self.get_sheet_info()
            sheet_id = None
            
            for sheet in sheet_info.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                logger.warning(f"Sheet '{sheet_name}' not found for resize")
                return {}
            
            body = {
                'requests': [{
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'gridProperties': {
                                'rowCount': rows,
                                'columnCount': cols
                            }
                        },
                        'fields': 'gridProperties.rowCount,gridProperties.columnCount'
                    }
                }]
            }
            
            result = await self._make_request(
                self.service.spreadsheets().batchUpdate,
                spreadsheetId=self.spreadsheet_id,
                body=body
            )
            
            logger.info(f"Resized sheet {sheet_name} to {rows}x{cols}")
            return result
            
        except Exception as e:
            logger.error(f"Error resizing sheet '{sheet_name}': {e}")
            return {}
    
    async def batch_update(self, updates: List[Dict]) -> Dict:
        """Пакетное обновление нескольких диапазонов"""
        if not self.service:
            await self.authenticate()
        
        body = {
            'valueInputOption': 'RAW',
            'data': updates
        }
        
        result = await self._make_request(
            self.service.spreadsheets().values().batchUpdate,
            spreadsheetId=self.spreadsheet_id,
            body=body
        )
        
        logger.info(f"Batch updated {len(updates)} ranges")
        return result
    
    async def create_sheet(self, sheet_name: str) -> Dict:
        """Создание нового листа"""
        if not self.service:
            await self.authenticate()
        
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
        }
        
        try:
            result = await self._make_request(
                self.service.spreadsheets().batchUpdate,
                spreadsheetId=self.spreadsheet_id,
                body=body
            )
            logger.info(f"Created sheet: {sheet_name}")
            return result
        except HttpError as e:
            if "already exists" in str(e):
                logger.info(f"Sheet '{sheet_name}' already exists")
                return {}
            raise

    async def delete_sheet(self, sheet_name: str) -> Dict:
        """Удаление листа"""
        if not self.service:
            await self.authenticate()
        
        try:
            # Сначала получаем информацию о листе для получения ID
            sheet_info = await self.get_sheet_info()
            sheet_id = None
            
            for sheet in sheet_info.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                logger.warning(f"Sheet '{sheet_name}' not found for deletion")
                return {}
            
            body = {
                'requests': [{
                    'deleteSheet': {
                        'sheetId': sheet_id
                    }
                }]
            }
            
            result = await self._make_request(
                self.service.spreadsheets().batchUpdate,
                spreadsheetId=self.spreadsheet_id,
                body=body
            )
            logger.info(f"Deleted sheet: {sheet_name}")
            return result
            
        except HttpError as e:
            if "not found" in str(e) or "does not exist" in str(e):
                logger.warning(f"Sheet '{sheet_name}' not found for deletion")
                return {}
            raise
        except Exception as e:
            logger.error(f"Error deleting sheet '{sheet_name}': {e}")
            raise
    
    async def get_sheet_info(self) -> Dict:
        """Получение информации о таблице и всех листах"""
        if not self.service:
            await self.authenticate()
        
        result = await self._make_request(
            self.service.spreadsheets().get,
            spreadsheetId=self.spreadsheet_id
        )
        
        return result

    async def get_spreadsheet_cell_count(self) -> int:
        """Подсчет общего количества используемых ячеек в таблице"""
        try:
            sheet_info = await self.get_sheet_info()
            total_cells = 0
            
            for sheet in sheet_info.get('sheets', []):
                properties = sheet.get('properties', {})
                grid_properties = properties.get('gridProperties', {})
                
                rows = grid_properties.get('rowCount', 0)
                cols = grid_properties.get('columnCount', 0)
                total_cells += rows * cols
            
            return total_cells
            
        except Exception as e:
            logger.error(f"Error calculating cell count: {e}")
            return 0

    async def estimate_data_cells(self, data: List[List[Any]]) -> int:
        """Оценка количества ячеек, которые займут данные"""
        if not data:
            return 0
        
        rows = len(data)
        cols = len(data[0]) if data else 0
        return rows * cols

    async def can_fit_data(self, data: List[List[Any]], max_cells: int = 9500000) -> bool:
        """Проверка, поместятся ли данные в лимит ячеек"""
        current_cells = await self.get_spreadsheet_cell_count()
        estimated_cells = await self.estimate_data_cells(data)
        
        logger.debug(f"Current cells: {current_cells}, Estimated new cells: {estimated_cells}, Max: {max_cells}")
        
        return (current_cells + estimated_cells) <= max_cells
    
    async def sheet_exists(self, sheet_name: str) -> bool:
        """Проверка существования листа"""
        try:
            sheet_info = await self.get_sheet_info()
            sheets = sheet_info.get('sheets', [])
            for sheet in sheets:
                if sheet['properties']['title'] == sheet_name:
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking sheet existence: {e}")
            return False
    
    def convert_to_sheet_format(self, data: List[Dict[str, Any]], 
                               field_mapping: Dict[str, str],
                               include_headers: bool = True) -> List[List[str]]:
        """Конвертация данных из MySQL в формат для Google Sheets"""
        if not data:
            return []
        
        # Создаем заголовки на основе маппинга
        headers = []
        field_order = []
        
        for mysql_field, sheet_header in field_mapping.items():
            if mysql_field in data[0]:  # Проверяем, что поле существует в данных
                headers.append(sheet_header)
                field_order.append(mysql_field)
        
        result = []
        if include_headers:
            result.append(headers)
        
        # Конвертируем данные
        for row in data:
            sheet_row = []
            for field in field_order:
                value = row.get(field, "")
                # Конвертируем все в строки для Google Sheets
                sheet_row.append(str(value) if value is not None else "")
            result.append(sheet_row)
        
        return result
    
    def convert_from_sheet_format(self, data: List[List[str]], 
                                 field_mapping: Dict[str, str],
                                 has_headers: bool = True) -> List[Dict[str, Any]]:
        """Конвертация данных из Google Sheets в формат для MySQL"""
        if not data:
            return []
        
        # Создаем обратный маппинг (sheet_header -> mysql_field)
        reverse_mapping = {v: k for k, v in field_mapping.items()}
        
        start_row = 1 if has_headers else 0
        headers = data[0] if has_headers else list(field_mapping.values())
        
        result = []
        for row_idx in range(start_row, len(data)):
            row_data = data[row_idx]
            mysql_row = {}
            
            for col_idx, header in enumerate(headers):
                if col_idx < len(row_data):
                    mysql_field = reverse_mapping.get(header)
                    if mysql_field:
                        value = row_data[col_idx].strip() if row_data[col_idx] else None
                        # Обработка пустых значений
                        mysql_row[mysql_field] = value if value else None
            
            if mysql_row:  # Добавляем только непустые строки
                result.append(mysql_row)
        
        return result
    
    async def append_data_in_batches(self, sheet_name: str, data: List[List[Any]], 
                                   batch_size: int = 1000) -> Dict[str, int]:
        """Добавление больших данных порциями для избежания лимитов"""
        total_written = 0
        batch_count = 0
        
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                batch_count += 1
                
                # Проверяем лимит ячеек перед записью каждой порции
                if not await self.can_fit_data(batch):
                    logger.warning(f"Cannot fit batch {batch_count} into sheet {sheet_name} - cell limit would be exceeded")
                    break
                
                try:
                    await self.append_sheet_data(sheet_name, batch)
                    total_written += len(batch)
                    logger.info(f"Written batch {batch_count}: {len(batch)} rows to {sheet_name}")
                    
                    # Пауза между батчами для соблюдения лимитов API
                    if batch_count % 10 == 0:  # Каждые 10 батчей делаем более длинную паузу
                        await asyncio.sleep(2)
                    else:
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    logger.error(f"Error writing batch {batch_count} to {sheet_name}: {e}")
                    if "limit" in str(e).lower() or "quota" in str(e).lower():
                        logger.warning("Hit API or cell limit, stopping batch write")
                        break
                    raise
            
            return {"synced": total_written, "batches": batch_count}
            
        except Exception as e:
            logger.error(f"Error in batch append to {sheet_name}: {e}")
            return {"synced": total_written, "batches": batch_count, "error": str(e)}

    async def sync_table_to_sheets(self, table_data: List[Dict[str, Any]], 
                                  sheet_name: str, field_mapping: Dict[str, str],
                                  clear_existing: bool = True, 
                                  max_rows: int = 50000) -> Dict[str, int]:
        """Синхронизация таблицы MySQL в Google Sheets с ограничением на количество строк"""
        try:
            # Ограничиваем количество строк для предотвращения превышения лимитов
            if len(table_data) > max_rows:
                logger.warning(f"Table {sheet_name} has {len(table_data)} rows, limiting to {max_rows}")
                table_data = table_data[:max_rows]
            
            # Конвертируем данные в формат Sheets
            sheet_data = self.convert_to_sheet_format(
                table_data, field_mapping, include_headers=True
            )
            
            if not sheet_data:
                logger.warning(f"No data to sync for sheet: {sheet_name}")
                return {"synced": 0}
            
            # Проверяем лимит ячеек
            if not await self.can_fit_data(sheet_data):
                logger.warning(f"Data for {sheet_name} ({len(sheet_data)} rows) exceeds cell limit")
                return {"synced": 0, "error": "Cell limit exceeded"}
            
            # Проверяем/создаем лист
            if not await self.sheet_exists(sheet_name):
                await self.create_sheet(sheet_name)
            
            # Очищаем существующие данные если требуется
            if clear_existing:
                await self.clear_sheet_data(sheet_name)
            
            # Для больших данных используем батчинг
            if len(sheet_data) > 5000:
                logger.info(f"Using batch write for large dataset: {len(sheet_data)} rows")
                
                # Записываем заголовки отдельно
                headers = sheet_data[0:1]
                data_rows = sheet_data[1:]
                
                await self.update_sheet_data(sheet_name, "A1", headers)
                result = await self.append_data_in_batches(sheet_name, data_rows, batch_size=1000)
                
                return {"synced": result.get("synced", 0) + 1}  # +1 для заголовка
            else:
                # Для небольших данных записываем сразу
                await self.update_sheet_data(sheet_name, "A1", sheet_data)
                logger.info(f"Synced {len(table_data)} rows to sheet: {sheet_name}")
                return {"synced": len(table_data)}
            
        except Exception as e:
            logger.error(f"Error syncing table to sheets: {e}")
            raise
    
    async def sync_sheets_to_table(self, sheet_name: str, 
                                  field_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """Получение данных из Google Sheets для синхронизации с MySQL"""
        try:
            # Получаем данные из листа
            sheet_data = await self.get_sheet_data(sheet_name)
            
            if not sheet_data:
                logger.warning(f"No data found in sheet: {sheet_name}")
                return []
            
            # Конвертируем в формат MySQL
            mysql_data = self.convert_from_sheet_format(
                sheet_data, field_mapping, has_headers=True
            )
            
            logger.info(f"Retrieved {len(mysql_data)} rows from sheet: {sheet_name}")
            return mysql_data
            
        except Exception as e:
            logger.error(f"Error syncing sheets to table: {e}")
            raise
