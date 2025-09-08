"""
Клиент для работы с MySQL базой данных
Обеспечивает подключение, выполнение запросов и пакетные операции
"""

import asyncio
import pymysql
from typing import List, Dict, Any, Optional, Tuple
from loguru import logger
from contextlib import asynccontextmanager
import aiomysql
from pymysql.cursors import DictCursor


class MySQLClient:
    """Асинхронный клиент для работы с MySQL"""
    
    def __init__(self, host: str, user: str, password: str, database: str, 
                 port: int = 3306, charset: str = 'utf8mb4'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.pool = None
        
    async def create_pool(self, minsize: int = 1, maxsize: int = 10):
        """Создание пула соединений"""
        try:
            self.pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.database,
                charset=self.charset,
                minsize=minsize,
                maxsize=maxsize,
                autocommit=True,
                cursorclass=aiomysql.DictCursor
            )
            logger.info(f"MySQL pool created: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to create MySQL pool: {e}")
            raise
    
    async def close_pool(self):
        """Закрытие пула соединений"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("MySQL pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для получения соединения из пула"""
        if not self.pool:
            await self.create_pool()
        
        async with self.pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                await conn.rollback()
                logger.error(f"Database error: {e}")
                raise
    
    async def execute_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """Выполнение запроса (INSERT, UPDATE, DELETE)"""
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                result = await cursor.execute(query, params)
                await conn.commit()
                logger.debug(f"Query executed: {query[:100]}...")
                return result
    
    async def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Получение всех записей"""
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                logger.debug(f"Fetched {len(result)} rows")
                return result
    
    async def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """Получение одной записи"""
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchone()
                return result
    
    async def get_table_data(self, table_name: str, limit: Optional[int] = None, 
                           offset: Optional[int] = None, 
                           where_clause: Optional[str] = None,
                           order_by: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение данных из таблицы с пагинацией"""
        query = f"SELECT * FROM `{table_name}`"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        else:
            query += " ORDER BY id"
        
        if limit:
            query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"
        
        return await self.fetch_all(query)
    
    async def get_table_count(self, table_name: str, where_clause: Optional[str] = None) -> int:
        """Получение количества записей в таблице"""
        query = f"SELECT COUNT(*) as count FROM `{table_name}`"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        result = await self.fetch_one(query)
        return result['count'] if result else 0
    
    async def batch_insert(self, table_name: str, data: List[Dict[str, Any]], 
                          on_duplicate: str = "UPDATE") -> int:
        """Пакетная вставка данных с обработкой дубликатов"""
        if not data:
            return 0
        
        # Получаем колонки из первой записи
        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'`{col}`' for col in columns])
        
        # Формируем основной запрос
        query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # Добавляем обработку дубликатов
        if on_duplicate.upper() == "UPDATE":
            update_clause = ', '.join([f'`{col}` = VALUES(`{col}`)' for col in columns])
            query += f" ON DUPLICATE KEY UPDATE {update_clause}"
        elif on_duplicate.upper() == "IGNORE":
            query = query.replace("INSERT", "INSERT IGNORE")
        
        # Подготавливаем данные для выполнения
        values = []
        for row in data:
            values.append(tuple(row.get(col) for col in columns))
        
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                result = await cursor.executemany(query, values)
                await conn.commit()
                logger.info(f"Batch inserted {len(data)} rows into {table_name}")
                return result
    
    async def batch_update(self, table_name: str, data: List[Dict[str, Any]], 
                          key_columns: List[str]) -> int:
        """Пакетное обновление данных"""
        if not data:
            return 0
        
        updated_count = 0
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                for row in data:
                    # Разделяем данные на ключевые и обновляемые поля
                    key_values = {k: v for k, v in row.items() if k in key_columns}
                    update_values = {k: v for k, v in row.items() if k not in key_columns}
                    
                    if not update_values:
                        continue
                    
                    # Формируем WHERE условие
                    where_conditions = ' AND '.join([f"`{k}` = %s" for k in key_values.keys()])
                    
                    # Формируем SET условие  
                    set_clause = ', '.join([f"`{k}` = %s" for k in update_values.keys()])
                    
                    query = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_conditions}"
                    params = tuple(update_values.values()) + tuple(key_values.values())
                    
                    result = await cursor.execute(query, params)
                    updated_count += result
                
                await conn.commit()
                logger.info(f"Batch updated {updated_count} rows in {table_name}")
                return updated_count
    
    async def upsert_data(self, table_name: str, data: List[Dict[str, Any]], 
                         unique_keys: List[str]) -> Dict[str, int]:
        """Upsert операция (вставка или обновление)"""
        if not data:
            return {"inserted": 0, "updated": 0}
        
        # Проверяем существующие записи
        existing_data = {}
        if unique_keys:
            # Формируем условие для поиска существующих записей
            key_combinations = []
            for row in data:
                key_combo = tuple(row.get(key) for key in unique_keys)
                if all(v is not None for v in key_combo):
                    key_combinations.append(key_combo)
            
            if key_combinations:
                # Строим запрос для поиска существующих записей
                where_conditions = []
                params = []
                for combo in key_combinations:
                    combo_condition = ' AND '.join([f"`{key}` = %s" for key in unique_keys])
                    where_conditions.append(f"({combo_condition})")
                    params.extend(combo)
                
                where_clause = ' OR '.join(where_conditions)
                query = f"SELECT {', '.join([f'`{key}`' for key in unique_keys])}, id FROM `{table_name}` WHERE {where_clause}"
                
                existing_rows = await self.fetch_all(query, tuple(params))
                for row in existing_rows:
                    key_combo = tuple(row.get(key) for key in unique_keys)
                    existing_data[key_combo] = row['id']
        
        # Разделяем данные на новые и существующие
        new_data = []
        update_data = []
        
        for row in data:
            key_combo = tuple(row.get(key) for key in unique_keys)
            if key_combo in existing_data:
                # Добавляем ID для обновления
                row['id'] = existing_data[key_combo]
                update_data.append(row)
            else:
                new_data.append(row)
        
        # Выполняем операции
        inserted = 0
        updated = 0
        
        if new_data:
            inserted = await self.batch_insert(table_name, new_data, "IGNORE")
        
        if update_data:
            updated = await self.batch_update(table_name, update_data, ['id'])
        
        logger.info(f"Upsert completed: {inserted} inserted, {updated} updated")
        return {"inserted": inserted, "updated": updated}
    
    async def get_table_structure(self, table_name: str) -> List[Dict[str, Any]]:
        """Получение структуры таблицы"""
        query = f"DESCRIBE `{table_name}`"
        return await self.fetch_all(query)
    
    async def table_exists(self, table_name: str) -> bool:
        """Проверка существования таблицы"""
        query = """
        SELECT COUNT(*) as count FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
        """
        result = await self.fetch_one(query, (self.database, table_name))
        return result['count'] > 0 if result else False
