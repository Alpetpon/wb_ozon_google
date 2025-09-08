"""
Загрузчик конфигурации для системы синхронизации
Читает настройки из YAML файлов и переменных окружения
"""

import os
import yaml
from typing import Dict, Any, Optional
from dataclasses import dataclass
from loguru import logger
from pathlib import Path


@dataclass
class MySQLConfig:
    """Конфигурация MySQL"""
    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    charset: str = 'utf8mb4'


@dataclass
class GoogleSheetsConfig:
    """Конфигурация Google Sheets"""
    credentials_file: str
    spreadsheet_id: str
    api_timeout: int = 30


@dataclass
class SyncConfig:
    """Конфигурация синхронизации"""
    default_sync_interval: int = 300
    max_batch_size: int = 2000
    retry_attempts: int = 3
    retry_delay: int = 5
    log_level: str = "INFO"
    log_file: str = "logs/sync.log"
    log_retention_days: int = 30


@dataclass
class TableConfig:
    """Конфигурация таблицы"""
    enabled: bool
    sheet_name: str
    sheet_range: str
    sync_direction: str  # both/mysql_to_sheets/sheets_to_mysql
    batch_size: int
    primary_key: str
    unique_keys: list
    fields: Dict[str, str]  # mysql_field -> sheet_header


@dataclass
class APIConfig:
    """Конфигурация API сервиса"""
    host: str = "0.0.0.0"
    port: int = 8000
    secret_key: str = "your-secret-key-here"


class ConfigLoader:
    """Загрузчик конфигурации"""
    
    def __init__(self, config_dir: str = None):
        if config_dir is None:
            config_dir = Path(__file__).parent
        self.config_dir = Path(config_dir)
        
        # Пути к файлам конфигурации
        self.table_mapping_file = self.config_dir / "table_mapping.yaml"
        
    def load_env_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из переменных окружения"""
        # Проверяем config.env файл
        env_file = Path("config.env")
        if env_file.exists():
            self._load_env_file(env_file)
        
        return {
            'mysql': MySQLConfig(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER', 'root'),
                password=os.getenv('MYSQL_PASSWORD', ''),
                database=os.getenv('MYSQL_DATABASE', 'test'),
                port=int(os.getenv('MYSQL_PORT', '3306')),
                charset=os.getenv('MYSQL_CHARSET', 'utf8mb4')
            ),
            'google_sheets': GoogleSheetsConfig(
                credentials_file=os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials/google.json'),
                spreadsheet_id=os.getenv('GOOGLE_SPREADSHEET_ID', ''),
                api_timeout=int(os.getenv('SHEETS_API_TIMEOUT', '30'))
            ),
            'api': APIConfig(
                host=os.getenv('API_HOST', '0.0.0.0'),
                port=int(os.getenv('API_PORT', '8000')),
                secret_key=os.getenv('API_SECRET_KEY', 'your-secret-key-here')
            ),
            'sync': SyncConfig(
                default_sync_interval=int(os.getenv('SYNC_INTERVAL_SECONDS', '300')),
                max_batch_size=int(os.getenv('BATCH_SIZE', '1000')),
                log_level=os.getenv('LOG_LEVEL', 'INFO')
            )
        }
    
    def _load_env_file(self, env_file: Path):
        """Загрузка переменных из .env файла"""
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
            logger.info(f"Loaded environment variables from {env_file}")
        except Exception as e:
            logger.warning(f"Failed to load env file {env_file}: {e}")
    
    def load_table_mapping(self) -> Dict[str, Any]:
        """Загрузка маппинга таблиц из YAML"""
        if not self.table_mapping_file.exists():
            logger.error(f"Table mapping file not found: {self.table_mapping_file}")
            raise FileNotFoundError(f"Table mapping file not found: {self.table_mapping_file}")
        
        try:
            with open(self.table_mapping_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Loaded table mapping from {self.table_mapping_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load table mapping: {e}")
            raise
    
    def get_table_config(self, table_name: str) -> Optional[TableConfig]:
        """Получение конфигурации конкретной таблицы"""
        mapping = self.load_table_mapping()
        tables = mapping.get('tables', {})
        
        if table_name not in tables:
            return None
        
        table_data = tables[table_name]
        return TableConfig(
            enabled=table_data.get('enabled', True),
            sheet_name=table_data['sheet_name'],
            sheet_range=table_data.get('sheet_range', 'A:Z'),
            sync_direction=table_data.get('sync_direction', 'both'),
            batch_size=table_data.get('batch_size', 1000),
            primary_key=table_data.get('primary_key', 'id'),
            unique_keys=table_data.get('unique_keys', []),
            fields=table_data.get('fields', {})
        )
    
    def get_enabled_tables(self) -> Dict[str, TableConfig]:
        """Получение всех включенных таблиц"""
        mapping = self.load_table_mapping()
        tables = mapping.get('tables', {})
        
        enabled_tables = {}
        for table_name, table_data in tables.items():
            if table_data.get('enabled', True):
                enabled_tables[table_name] = TableConfig(
                    enabled=True,
                    sheet_name=table_data['sheet_name'],
                    sheet_range=table_data.get('sheet_range', 'A:Z'),
                    sync_direction=table_data.get('sync_direction', 'both'),
                    batch_size=table_data.get('batch_size', 1000),
                    primary_key=table_data.get('primary_key', 'id'),
                    unique_keys=table_data.get('unique_keys', []),
                    fields=table_data.get('fields', {})
                )
        
        return enabled_tables
    
    def get_sync_settings(self) -> SyncConfig:
        """Получение настроек синхронизации"""
        try:
            mapping = self.load_table_mapping()
            settings = mapping.get('sync_settings', {})
            
            return SyncConfig(
                default_sync_interval=settings.get('default_sync_interval', 300),
                max_batch_size=settings.get('max_batch_size', 2000),
                retry_attempts=settings.get('retry_attempts', 3),
                retry_delay=settings.get('retry_delay', 5),
                log_level=settings.get('log_level', 'INFO'),
                log_file=settings.get('log_file', 'logs/sync.log'),
                log_retention_days=settings.get('log_retention_days', 30)
            )
        except Exception as e:
            logger.warning(f"Failed to load sync settings, using defaults: {e}")
            return SyncConfig()
    
    def validate_config(self) -> bool:
        """Валидация конфигурации"""
        try:
            # Проверяем основные файлы
            if not self.table_mapping_file.exists():
                logger.error("Table mapping file missing")
                return False
            
            # Проверяем переменные окружения
            env_config = self.load_env_config()
            
            # Проверяем MySQL конфиг
            mysql_config = env_config['mysql']
            if not all([mysql_config.host, mysql_config.user, mysql_config.database]):
                logger.error("MySQL configuration incomplete")
                return False
            
            # Проверяем Google Sheets конфиг  
            sheets_config = env_config['google_sheets']
            if not all([sheets_config.credentials_file, sheets_config.spreadsheet_id]):
                logger.error("Google Sheets configuration incomplete")
                return False
            
            # Проверяем файл credentials или переменные окружения
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            credentials_base64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')
            
            if not credentials_base64 and not credentials_json and not Path(sheets_config.credentials_file).exists():
                logger.error(f"No Google credentials found. Set GOOGLE_CREDENTIALS_BASE64 or GOOGLE_CREDENTIALS_JSON environment variable, or provide file: {sheets_config.credentials_file}")
                return False
            
            # Проверяем маппинг таблиц
            table_mapping = self.load_table_mapping()
            if not table_mapping.get('tables'):
                logger.error("No tables configured for sync")
                return False
            
            logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False
    
    def create_sample_config(self):
        """Создание примера конфигурации"""
        sample_config = {
            'tables': {
                'example_table': {
                    'enabled': True,
                    'sheet_name': 'Example Sheet',
                    'sheet_range': 'A:E',
                    'sync_direction': 'both',
                    'batch_size': 1000,
                    'primary_key': 'id',
                    'unique_keys': ['id'],
                    'fields': {
                        'id': 'ID',
                        'name': 'Name',
                        'email': 'Email',
                        'created_at': 'Created'
                    }
                }
            },
            'sync_settings': {
                'default_sync_interval': 300,
                'max_batch_size': 2000,
                'retry_attempts': 3,
                'retry_delay': 5,
                'log_level': 'INFO',
                'log_file': 'logs/sync.log',
                'log_retention_days': 30
            }
        }
        
        sample_file = self.config_dir / "table_mapping.sample.yaml"
        with open(sample_file, 'w', encoding='utf-8') as f:
            yaml.dump(sample_config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Sample config created: {sample_file}")


# Глобальный экземпляр загрузчика
config_loader = ConfigLoader()
