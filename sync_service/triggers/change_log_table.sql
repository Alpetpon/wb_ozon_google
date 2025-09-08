-- Создание таблицы change_log для отслеживания изменений
-- Эта таблица будет хранить информацию о всех изменениях в отслеживаемых таблицах

CREATE TABLE IF NOT EXISTS `change_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `table_name` varchar(64) NOT NULL COMMENT 'Название таблицы',
  `operation_type` enum('INSERT','UPDATE','DELETE') NOT NULL COMMENT 'Тип операции',
  `record_id` varchar(100) NOT NULL COMMENT 'ID записи (primary key)',
  `old_data` json DEFAULT NULL COMMENT 'Старые данные (для UPDATE и DELETE)',
  `new_data` json DEFAULT NULL COMMENT 'Новые данные (для INSERT и UPDATE)',
  `changed_fields` json DEFAULT NULL COMMENT 'Список изменившихся полей',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Время изменения',
  `processed_at` timestamp NULL DEFAULT NULL COMMENT 'Время обработки изменения',
  `sync_status` enum('PENDING','PROCESSING','COMPLETED','FAILED') NOT NULL DEFAULT 'PENDING' COMMENT 'Статус синхронизации',
  `error_message` text DEFAULT NULL COMMENT 'Сообщение об ошибке',
  `retry_count` int(11) NOT NULL DEFAULT 0 COMMENT 'Количество попыток',
  `cabinet` varchar(200) DEFAULT NULL COMMENT 'Кабинет (для фильтрации)',
  PRIMARY KEY (`id`),
  KEY `idx_table_status` (`table_name`, `sync_status`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_record_id` (`record_id`),
  KEY `idx_cabinet` (`cabinet`),
  KEY `idx_pending` (`sync_status`, `created_at`) -- Для быстрого поиска pending записей
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Журнал изменений для синхронизации';

-- Индекс для очистки старых записей
CREATE INDEX `idx_processed_at` ON `change_log` (`processed_at`);
