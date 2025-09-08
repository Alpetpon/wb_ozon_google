-- Скрипт создания триггеров для всех отслеживаемых таблиц
-- Триггеры будут записывать изменения в таблицу change_log

-- Сначала удаляем существующие триггеры если они есть
DROP TRIGGER IF EXISTS `tr_tovar_insert`;
DROP TRIGGER IF EXISTS `tr_tovar_update`;
DROP TRIGGER IF EXISTS `tr_tovar_delete`;

DROP TRIGGER IF EXISTS `tr_tovar_wb_insert`;
DROP TRIGGER IF EXISTS `tr_tovar_wb_update`;
DROP TRIGGER IF EXISTS `tr_tovar_wb_delete`;

DROP TRIGGER IF EXISTS `tr_stocks_insert`;
DROP TRIGGER IF EXISTS `tr_stocks_update`;
DROP TRIGGER IF EXISTS `tr_stocks_delete`;

DROP TRIGGER IF EXISTS `tr_stock_wb_insert`;
DROP TRIGGER IF EXISTS `tr_stock_wb_update`;
DROP TRIGGER IF EXISTS `tr_stock_wb_delete`;

DROP TRIGGER IF EXISTS `tr_prices_insert`;
DROP TRIGGER IF EXISTS `tr_prices_update`;
DROP TRIGGER IF EXISTS `tr_prices_delete`;

DROP TRIGGER IF EXISTS `tr_report_fbo_insert`;
DROP TRIGGER IF EXISTS `tr_report_fbo_update`;
DROP TRIGGER IF EXISTS `tr_report_fbo_delete`;

-- ========================================
-- ТРИГГЕРЫ ДЛЯ ТАБЛИЦЫ tovar (OZON товары)
-- ========================================

DELIMITER $$

CREATE TRIGGER `tr_tovar_insert`
  AFTER INSERT ON `tovar`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `new_data`, `cabinet`
  ) VALUES (
    'tovar', 'INSERT', NEW.id,
    JSON_OBJECT(
      'id', NEW.id,
      'cabinet', NEW.cabinet,
      'sku', NEW.sku,
      'offer_id', NEW.offer_id,
      'title', NEW.title,
      'barcode', NEW.barcode,
      'old_price', NEW.old_price,
      'min_price', NEW.min_price,
      'marketing_price', NEW.marketing_price,
      'price', NEW.price,
      'marketing_seller_price', NEW.marketing_seller_price,
      'volume_weight', NEW.volume_weight,
      'height', NEW.height,
      'depth', NEW.depth,
      'width', NEW.width,
      'weight', NEW.weight,
      'vol', NEW.vol,
      'img', NEW.img
    ),
    NEW.cabinet
  );
END$$

CREATE TRIGGER `tr_tovar_update`
  AFTER UPDATE ON `tovar`
  FOR EACH ROW
BEGIN
  DECLARE changed_fields JSON DEFAULT JSON_ARRAY();
  
  -- Проверяем какие поля изменились
  IF OLD.cabinet != NEW.cabinet THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'cabinet'); END IF;
  IF OLD.sku != NEW.sku THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'sku'); END IF;
  IF OLD.offer_id != NEW.offer_id THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'offer_id'); END IF;
  IF OLD.title != NEW.title THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'title'); END IF;
  IF OLD.barcode != NEW.barcode THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'barcode'); END IF;
  IF OLD.old_price != NEW.old_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'old_price'); END IF;
  IF OLD.min_price != NEW.min_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'min_price'); END IF;
  IF OLD.marketing_price != NEW.marketing_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'marketing_price'); END IF;
  IF OLD.price != NEW.price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'price'); END IF;
  IF OLD.marketing_seller_price != NEW.marketing_seller_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'marketing_seller_price'); END IF;
  IF OLD.volume_weight != NEW.volume_weight THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'volume_weight'); END IF;
  IF OLD.height != NEW.height THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'height'); END IF;
  IF OLD.depth != NEW.depth THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'depth'); END IF;
  IF OLD.width != NEW.width THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'width'); END IF;
  IF OLD.weight != NEW.weight THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'weight'); END IF;
  IF OLD.vol != NEW.vol THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'vol'); END IF;
  IF OLD.img != NEW.img THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'img'); END IF;
  
  -- Записываем изменение только если что-то действительно изменилось
  IF JSON_LENGTH(changed_fields) > 0 THEN
    INSERT INTO `change_log` (
      `table_name`, `operation_type`, `record_id`, `old_data`, `new_data`, `changed_fields`, `cabinet`
    ) VALUES (
      'tovar', 'UPDATE', NEW.id,
      JSON_OBJECT(
        'id', OLD.id, 'cabinet', OLD.cabinet, 'sku', OLD.sku, 'offer_id', OLD.offer_id,
        'title', OLD.title, 'barcode', OLD.barcode, 'old_price', OLD.old_price,
        'min_price', OLD.min_price, 'marketing_price', OLD.marketing_price,
        'price', OLD.price, 'marketing_seller_price', OLD.marketing_seller_price,
        'volume_weight', OLD.volume_weight, 'height', OLD.height, 'depth', OLD.depth,
        'width', OLD.width, 'weight', OLD.weight, 'vol', OLD.vol, 'img', OLD.img
      ),
      JSON_OBJECT(
        'id', NEW.id, 'cabinet', NEW.cabinet, 'sku', NEW.sku, 'offer_id', NEW.offer_id,
        'title', NEW.title, 'barcode', NEW.barcode, 'old_price', NEW.old_price,
        'min_price', NEW.min_price, 'marketing_price', NEW.marketing_price,
        'price', NEW.price, 'marketing_seller_price', NEW.marketing_seller_price,
        'volume_weight', NEW.volume_weight, 'height', NEW.height, 'depth', NEW.depth,
        'width', NEW.width, 'weight', NEW.weight, 'vol', NEW.vol, 'img', NEW.img
      ),
      changed_fields,
      NEW.cabinet
    );
  END IF;
END$$

CREATE TRIGGER `tr_tovar_delete`
  AFTER DELETE ON `tovar`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `old_data`, `cabinet`
  ) VALUES (
    'tovar', 'DELETE', OLD.id,
    JSON_OBJECT(
      'id', OLD.id, 'cabinet', OLD.cabinet, 'sku', OLD.sku, 'offer_id', OLD.offer_id,
      'title', OLD.title, 'barcode', OLD.barcode, 'old_price', OLD.old_price,
      'min_price', OLD.min_price, 'marketing_price', OLD.marketing_price,
      'price', OLD.price, 'marketing_seller_price', OLD.marketing_seller_price,
      'volume_weight', OLD.volume_weight, 'height', OLD.height, 'depth', OLD.depth,
      'width', OLD.width, 'weight', OLD.weight, 'vol', OLD.vol, 'img', OLD.img
    ),
    OLD.cabinet
  );
END$$

-- ========================================
-- ТРИГГЕРЫ ДЛЯ ТАБЛИЦЫ tovar_wb (WB товары)
-- ========================================

CREATE TRIGGER `tr_tovar_wb_insert`
  AFTER INSERT ON `tovar_wb`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `new_data`, `cabinet`
  ) VALUES (
    'tovar_wb', 'INSERT', NEW.id,
    JSON_OBJECT(
      'id', NEW.id, 'cabinet', NEW.cabinet, 'artWB', NEW.artWB,
      'vendorCode', NEW.vendorCode, 'subjectName', NEW.subjectName,
      'brand', NEW.brand, 'title', NEW.title, 'video', NEW.video,
      'description', NEW.description, 'photo', NEW.photo,
      'width', NEW.width, 'height', NEW.height, 'length', NEW.length,
      'weightBrutto', NEW.weightBrutto, 'barcode', NEW.barcode,
      'techSize', NEW.techSize, 'wbSize', NEW.wbSize
    ),
    NEW.cabinet
  );
END$$

CREATE TRIGGER `tr_tovar_wb_update`
  AFTER UPDATE ON `tovar_wb`
  FOR EACH ROW
BEGIN
  DECLARE changed_fields JSON DEFAULT JSON_ARRAY();
  
  IF OLD.cabinet != NEW.cabinet THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'cabinet'); END IF;
  IF OLD.artWB != NEW.artWB THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'artWB'); END IF;
  IF OLD.vendorCode != NEW.vendorCode THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'vendorCode'); END IF;
  IF OLD.subjectName != NEW.subjectName THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'subjectName'); END IF;
  IF OLD.brand != NEW.brand THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'brand'); END IF;
  IF OLD.title != NEW.title THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'title'); END IF;
  IF OLD.video != NEW.video THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'video'); END IF;
  IF OLD.description != NEW.description THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'description'); END IF;
  IF OLD.photo != NEW.photo THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'photo'); END IF;
  IF OLD.width != NEW.width THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'width'); END IF;
  IF OLD.height != NEW.height THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'height'); END IF;
  IF OLD.length != NEW.length THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'length'); END IF;
  IF OLD.weightBrutto != NEW.weightBrutto THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'weightBrutto'); END IF;
  IF OLD.barcode != NEW.barcode THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'barcode'); END IF;
  IF OLD.techSize != NEW.techSize THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'techSize'); END IF;
  IF OLD.wbSize != NEW.wbSize THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'wbSize'); END IF;
  
  IF JSON_LENGTH(changed_fields) > 0 THEN
    INSERT INTO `change_log` (
      `table_name`, `operation_type`, `record_id`, `old_data`, `new_data`, `changed_fields`, `cabinet`
    ) VALUES (
      'tovar_wb', 'UPDATE', NEW.id,
      JSON_OBJECT(
        'id', OLD.id, 'cabinet', OLD.cabinet, 'artWB', OLD.artWB,
        'vendorCode', OLD.vendorCode, 'subjectName', OLD.subjectName,
        'brand', OLD.brand, 'title', OLD.title, 'video', OLD.video,
        'description', OLD.description, 'photo', OLD.photo,
        'width', OLD.width, 'height', OLD.height, 'length', OLD.length,
        'weightBrutto', OLD.weightBrutto, 'barcode', OLD.barcode,
        'techSize', OLD.techSize, 'wbSize', OLD.wbSize
      ),
      JSON_OBJECT(
        'id', NEW.id, 'cabinet', NEW.cabinet, 'artWB', NEW.artWB,
        'vendorCode', NEW.vendorCode, 'subjectName', NEW.subjectName,
        'brand', NEW.brand, 'title', NEW.title, 'video', NEW.video,
        'description', NEW.description, 'photo', NEW.photo,
        'width', NEW.width, 'height', NEW.height, 'length', NEW.length,
        'weightBrutto', NEW.weightBrutto, 'barcode', NEW.barcode,
        'techSize', NEW.techSize, 'wbSize', NEW.wbSize
      ),
      changed_fields,
      NEW.cabinet
    );
  END IF;
END$$

CREATE TRIGGER `tr_tovar_wb_delete`
  AFTER DELETE ON `tovar_wb`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `old_data`, `cabinet`
  ) VALUES (
    'tovar_wb', 'DELETE', OLD.id,
    JSON_OBJECT(
      'id', OLD.id, 'cabinet', OLD.cabinet, 'artWB', OLD.artWB,
      'vendorCode', OLD.vendorCode, 'subjectName', OLD.subjectName,
      'brand', OLD.brand, 'title', OLD.title, 'video', OLD.video,
      'description', OLD.description, 'photo', OLD.photo,
      'width', OLD.width, 'height', OLD.height, 'length', OLD.length,
      'weightBrutto', OLD.weightBrutto, 'barcode', OLD.barcode,
      'techSize', OLD.techSize, 'wbSize', OLD.wbSize
    ),
    OLD.cabinet
  );
END$$

-- ========================================
-- ТРИГГЕРЫ ДЛЯ ТАБЛИЦЫ stocks (остатки OZON)
-- ========================================

CREATE TRIGGER `tr_stocks_insert`
  AFTER INSERT ON `stocks`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `new_data`, `cabinet`
  ) VALUES (
    'stocks', 'INSERT', NEW.id,
    JSON_OBJECT(
      'id', NEW.id, 'cabinet', NEW.cabinet, 'dat', NEW.dat,
      'sku', NEW.sku, 'item_code', NEW.item_code, 'item_name', NEW.item_name,
      'free_to_sell_amount', NEW.free_to_sell_amount, 'promised_amount', NEW.promised_amount,
      'reserved_amount', NEW.reserved_amount, 'warehouse_name', NEW.warehouse_name,
      'idc', NEW.idc
    ),
    NEW.cabinet
  );
END$$

CREATE TRIGGER `tr_stocks_update`
  AFTER UPDATE ON `stocks`
  FOR EACH ROW
BEGIN
  DECLARE changed_fields JSON DEFAULT JSON_ARRAY();
  
  IF OLD.cabinet != NEW.cabinet THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'cabinet'); END IF;
  IF OLD.dat != NEW.dat THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'dat'); END IF;
  IF OLD.sku != NEW.sku THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'sku'); END IF;
  IF OLD.item_code != NEW.item_code THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'item_code'); END IF;
  IF OLD.item_name != NEW.item_name THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'item_name'); END IF;
  IF OLD.free_to_sell_amount != NEW.free_to_sell_amount THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'free_to_sell_amount'); END IF;
  IF OLD.promised_amount != NEW.promised_amount THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'promised_amount'); END IF;
  IF OLD.reserved_amount != NEW.reserved_amount THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'reserved_amount'); END IF;
  IF OLD.warehouse_name != NEW.warehouse_name THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'warehouse_name'); END IF;
  IF OLD.idc != NEW.idc THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'idc'); END IF;
  
  IF JSON_LENGTH(changed_fields) > 0 THEN
    INSERT INTO `change_log` (
      `table_name`, `operation_type`, `record_id`, `old_data`, `new_data`, `changed_fields`, `cabinet`
    ) VALUES (
      'stocks', 'UPDATE', NEW.id,
      JSON_OBJECT(
        'id', OLD.id, 'cabinet', OLD.cabinet, 'dat', OLD.dat,
        'sku', OLD.sku, 'item_code', OLD.item_code, 'item_name', OLD.item_name,
        'free_to_sell_amount', OLD.free_to_sell_amount, 'promised_amount', OLD.promised_amount,
        'reserved_amount', OLD.reserved_amount, 'warehouse_name', OLD.warehouse_name,
        'idc', OLD.idc
      ),
      JSON_OBJECT(
        'id', NEW.id, 'cabinet', NEW.cabinet, 'dat', NEW.dat,
        'sku', NEW.sku, 'item_code', NEW.item_code, 'item_name', NEW.item_name,
        'free_to_sell_amount', NEW.free_to_sell_amount, 'promised_amount', NEW.promised_amount,
        'reserved_amount', NEW.reserved_amount, 'warehouse_name', NEW.warehouse_name,
        'idc', NEW.idc
      ),
      changed_fields,
      NEW.cabinet
    );
  END IF;
END$$

CREATE TRIGGER `tr_stocks_delete`
  AFTER DELETE ON `stocks`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `old_data`, `cabinet`
  ) VALUES (
    'stocks', 'DELETE', OLD.id,
    JSON_OBJECT(
      'id', OLD.id, 'cabinet', OLD.cabinet, 'dat', OLD.dat,
      'sku', OLD.sku, 'item_code', OLD.item_code, 'item_name', OLD.item_name,
      'free_to_sell_amount', OLD.free_to_sell_amount, 'promised_amount', OLD.promised_amount,
      'reserved_amount', OLD.reserved_amount, 'warehouse_name', OLD.warehouse_name,
      'idc', OLD.idc
    ),
    OLD.cabinet
  );
END$$

-- ========================================
-- ТРИГГЕРЫ ДЛЯ ТАБЛИЦЫ prices (цены OZON)
-- ========================================

CREATE TRIGGER `tr_prices_insert`
  AFTER INSERT ON `prices`
  FOR EACH ROW
BEGIN
  INSERT INTO `change_log` (
    `table_name`, `operation_type`, `record_id`, `new_data`, `cabinet`
  ) VALUES (
    'prices', 'INSERT', NEW.id,
    JSON_OBJECT(
      'id', NEW.id, 'cabinet', NEW.cabinet, 'dat', NEW.dat,
      'sku', NEW.sku, 'offer_id', NEW.offer_id, 'title', NEW.title,
      'barcode', NEW.barcode, 'old_price', NEW.old_price, 'min_price', NEW.min_price,
      'marketing_price', NEW.marketing_price, 'price', NEW.price,
      'marketing_seller_price', NEW.marketing_seller_price
    ),
    NEW.cabinet
  );
END$$

CREATE TRIGGER `tr_prices_update`
  AFTER UPDATE ON `prices`
  FOR EACH ROW
BEGIN
  DECLARE changed_fields JSON DEFAULT JSON_ARRAY();
  
  IF OLD.cabinet != NEW.cabinet THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'cabinet'); END IF;
  IF OLD.dat != NEW.dat THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'dat'); END IF;
  IF OLD.sku != NEW.sku THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'sku'); END IF;
  IF OLD.offer_id != NEW.offer_id THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'offer_id'); END IF;
  IF OLD.title != NEW.title THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'title'); END IF;
  IF OLD.barcode != NEW.barcode THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'barcode'); END IF;
  IF OLD.old_price != NEW.old_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'old_price'); END IF;
  IF OLD.min_price != NEW.min_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'min_price'); END IF;
  IF OLD.marketing_price != NEW.marketing_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'marketing_price'); END IF;
  IF OLD.price != NEW.price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'price'); END IF;
  IF OLD.marketing_seller_price != NEW.marketing_seller_price THEN SET changed_fields = JSON_ARRAY_APPEND(changed_fields, '$', 'marketing_seller_price'); END IF;
  
  IF JSON_LENGTH(changed_fields) > 0 THEN
    INSERT INTO `change_log` (
      `table_name`, `operation_type`, `record_id`, `old_data`, `new_data`, `changed_fields`, `cabinet`
    ) VALUES (
      'prices', 'UPDATE', NEW.id,
      JSON_OBJECT(
        'id', OLD.id, 'cabinet', OLD.cabinet, 'dat', OLD.dat,
        'sku', OLD.sku, 'offer_id', OLD.offer_id, 'title', OLD.title,
        'barcode', OLD.barcode, 'old_price', OLD.old_price, 'min_price', OLD.min_price,
        'marketing_price', OLD.marketing_price, 'price', OLD.price,
        'marketing_seller_price', OLD.marketing_seller_price
      ),
      JSON_OBJECT(
        'id', NEW.id, 'cabinet', NEW.cabinet, 'dat', NEW.dat,
        'sku', NEW.sku, 'offer_id', NEW.offer_id, 'title', NEW.title,
        'barcode', NEW.barcode, 'old_price', NEW.old_price, 'min_price', NEW.min_price,
        'marketing_price', NEW.marketing_price, 'price', NEW.price,
        'marketing_seller_price', NEW.marketing_seller_price
      ),
      changed_fields,
      NEW.cabinet
    );
  END IF;
END$$

DELIMITER ;

-- Проверка созданных триггеров
SELECT 
    TRIGGER_NAME,
    EVENT_MANIPULATION,
    EVENT_OBJECT_TABLE,
    TRIGGER_SCHEMA
FROM information_schema.TRIGGERS 
WHERE TRIGGER_SCHEMA = DATABASE()
ORDER BY EVENT_OBJECT_TABLE, EVENT_MANIPULATION;
