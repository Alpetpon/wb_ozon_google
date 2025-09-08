-- --------------------------------------------------------
-- Хост:                         mysql.559b5f87f193.hosting.myjino.ru
-- Версия сервера:               10.11.13-MariaDB-cll-lve-log - MariaDB Server
-- Операционная система:         Linux
-- HeidiSQL Версия:              12.10.0.7000
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

-- Дамп структуры для таблица j01007093_ozon.bot_apikey_ozon
CREATE TABLE IF NOT EXISTS `bot_apikey_ozon` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(200) DEFAULT NULL,
  `client_id` varchar(200) NOT NULL DEFAULT '0',
  `oz_api` varchar(200) NOT NULL DEFAULT '0',
  `oz_perf` varchar(200) NOT NULL DEFAULT '0',
  `oz_secret` varchar(200) NOT NULL DEFAULT '0',
  `cabinet` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.bot_apikey_wb
CREATE TABLE IF NOT EXISTS `bot_apikey_wb` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(50) DEFAULT NULL,
  `apikey` varchar(1000) DEFAULT NULL,
  `cabinet` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.detaliz
CREATE TABLE IF NOT EXISTS `detaliz` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `c1` varchar(200) NOT NULL,
  `c2` varchar(200) NOT NULL,
  `c3` varchar(200) NOT NULL,
  `c4` varchar(200) NOT NULL,
  `c5` varchar(200) NOT NULL,
  `c6` varchar(200) NOT NULL,
  `c7` varchar(200) NOT NULL,
  `c8` varchar(200) NOT NULL,
  `c9` varchar(200) NOT NULL,
  `c10` varchar(200) NOT NULL,
  `c11` varchar(200) NOT NULL,
  `c12` varchar(200) NOT NULL,
  `c13` varchar(200) NOT NULL,
  `c14` varchar(200) NOT NULL,
  `c15` varchar(200) NOT NULL,
  `c16` varchar(200) NOT NULL,
  `c17` varchar(200) NOT NULL,
  `c18` varchar(200) NOT NULL,
  `c19` varchar(200) NOT NULL,
  `c20` varchar(200) NOT NULL,
  `c21` varchar(200) NOT NULL,
  `c22` varchar(200) NOT NULL,
  `c23` varchar(200) NOT NULL,
  `c24` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1397610 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.detaliz_wb
CREATE TABLE IF NOT EXISTS `detaliz_wb` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(50) DEFAULT NULL,
  `realizationreport_id` varchar(50) DEFAULT NULL,
  `date_from` varchar(50) DEFAULT NULL,
  `date_to` varchar(50) DEFAULT NULL,
  `create_dt` varchar(50) DEFAULT NULL,
  `currency_name` varchar(50) DEFAULT NULL,
  `suppliercontract_code` varchar(50) DEFAULT NULL,
  `rrd_id` varchar(100) DEFAULT NULL,
  `gi_id` varchar(100) DEFAULT NULL,
  `subject_name` varchar(100) DEFAULT NULL,
  `nm_id` varchar(100) DEFAULT NULL,
  `brand_name` varchar(100) DEFAULT NULL,
  `sa_name` varchar(100) DEFAULT NULL,
  `ts_name` varchar(100) DEFAULT NULL,
  `barcode` varchar(100) DEFAULT NULL,
  `doc_type_name` varchar(100) DEFAULT NULL,
  `quantity` varchar(100) DEFAULT NULL,
  `retail_price` varchar(100) DEFAULT NULL,
  `retail_amount` varchar(100) DEFAULT NULL,
  `sale_percent` varchar(100) DEFAULT NULL,
  `commission_percent` varchar(100) DEFAULT NULL,
  `office_name` varchar(100) DEFAULT NULL,
  `supplier_oper_name` varchar(100) DEFAULT NULL,
  `order_dt` varchar(100) DEFAULT NULL,
  `sale_dt` varchar(100) DEFAULT NULL,
  `rr_dt` varchar(100) DEFAULT NULL,
  `shk_id` varchar(100) DEFAULT NULL,
  `retail_price_withdisc_rub` varchar(100) DEFAULT NULL,
  `delivery_amount` varchar(100) DEFAULT NULL,
  `return_amount` varchar(100) DEFAULT NULL,
  `delivery_rub` varchar(100) DEFAULT NULL,
  `gi_box_type_name` varchar(100) DEFAULT NULL,
  `product_discount_for_report` varchar(100) DEFAULT NULL,
  `supplier_promo` varchar(100) DEFAULT NULL,
  `rid` varchar(100) DEFAULT NULL,
  `ppvz_spp_prc` varchar(100) DEFAULT NULL,
  `ppvz_kvw_prc_base` varchar(100) DEFAULT NULL,
  `ppvz_kvw_prc` varchar(100) DEFAULT NULL,
  `sup_rating_prc_up` varchar(100) DEFAULT NULL,
  `is_kgvp_v2` varchar(100) DEFAULT NULL,
  `ppvz_sales_commission` varchar(100) DEFAULT NULL,
  `ppvz_for_pay` varchar(100) DEFAULT NULL,
  `ppvz_reward` varchar(100) DEFAULT NULL,
  `acquiring_fee` varchar(100) DEFAULT NULL,
  `acquiring_bank` varchar(100) DEFAULT NULL,
  `ppvz_vw` varchar(100) DEFAULT NULL,
  `ppvz_vw_nds` varchar(100) DEFAULT NULL,
  `ppvz_office_id` varchar(100) DEFAULT NULL,
  `ppvz_office_name` varchar(100) DEFAULT NULL,
  `ppvz_supplier_id` varchar(100) DEFAULT NULL,
  `ppvz_supplier_name` varchar(100) DEFAULT NULL,
  `ppvz_inn` varchar(100) DEFAULT NULL,
  `declaration_number` varchar(100) DEFAULT NULL,
  `bonus_type_name` varchar(100) DEFAULT NULL,
  `sticker_id` varchar(100) DEFAULT NULL,
  `site_country` varchar(100) DEFAULT NULL,
  `penalty` varchar(100) DEFAULT NULL,
  `additional_payment` varchar(100) DEFAULT NULL,
  `rebill_logistic_cost` varchar(100) DEFAULT NULL,
  `rebill_logistic_org` varchar(100) DEFAULT NULL,
  `kiz` varchar(100) DEFAULT NULL,
  `storage_fee` varchar(100) DEFAULT NULL,
  `deduction` varchar(100) DEFAULT NULL,
  `acceptance` varchar(100) DEFAULT NULL,
  `srid` varchar(100) DEFAULT NULL,
  `report_type` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=365099 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.hranenie_wb
CREATE TABLE IF NOT EXISTS `hranenie_wb` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(50) DEFAULT NULL,
  `date` varchar(100) DEFAULT NULL,
  `logWarehouseCoef` varchar(100) DEFAULT NULL,
  `officeId` varchar(100) DEFAULT NULL,
  `warehouse` varchar(100) DEFAULT NULL,
  `warehouseCoef` varchar(100) DEFAULT NULL,
  `giId` varchar(100) DEFAULT NULL,
  `chrtId` varchar(100) DEFAULT NULL,
  `size` varchar(100) DEFAULT NULL,
  `barcode` varchar(100) DEFAULT NULL,
  `subject` varchar(100) DEFAULT NULL,
  `brand` varchar(100) DEFAULT NULL,
  `vendorCode` varchar(100) DEFAULT NULL,
  `nmId` varchar(100) DEFAULT NULL,
  `volume` varchar(100) DEFAULT NULL,
  `calcType` varchar(100) DEFAULT NULL,
  `warehousePrice` varchar(100) DEFAULT NULL,
  `barcodesCount` varchar(100) DEFAULT NULL,
  `palletPlaceCode` varchar(100) DEFAULT NULL,
  `palletCount` varchar(100) DEFAULT NULL,
  `originalDate` varchar(100) DEFAULT NULL,
  `loyaltyDiscount` varchar(100) DEFAULT NULL,
  `tariffFixDate` varchar(100) DEFAULT NULL,
  `tariffLowerDate` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=369642 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.list_rk
CREATE TABLE IF NOT EXISTS `list_rk` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `idc` varchar(200) NOT NULL,
  `title` varchar(200) NOT NULL,
  `fromDate` varchar(200) NOT NULL,
  `toDate` varchar(200) NOT NULL,
  `advObjectType` varchar(200) NOT NULL,
  `state` varchar(200) NOT NULL,
  `refresh` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=296114 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.prices
CREATE TABLE IF NOT EXISTS `prices` (
  `id` int(7) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(100) NOT NULL,
  `dat` varchar(15) NOT NULL,
  `sku` varchar(200) NOT NULL,
  `offer_id` varchar(200) NOT NULL,
  `title` varchar(200) NOT NULL,
  `barcode` varchar(200) NOT NULL,
  `old_price` varchar(200) NOT NULL,
  `min_price` varchar(200) NOT NULL,
  `marketing_price` varchar(200) NOT NULL,
  `price` varchar(200) NOT NULL,
  `marketing_seller_price` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=353862 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.price_wb
CREATE TABLE IF NOT EXISTS `price_wb` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) DEFAULT NULL,
  `dF` varchar(200) DEFAULT NULL,
  `artWB` varchar(200) DEFAULT NULL,
  `article_size` varchar(200) DEFAULT NULL,
  `item_name` varchar(200) DEFAULT NULL,
  `vendCode` varchar(200) DEFAULT NULL,
  `price` varchar(200) DEFAULT NULL,
  `discount` varchar(200) DEFAULT NULL,
  `price_with_discount` varchar(200) DEFAULT NULL,
  `price_with_spp` varchar(200) DEFAULT NULL,
  `spp` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1198 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.priem_wb
CREATE TABLE IF NOT EXISTS `priem_wb` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) DEFAULT NULL,
  `dF` varchar(200) DEFAULT NULL,
  `count` varchar(200) DEFAULT NULL,
  `giCreateDate` varchar(200) DEFAULT NULL,
  `incomeId` varchar(200) DEFAULT NULL,
  `nmID` varchar(200) DEFAULT NULL,
  `shkСreateDate` varchar(200) DEFAULT NULL,
  `subjectName` varchar(200) DEFAULT NULL,
  `total` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.rekl_wb
CREATE TABLE IF NOT EXISTS `rekl_wb` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) DEFAULT NULL,
  `dF` varchar(200) DEFAULT NULL,
  `advertId` varchar(200) DEFAULT NULL,
  `adname` varchar(200) DEFAULT NULL,
  `typid` varchar(200) DEFAULT NULL,
  `appType` varchar(200) DEFAULT NULL,
  `nmId` varchar(200) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `views` varchar(200) DEFAULT NULL,
  `clicks` varchar(200) DEFAULT NULL,
  `ctr` varchar(200) DEFAULT NULL,
  `cpc` varchar(200) DEFAULT NULL,
  `sum` varchar(200) DEFAULT NULL,
  `atbs` varchar(200) DEFAULT NULL,
  `orders` varchar(200) DEFAULT NULL,
  `cr` varchar(50) DEFAULT NULL,
  `shks` varchar(50) DEFAULT NULL,
  `sum_price` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=107168 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.report_fbo
CREATE TABLE IF NOT EXISTS `report_fbo` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `order_id` varchar(200) NOT NULL,
  `order_number` varchar(200) NOT NULL,
  `posting_number` varchar(200) NOT NULL,
  `status` varchar(200) NOT NULL,
  `cancel_reason_id` varchar(200) NOT NULL,
  `created_at` varchar(200) NOT NULL,
  `in_process_at` varchar(200) NOT NULL,
  `sku` varchar(200) NOT NULL,
  `name` varchar(200) NOT NULL,
  `quantity` varchar(200) NOT NULL,
  `price` varchar(200) NOT NULL,
  `cluster_from` varchar(200) NOT NULL,
  `cluster_to` varchar(200) NOT NULL,
  `delivery_type` varchar(200) NOT NULL,
  `is_premium` varchar(200) NOT NULL,
  `payment_type_group_name` varchar(200) NOT NULL,
  `warehouse_id` varchar(200) NOT NULL,
  `warehouse_name` varchar(200) NOT NULL,
  `is_legal` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=534574 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.report_fbs
CREATE TABLE IF NOT EXISTS `report_fbs` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `order_id` varchar(200) NOT NULL,
  `posting_number` varchar(200) NOT NULL,
  `order_number` varchar(200) NOT NULL,
  `in_process_at` varchar(200) NOT NULL,
  `shipment_date` varchar(200) NOT NULL,
  `delivering_date` varchar(200) NOT NULL,
  `delivery_date_begin` varchar(200) NOT NULL,
  `delivery_date_end` varchar(200) NOT NULL,
  `status` varchar(200) NOT NULL,
  `sku` varchar(200) NOT NULL,
  `offer_id` varchar(200) NOT NULL,
  `name` varchar(200) NOT NULL,
  `quantity` varchar(200) NOT NULL,
  `old_price` varchar(200) NOT NULL,
  `price` varchar(200) NOT NULL,
  `total_discount_value` varchar(200) NOT NULL,
  `total_discount_percent` varchar(200) NOT NULL,
  `delivery_method_name` varchar(200) NOT NULL,
  `warehouse_name` varchar(200) NOT NULL,
  `tpl_provider` varchar(200) NOT NULL,
  `tracking_number` varchar(200) NOT NULL,
  `tpl_integration_type` varchar(200) NOT NULL,
  `region_post_rfbs` varchar(200) NOT NULL,
  `city_post_rfbs` varchar(200) NOT NULL,
  `cluster_from` varchar(200) NOT NULL,
  `cluster_to` varchar(200) NOT NULL,
  `substatus` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3087 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.stat_rk
CREATE TABLE IF NOT EXISTS `stat_rk` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `id_rk` varchar(200) NOT NULL,
  `type_rekl` varchar(200) NOT NULL,
  `title_rk` varchar(200) NOT NULL,
  `date` varchar(200) NOT NULL,
  `sku` varchar(200) NOT NULL,
  `views` varchar(200) NOT NULL,
  `clicks` varchar(200) NOT NULL,
  `ctr` varchar(200) NOT NULL,
  `moneySpent` varchar(200) NOT NULL,
  `avgBid` varchar(200) NOT NULL,
  `orders` varchar(200) NOT NULL,
  `ordersMoney` varchar(200) NOT NULL,
  `models` varchar(200) NOT NULL,
  `modelsMoney` varchar(200) NOT NULL,
  `title_t` varchar(200) NOT NULL,
  `price` varchar(200) NOT NULL,
  `toCart` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=113629 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.stocks
CREATE TABLE IF NOT EXISTS `stocks` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `dat` varchar(100) NOT NULL,
  `sku` varchar(100) NOT NULL,
  `item_code` varchar(100) NOT NULL,
  `item_name` varchar(100) NOT NULL,
  `free_to_sell_amount` varchar(100) NOT NULL,
  `promised_amount` varchar(100) NOT NULL,
  `reserved_amount` varchar(100) NOT NULL,
  `warehouse_name` varchar(100) NOT NULL,
  `idc` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=774659 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.stock_wb
CREATE TABLE IF NOT EXISTS `stock_wb` (
  `id` int(100) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) DEFAULT NULL,
  `dF` varchar(200) DEFAULT NULL,
  `nmId` varchar(200) DEFAULT NULL,
  `barcode` varchar(200) DEFAULT NULL,
  `techSize` varchar(200) DEFAULT NULL,
  `warehouseName` varchar(200) DEFAULT NULL,
  `quantity` varchar(200) DEFAULT NULL,
  `inWayToClient` varchar(200) DEFAULT NULL,
  `inWayFromClient` varchar(200) DEFAULT NULL,
  `quantityFull` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=83911 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.token_rekl
CREATE TABLE IF NOT EXISTS `token_rekl` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `token` varchar(1000) NOT NULL,
  `endtime` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=32107 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.tovar
CREATE TABLE IF NOT EXISTS `tovar` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `sku` varchar(50) NOT NULL,
  `offer_id` varchar(200) NOT NULL,
  `title` varchar(200) NOT NULL,
  `barcode` varchar(200) NOT NULL,
  `old_price` varchar(200) NOT NULL,
  `min_price` varchar(200) NOT NULL,
  `marketing_price` varchar(200) NOT NULL,
  `price` varchar(200) NOT NULL,
  `marketing_seller_price` varchar(200) NOT NULL,
  `volume_weight` varchar(200) NOT NULL,
  `height` varchar(200) NOT NULL,
  `depth` varchar(200) NOT NULL,
  `width` varchar(200) NOT NULL,
  `weight` varchar(200) NOT NULL,
  `vol` varchar(200) NOT NULL,
  `img` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=343246 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.tovar_wb
CREATE TABLE IF NOT EXISTS `tovar_wb` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(50) DEFAULT NULL,
  `artWB` varchar(200) DEFAULT NULL,
  `vendorCode` varchar(200) DEFAULT NULL,
  `subjectName` varchar(200) DEFAULT NULL,
  `brand` varchar(200) DEFAULT NULL,
  `title` varchar(200) DEFAULT NULL,
  `video` varchar(200) DEFAULT NULL,
  `description` varchar(2000) DEFAULT NULL,
  `photo` varchar(200) DEFAULT NULL,
  `width` varchar(200) DEFAULT NULL,
  `height` varchar(200) DEFAULT NULL,
  `length` varchar(200) DEFAULT NULL,
  `weightBrutto` varchar(200) DEFAULT NULL,
  `barcode` varchar(200) DEFAULT NULL,
  `techSize` varchar(200) DEFAULT NULL,
  `wbSize` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13932 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.voronka
CREATE TABLE IF NOT EXISTS `voronka` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `dat` varchar(200) NOT NULL,
  `sku` varchar(200) NOT NULL,
  `nam` varchar(200) NOT NULL,
  `empt` varchar(200) NOT NULL,
  `ordered_units` varchar(200) NOT NULL,
  `revenue` varchar(200) NOT NULL,
  `session_view` varchar(200) NOT NULL,
  `session_view_pdp` varchar(200) NOT NULL,
  `conv_tocart_pdp` varchar(200) NOT NULL,
  `cancellations` varchar(200) NOT NULL,
  `returns` varchar(200) NOT NULL,
  `position_category` varchar(200) NOT NULL,
  `hits_view_search` varchar(200) NOT NULL,
  `session_view_search` varchar(200) NOT NULL,
  `conv_tocart_search` varchar(200) NOT NULL,
  `conv_tocart` varchar(200) NOT NULL,
  `hits_tocart_search` varchar(200) NOT NULL,
  `hits_view` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=224212 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.voronka_wb
CREATE TABLE IF NOT EXISTS `voronka_wb` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(50) NOT NULL DEFAULT '0',
  `dF` varchar(200) NOT NULL DEFAULT '0',
  `nmID` varchar(200) NOT NULL DEFAULT '0',
  `vendorCode` varchar(200) NOT NULL DEFAULT '0',
  `brandName` varchar(200) NOT NULL DEFAULT '0',
  `object_id` varchar(200) NOT NULL DEFAULT '0',
  `object_name` varchar(200) NOT NULL DEFAULT '0',
  `openCardCount` varchar(200) NOT NULL DEFAULT '0',
  `addToCartCount` varchar(200) NOT NULL DEFAULT '0',
  `ordersCount` varchar(200) NOT NULL DEFAULT '0',
  `buyoutsCount` varchar(200) NOT NULL DEFAULT '0',
  `cancelCount` varchar(200) NOT NULL DEFAULT '0',
  `ordersSumRub` varchar(200) NOT NULL DEFAULT '0',
  `buyoutsSumRub` varchar(200) NOT NULL DEFAULT '0',
  `cancelSumRub` varchar(200) NOT NULL DEFAULT '0',
  `stocksWb` varchar(200) NOT NULL DEFAULT '0',
  `stocksMp` varchar(200) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=67988 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.workdata
CREATE TABLE IF NOT EXISTS `workdata` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `data_type` varchar(200) NOT NULL,
  `data_values` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=144945 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.zakaz_fbo
CREATE TABLE IF NOT EXISTS `zakaz_fbo` (
  `id` int(100) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `c0` int(11) NOT NULL,
  `c1` varchar(200) NOT NULL,
  `c2` varchar(200) NOT NULL,
  `c3` varchar(200) NOT NULL,
  `c4` varchar(200) NOT NULL,
  `c5` varchar(200) NOT NULL,
  `c6` varchar(200) NOT NULL,
  `c7` varchar(200) NOT NULL,
  `c8` varchar(200) NOT NULL,
  `c9` varchar(200) NOT NULL,
  `c10` varchar(200) NOT NULL,
  `c11` varchar(200) NOT NULL,
  `c12` varchar(200) NOT NULL,
  `c13` varchar(200) NOT NULL,
  `c14` varchar(200) NOT NULL,
  `c15` varchar(200) NOT NULL,
  `c16` varchar(200) NOT NULL,
  `c17` varchar(200) NOT NULL,
  `c18` varchar(200) NOT NULL,
  `c19` varchar(200) NOT NULL,
  `c20` varchar(200) NOT NULL,
  `c21` varchar(200) NOT NULL,
  `c22` varchar(200) NOT NULL,
  `c23` varchar(200) NOT NULL,
  `c24` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=417475 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

-- Дамп структуры для таблица j01007093_ozon.zakaz_fbs
CREATE TABLE IF NOT EXISTS `zakaz_fbs` (
  `id` int(100) NOT NULL AUTO_INCREMENT,
  `cabinet` varchar(200) NOT NULL,
  `c0` varchar(200) NOT NULL,
  `c1` varchar(200) NOT NULL,
  `c2` varchar(200) NOT NULL,
  `c3` varchar(200) NOT NULL,
  `c4` varchar(200) NOT NULL,
  `c5` varchar(200) NOT NULL,
  `c6` varchar(200) NOT NULL,
  `c7` varchar(200) NOT NULL,
  `c8` varchar(200) NOT NULL,
  `c9` varchar(200) NOT NULL,
  `c10` varchar(200) NOT NULL,
  `c11` varchar(200) NOT NULL,
  `c12` varchar(200) NOT NULL,
  `c13` varchar(200) NOT NULL,
  `c14` varchar(200) NOT NULL,
  `c15` varchar(200) NOT NULL,
  `c16` varchar(200) NOT NULL,
  `c17` varchar(200) NOT NULL,
  `c18` varchar(200) NOT NULL,
  `c19` varchar(200) NOT NULL,
  `c20` varchar(200) NOT NULL,
  `c21` varchar(200) NOT NULL,
  `c22` varchar(200) NOT NULL,
  `c23` varchar(200) NOT NULL,
  `c24` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5579 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Экспортируемые данные не выделены.

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
