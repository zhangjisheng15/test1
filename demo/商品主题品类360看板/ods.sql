
create database gd_04;


use gd_04;


-- 创建ODS层用户信息全量表
DROP TABLE IF EXISTS ods_user_info_full;
CREATE EXTERNAL TABLE ods_user_info_full (
                                             user_id INT COMMENT '用户ID',
                                             user_name STRING COMMENT '用户姓名',
                                             gender STRING COMMENT '性别',
                                             age INT COMMENT '年龄',
                                             register_time STRING COMMENT '注册时间'
) COMMENT 'ODS层用户信息全量表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/user_info_full/';

-- 创建ODS层商品信息全量表
DROP TABLE IF EXISTS ods_product_info_full;
CREATE EXTERNAL TABLE ods_product_info_full (
                                                product_id INT COMMENT '商品ID',
                                                product_name STRING COMMENT '商品名称',
                                                product_category STRING COMMENT '商品类别',
                                                price DECIMAL(10,2) COMMENT '商品价格',
                                                stock INT COMMENT '商品库存'
) COMMENT 'ODS层商品信息全量表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/product_info_full/';

-- 创建ODS层订单增量表
DROP TABLE IF EXISTS ods_order_info_inc;
CREATE EXTERNAL TABLE ods_order_info_inc (
                                             order_id INT COMMENT '订单ID',
                                             user_id INT COMMENT '用户ID',
                                             order_time STRING COMMENT '订单时间',
                                             order_status STRING COMMENT '订单状态',
                                             update_time STRING COMMENT '更新时间'
) COMMENT 'ODS层订单增量表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/order_info_inc/';


-- 创建ODS层订单详情增量表
DROP TABLE IF EXISTS ods_order_detail_inc;
CREATE EXTERNAL TABLE ods_order_detail_inc (
                                               order_detail_id INT COMMENT '订单详情ID',
                                               order_id INT COMMENT '订单ID',
                                               product_id INT COMMENT '商品ID',
                                               product_quantity INT COMMENT '商品数量'
) COMMENT 'ODS层订单详情增量表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/order_detail_inc/';

-- 创建ODS层用户行为日志增量表
DROP TABLE IF EXISTS ods_user_action_log_inc;
CREATE EXTERNAL TABLE ods_user_action_log_inc (
                                                  log_id INT COMMENT '日志ID',
                                                  user_id INT COMMENT '用户ID',
                                                  action_type STRING COMMENT '行为类型',
                                                  action_time STRING COMMENT '行为时间',
                                                  related_product_id INT COMMENT '关联商品ID'
) COMMENT 'ODS层用户行为日志增量表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/user_action_log_inc/';

-- 创建ODS层地区全量表
DROP TABLE IF EXISTS ods_region_info_full;
CREATE EXTERNAL TABLE ods_region_info_full (
                                               region_id INT COMMENT '地区ID',
                                               region_name STRING COMMENT '地区名称',
                                               parent_region_id INT COMMENT '上级地区ID'
) COMMENT 'ODS层地区全量表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/region_info_full/';




