use gd_04;

-- 创建交易域订单事实表
DROP TABLE IF EXISTS dwd_trade_order_inc;
CREATE EXTERNAL TABLE dwd_trade_order_inc (
                                                  order_id INT COMMENT '订单ID',
                                                  user_id INT COMMENT '用户ID',
                                                  order_date_id STRING COMMENT '下单日期ID',
                                                  order_time STRING COMMENT '下单时间',
                                                  order_status STRING COMMENT '订单状态'
) COMMENT '交易域订单事实表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dwd/dwd_trade_order_inc/';

-- 从ODS层同步并清洗数据
INSERT OVERWRITE TABLE dwd_trade_order_inc PARTITION (ds='2025-08-07')
SELECT
    order_id,
    user_id,
    date_format(to_date(order_time), 'yyyyMMdd') AS order_date_id,
    order_time,
    order_status
FROM ods_order_info
WHERE ds='20250807';

select * from dwd_trade_order_inc;


-- 创建交易域订单详情事实表
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc (
                                                         order_detail_id INT COMMENT '订单详情ID',
                                                         order_id INT COMMENT '订单ID',
                                                         product_id INT COMMENT '商品ID',
                                                         product_quantity INT COMMENT '商品数量',
                                                         product_price DECIMAL(10,2) COMMENT '商品单价',
                                                         total_amount DECIMAL(10,2) COMMENT '商品总价'
) COMMENT '交易域订单详情事实表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dwd/dwd_trade_order_detail_inc/';

-- 从ODS层关联商品维度表计算金额
INSERT OVERWRITE TABLE dwd_trade_order_detail_inc PARTITION (ds='2025-08-07')
SELECT
    od.order_detail_id,
    od.order_id,
    od.product_id,
    od.product_quantity,
    dp.price AS product_price,
    od.product_quantity * dp.price AS total_amount
FROM ods_order_detail od
         JOIN dim_product_info dp ON od.product_id = dp.product_id AND dp.ds='2025-08-07'
WHERE od.ds='20250807';

select * from dwd_trade_order_detail_inc;

-- 创建用户行为事实表
DROP TABLE IF EXISTS dwd_user_action_inc;
CREATE EXTERNAL TABLE dwd_user_action_inc (
                                                  log_id INT COMMENT '日志ID',
                                                  user_id INT COMMENT '用户ID',
                                                  action_type STRING COMMENT '行为类型',
                                                  action_date_id STRING COMMENT '行为日期ID',
                                                  action_time STRING COMMENT '行为时间',
                                                  related_product_id INT COMMENT '关联商品ID'
) COMMENT '用户行为事实表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dwd/dwd_user_action_inc/';

-- 从ODS层同步并转换数据
INSERT OVERWRITE TABLE dwd_user_action_inc PARTITION (ds='2025-08-07')
SELECT
    log_id,
    user_id,
    action_type,
    date_format(to_date(action_time), 'yyyyMMdd') AS action_date_id,
    action_time,
    related_product_id
FROM ods_user_action_log
WHERE ds='20250807';


select * from dwd_user_action_inc;
