
use gd_04;

-- 创建商品销售汇总表
DROP TABLE IF EXISTS dws_product_sales_daily;
CREATE EXTERNAL TABLE dws_product_sales_daily (
                                                      product_id INT COMMENT '商品ID',
                                                      product_name STRING COMMENT '商品名称',
                                                      product_category STRING COMMENT '商品类别',
                                                      date_id STRING COMMENT '日期ID',
                                                      total_sales_num INT COMMENT '销售总数量',
                                                      total_sales_amount DECIMAL(10,2) COMMENT '销售总金额'
) COMMENT '商品销售汇总表（按日）'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dws/dws_product_sales_daily/';

-- 从DWD层聚合数据
INSERT OVERWRITE TABLE dws_product_sales_daily PARTITION (ds='2025-08-07')
SELECT
    dod.product_id,
    dp.product_name,
    dp.product_category,
    o.order_date_id AS date_id,
    SUM(dod.product_quantity) AS total_sales_num,
    SUM(dod.total_amount) AS total_sales_amount
FROM dwd_trade_order_inc o
         JOIN dwd_trade_order_detail_inc dod ON o.order_id = dod.order_id AND o.ds='2025-08-07' AND dod.ds='2025-08-07'
         JOIN dim_product_info dp ON dod.product_id = dp.product_id AND dp.ds='2025-08-07'
WHERE o.order_status IN ('已付款', '已发货', '已完成')
GROUP BY dod.product_id, dp.product_name, dp.product_category, o.order_date_id;


select * from dws_product_sales_daily;



-- 创建品类销售汇总表
DROP TABLE IF EXISTS dws_category_sales_daily;
CREATE EXTERNAL TABLE dws_category_sales_daily (
                                                       product_category STRING COMMENT '商品类别',
                                                       date_id STRING COMMENT '日期ID',
                                                       total_sales_num INT COMMENT '销售总数量',
                                                       total_sales_amount DECIMAL(10,2) COMMENT '销售总金额',
                                                       product_count INT COMMENT '品类商品数量',
                                                       order_count INT COMMENT '订单总数'
) COMMENT '品类销售汇总表（按日）'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dws/dws_category_sales_daily/';

-- 从DWS层聚合数据
INSERT OVERWRITE TABLE dws_category_sales_daily PARTITION (ds='2025-08-07')
SELECT
    product_category,
    date_id,
    SUM(total_sales_num) AS total_sales_num,
    SUM(total_sales_amount) AS total_sales_amount,
    COUNT(DISTINCT product_id) AS product_count,
    COUNT(DISTINCT order_id) AS order_count
FROM (
         SELECT
             dod.product_id,
             dp.product_category,
             o.order_date_id AS date_id,
             dod.product_quantity AS total_sales_num,
             dod.total_amount AS total_sales_amount,
             o.order_id
         FROM dwd_trade_order_inc o
                  JOIN dwd_trade_order_detail_inc dod ON o.order_id = dod.order_id AND o.ds='2025-08-07' AND dod.ds='2025-08-07'
                  JOIN dim_product_info dp ON dod.product_id = dp.product_id AND dp.ds='2025-08-07'
         WHERE o.order_status IN ('已付款', '已发货', '已完成')
     ) t
GROUP BY product_category, date_id;


select * from dws_category_sales_daily;


-- 创建用户行为汇总表
DROP TABLE IF EXISTS dws_user_action_daily;
CREATE EXTERNAL TABLE dws_user_action_daily (
                                                    user_id INT COMMENT '用户ID',
                                                    date_id STRING COMMENT '日期ID',
                                                    browse_count INT COMMENT '浏览次数',
                                                    cart_count INT COMMENT '加购次数',
                                                    favorite_count INT COMMENT '收藏次数',
                                                    order_count INT COMMENT '下单次数',
                                                    pay_count INT COMMENT '支付次数'
) COMMENT '用户行为汇总表（按日）'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dws/dws_user_action_daily/';

-- 从DWD层聚合用户行为数据
INSERT OVERWRITE TABLE dws_user_action_daily PARTITION (ds='2025-08-07')
SELECT
    user_id,
    action_date_id AS date_id,
    SUM(CASE WHEN action_type = '浏览' THEN 1 ELSE 0 END) AS browse_count,
    SUM(CASE WHEN action_type = '添加购物车' THEN 1 ELSE 0 END) AS cart_count,
    SUM(CASE WHEN action_type = '收藏' THEN 1 ELSE 0 END) AS favorite_count,
    0 AS order_count,  -- 后续可关联订单表补充
    0 AS pay_count     -- 后续可关联订单表补充
FROM dwd_user_action_inc
WHERE ds='2025-08-07'
GROUP BY user_id, action_date_id;

select * from dws_user_action_daily;



-- 创建流量渠道汇总表
DROP TABLE IF EXISTS dws_traffic_channel_daily;
CREATE EXTERNAL TABLE dws_traffic_channel_daily (
                                                        product_category STRING COMMENT '商品类别',
                                                        date_id STRING COMMENT '日期ID',
                                                        browse_uv INT COMMENT '浏览用户数',
                                                        browse_pv INT COMMENT '浏览次数',
                                                        cart_uv INT COMMENT '加购用户数',
                                                        cart_pv INT COMMENT '加购次数',
                                                        favorite_uv INT COMMENT '收藏用户数',
                                                        favorite_pv INT COMMENT '收藏次数'
) COMMENT '流量渠道汇总表（按日）'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dws/dws_traffic_channel_daily/';

-- 从DWD层聚合流量数据
INSERT OVERWRITE TABLE dws_traffic_channel_daily PARTITION (ds='2025-08-07')
SELECT
    dp.product_category,
    uda.action_date_id AS date_id,
    COUNT(DISTINCT CASE WHEN uda.action_type = '浏览' THEN uda.user_id END) AS browse_uv,
    SUM(CASE WHEN uda.action_type = '浏览' THEN 1 ELSE 0 END) AS browse_pv,
    COUNT(DISTINCT CASE WHEN uda.action_type = '添加购物车' THEN uda.user_id END) AS cart_uv,
    SUM(CASE WHEN uda.action_type = '添加购物车' THEN 1 ELSE 0 END) AS cart_pv,
    COUNT(DISTINCT CASE WHEN uda.action_type = '收藏' THEN uda.user_id END) AS favorite_uv,
    SUM(CASE WHEN uda.action_type = '收藏' THEN 1 ELSE 0 END) AS favorite_pv
FROM dwd_user_action_inc uda
         JOIN dim_product_info dp ON uda.related_product_id = dp.product_id AND dp.ds='2025-08-07'
WHERE uda.ds='2025-08-07'
GROUP BY dp.product_category, uda.action_date_id;

select * from dws_traffic_channel_daily;



