
use gd_02;

-- 商品销售日汇总宽表（DWS 层）
CREATE EXTERNAL TABLE dws_product_sales_daily (
                                                      product_id BIGINT COMMENT '商品ID',
                                                      stat_date DATE COMMENT '统计日期',
                                                      total_sales_amount DECIMAL(12,2) COMMENT '当日总销售额',
                                                      total_sales_quantity INT COMMENT '当日总销量',
                                                      total_visitor_count INT COMMENT '当日总访客数',
                                                      total_pay_buyer_count INT COMMENT '当日总支付买家数',
                                                      avg_pay_conversion_rate DECIMAL(5,4) COMMENT '当日平均支付转化率'
                                                  )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/dws_product_sales_daily';

-- 从 DWD 层汇总数据
INSERT OVERWRITE TABLE dws_product_sales_daily PARTITION (dt='20250806')
SELECT
    product_id,
    stat_date,
    SUM(sales_amount) AS total_sales_amount,
    SUM(sales_quantity) AS total_sales_quantity,
    SUM(visitor_count) AS total_visitor_count,
    SUM(pay_buyer_count) AS total_pay_buyer_count,
    AVG(pay_conversion_rate) AS avg_pay_conversion_rate
FROM dwd_product_sales_detail
WHERE dt = '20250806'
GROUP BY product_id, stat_date;

-- 商品流量渠道汇总宽表（DWS 层）
CREATE EXTERNAL TABLE dws_product_traffic_channel (
                                                          product_id BIGINT COMMENT '商品ID',
                                                          stat_date DATE COMMENT '统计日期',
                                                          traffic_source STRING COMMENT '流量来源类型',
                                                          total_visitor_count INT COMMENT '该渠道总访客数',
                                                          avg_pay_conversion_rate DECIMAL(5,4) COMMENT '该渠道平均支付转化率'
                                                  )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/dws_product_traffic_channel';

INSERT OVERWRITE TABLE dws_product_traffic_channel PARTITION (dt='20250806')
SELECT
    product_id,
    stat_date,
    traffic_source,
    SUM(visitor_count) AS total_visitor_count,
    AVG(pay_conversion_rate) AS avg_pay_conversion_rate
FROM dwd_product_traffic_detail
WHERE dt = '20250806'
GROUP BY product_id, stat_date, traffic_source;