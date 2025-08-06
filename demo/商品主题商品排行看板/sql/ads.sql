
use gd_02;

-- 商品销售核心指标表（ADS 层）
CREATE EXTERNAL TABLE ads_product_sales_core (
                                                     product_id BIGINT COMMENT '商品ID',
                                                     product_name STRING COMMENT '商品名称（关联 DIM 层）',
                                                     category_name STRING COMMENT '商品分类名称（关联 DIM 层）',
                                                     stat_date DATE COMMENT '统计日期',
                                                     total_sales_amount DECIMAL(12,2) COMMENT '当日总销售额',
                                                     total_sales_quantity INT COMMENT '当日总销量',
                                                     total_visitor_count INT COMMENT '当日总访客数',
                                                     total_pay_buyer_count INT COMMENT '当日总支付买家数',
                                                     avg_pay_conversion_rate DECIMAL(5,4) COMMENT '当日平均支付转化率'
                                             )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/ads.db/ads_product_sales_core';

-- 从 DWS 层 + DIM 层关联生成指标
INSERT OVERWRITE TABLE ads_product_sales_core PARTITION (dt='20250806')
SELECT
    s.product_id,
    d.product_name,            -- 从商品维度表取名称
    c.category_name,           -- 从分类维度表取分类名称
    s.stat_date,
    s.total_sales_amount,
    s.total_sales_quantity,
    s.total_visitor_count,
    s.total_pay_buyer_count,
    s.avg_pay_conversion_rate
FROM dws_product_sales_daily s
         JOIN dim_product_base d
              ON s.product_id = d.product_id
                  AND s.dt = d.dt
         JOIN dim_product_category c
              ON d.category_id = c.category_id
                  AND d.dt = c.dt
WHERE s.dt = '20250806';


CREATE EXTERNAL TABLE ads_product_traffic_channel (
                                                          product_id BIGINT COMMENT '商品ID',
                                                          product_name STRING COMMENT '商品名称',
                                                          stat_date DATE COMMENT '统计日期',
                                                          traffic_source STRING COMMENT '流量来源（如APP、小程序）',
                                                          total_visitor_count INT COMMENT '该渠道总访客数',
                                                          avg_pay_conversion_rate DECIMAL(5,4) COMMENT '该渠道支付转化率'
                                                      )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/ads.db/ads_product_traffic_channel';

INSERT OVERWRITE TABLE ads_product_traffic_channel PARTITION (dt='20250806')
SELECT
    t.product_id,
    d.product_name,
    t.stat_date,
    t.traffic_source,
    t.total_visitor_count,
    t.avg_pay_conversion_rate
FROM dws_product_traffic_channel t
         JOIN dim_product_base d
              ON t.product_id = d.product_id
                  AND t.dt = d.dt
WHERE t.dt = '20250806';


