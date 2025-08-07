
use gd_04;

-- 创建销售核心概况指标表
DROP TABLE IF EXISTS ads_sales_overview;
CREATE EXTERNAL TABLE ads_sales_overview (
                                                 product_category STRING COMMENT '商品类别',
                                                 date_id STRING COMMENT '日期ID',
                                                 total_sales_num INT COMMENT '销售总数量',
                                                 total_sales_amount DECIMAL(10,2) COMMENT '销售总金额',
                                                 sales_growth_rate DECIMAL(5,4) COMMENT '销售额增长率',
                                                 month_target_gmv DECIMAL(10,2) COMMENT '月度目标GMV',
                                                 month_target_progress DECIMAL(5,4) COMMENT '月度目标达成率',
                                                 store_contribution_rate DECIMAL(5,4) COMMENT '店铺贡献占比'
) COMMENT '销售核心概况指标表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/ads/ads_sales_overview/';

-- 计算销售核心指标
INSERT OVERWRITE TABLE ads_sales_overview PARTITION (ds='2025-08-07')
SELECT
    csd.product_category,
    csd.date_id,
    csd.total_sales_num,
    csd.total_sales_amount,
    -- 销售额增长率（与前一日对比）
    (csd.total_sales_amount / prev_day.total_sales_amount - 1) AS sales_growth_rate,
    1000000 AS month_target_gmv,  -- 假设月度目标GMV为100万
    -- 月度目标达成率（截至当日累计/月度目标）
    (SUM(csd.total_sales_amount) OVER (PARTITION BY csd.product_category ORDER BY csd.date_id)
        / 1000000) AS month_target_progress,
    -- 店铺贡献占比（品类销售额/全店销售额）
    csd.total_sales_amount / total_store_sales.total_amount AS store_contribution_rate
FROM dws_category_sales_daily csd
-- 关联前一日数据计算增长率
         LEFT JOIN dws_category_sales_daily prev_day
                   ON csd.product_category = prev_day.product_category
                       AND prev_day.date_id = date_format(date_add(to_date(csd.date_id), -1), 'yyyyMMdd')
                       AND prev_day.ds='2025-08-07'
-- 计算全店总销售额
         CROSS JOIN (
    SELECT SUM(total_sales_amount) AS total_amount
    FROM dws_category_sales_daily
    WHERE ds='2025-08-07'
) total_store_sales
WHERE csd.ds='2025-08-07';

select * from ads_sales_overview;




-- 创建品类属性分析指标表
DROP TABLE IF EXISTS ads_category_attribute_analysis;
CREATE EXTERNAL TABLE ads_category_attribute_analysis (
                                                              product_category STRING COMMENT '商品类别',
                                                              date_id STRING COMMENT '日期ID',
                                                              attribute_type STRING COMMENT '属性类型（模拟）',
                                                              attribute_value STRING COMMENT '属性值（模拟）',
                                                              traffic_uv INT COMMENT '属性流量用户数',
                                                              pay_conversion_rate DECIMAL(5,4) COMMENT '支付转化率',
                                                              active_product_num INT COMMENT '动销商品数'
) COMMENT '品类属性分析指标表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/ads/ads_category_attribute_analysis/';

-- 计算品类属性指标（基于商品类别模拟属性分析）
INSERT OVERWRITE TABLE ads_category_attribute_analysis PARTITION (ds='2025-08-07')
SELECT
    dp.product_category,
    psd.date_id,
    '价格区间' AS attribute_type,
    -- 模拟价格区间属性
    CASE
        WHEN dp.price < 50 THEN '0-50元'
        WHEN dp.price < 200 THEN '50-200元'
        WHEN dp.price < 500 THEN '200-500元'
        ELSE '500元以上'
        END AS attribute_value,
    COUNT(DISTINCT uda.user_id) AS traffic_uv,
    -- 支付转化率=支付用户数/流量用户数
    COUNT(DISTINCT o.user_id) / COUNT(DISTINCT uda.user_id) AS pay_conversion_rate,
    COUNT(DISTINCT psd.product_id) AS active_product_num
FROM dws_product_sales_daily psd
         JOIN dim_product_info dp ON psd.product_id = dp.product_id AND dp.ds='2025-08-07'
         LEFT JOIN dwd_user_action_inc uda
                   ON psd.product_id = uda.related_product_id
                       AND psd.date_id = uda.action_date_id
                       AND uda.ds='2025-08-07'
         LEFT JOIN dwd_trade_order_inc o
                   ON psd.date_id = o.order_date_id
                       AND o.order_status = '已付款'
                       AND o.ds='2025-08-07'
WHERE psd.ds='2025-08-07'
GROUP BY dp.product_category, psd.date_id,
         CASE
             WHEN dp.price < 50 THEN '0-50元'
             WHEN dp.price < 200 THEN '50-200元'
             WHEN dp.price < 500 THEN '200-500元'
             ELSE '500元以上'
             END;


select * from ads_category_attribute_analysis;


-- ADS 层：商品销售分析表
DROP TABLE IF EXISTS ads_product_sales_analysis;
CREATE EXTERNAL TABLE ads_product_sales_analysis (
                                                         product_id INT COMMENT '商品ID',
                                                         product_name STRING COMMENT '商品名称',
                                                         product_category STRING COMMENT '商品品类',
                                                         date_id STRING COMMENT '销售日期',
                                                         total_sales_num INT COMMENT '销售总量',
                                                         total_sales_amount DECIMAL(10,2) COMMENT '销售总金额',
                                                         avg_price DECIMAL(10,2) COMMENT '客单价=总金额/总量',
                                                         category_sales_ratio DECIMAL(5,4) COMMENT '品类内销售额占比'
)
    PARTITIONED BY (ds STRING COMMENT '分区日期')
    STORED AS ORC
    LOCATION '/warehouse/ads/ads_product_sales_analysis/';


INSERT OVERWRITE TABLE ads_product_sales_analysis
    PARTITION (ds='2025-08-07')
SELECT
    psd.product_id,
    dim_prod.product_name,  -- 关联商品维度表取名称
    dim_prod.product_category,
    psd.date_id,
    psd.total_sales_num,  -- 从 DWS 层每日销售聚合表获取
    psd.total_sales_amount,  -- 从 DWS 层获取
    psd.total_sales_amount / psd.total_sales_num AS avg_price,
    -- 计算该商品在品类中的销售额占比
    psd.total_sales_amount / csd.total_sales_amount AS category_sales_ratio
FROM
    -- DWS 层真实表：商品每日销售汇总
    dws_product_sales_daily psd
-- 关联 DIM 层商品表取名称和品类
        JOIN dim_product_info dim_prod
             ON psd.product_id = dim_prod.product_id
                 AND dim_prod.ds='2025-08-07'
-- 关联 DWS 层品类销售表取品类总销售额
        JOIN dws_category_sales_daily csd
             ON psd.product_category = csd.product_category
                 AND psd.date_id = csd.date_id
                 AND csd.ds='2025-08-07'
WHERE
        psd.ds='2025-08-07';

select * from ads_product_sales_analysis;


-- ADS 层：用户行为转化分析表（基于文件中真实的 DWS 表）
DROP TABLE IF EXISTS ads_user_behavior_conversion;
CREATE EXTERNAL TABLE ads_user_behavior_conversion (
                                                           product_category STRING COMMENT '商品品类',
                                                           date_id STRING COMMENT '日期',
                                                           browse_uv INT COMMENT '浏览用户数',
                                                           cart_uv INT COMMENT '加购用户数',
                                                           order_uv INT COMMENT '下单用户数',
                                                           browse_to_cart_rate DECIMAL(5,4) COMMENT '浏览→加购转化率',
                                                           cart_to_order_rate DECIMAL(5,4) COMMENT '加购→下单转化率'
)
    PARTITIONED BY (ds STRING COMMENT '分区日期')
    STORED AS ORC
    LOCATION '/warehouse/ads/ads_user_behavior_conversion/';

INSERT OVERWRITE TABLE ads_user_behavior_conversion
    PARTITION (ds='2025-08-07')
SELECT
    tcd.product_category,
    tcd.date_id,
    tcd.browse_uv,  -- 从 DWS 层流量表获取
    tcd.cart_uv,    -- 从 DWS 层流量表获取
    -- 关联订单表计算下单用户数
    COUNT(DISTINCT o.user_id) AS order_uv,
    -- 浏览→加购转化率
    tcd.cart_uv / NULLIF(tcd.browse_uv, 0) AS browse_to_cart_rate,
    -- 加购→下单转化率
    COUNT(DISTINCT o.user_id) / NULLIF(tcd.cart_uv, 0) AS cart_to_order_rate
FROM
    -- DWS 层真实表：流量渠道汇总表
    dws_traffic_channel_daily tcd
-- 关联 DWD 层订单表取下单用户
        LEFT JOIN dwd_trade_order_inc o
                  ON tcd.date_id = o.order_date_id
                      AND o.order_status IN ('已付款', '已完成')
                      AND o.ds='2025-08-07'
WHERE
        tcd.ds='2025-08-07'
GROUP BY
    tcd.product_category, tcd.date_id, tcd.browse_uv, tcd.cart_uv;

select * from ads_user_behavior_conversion;


