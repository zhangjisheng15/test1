use gd_02;

-- 创建商品核心指标汇总表，计算销售额、销量等核心指标
CREATE  TABLE dws_goods_core_indicators (
                                                     product_id STRING COMMENT '商品ID',
                                                     product_name STRING COMMENT '商品名称',
                                                     category_name STRING COMMENT '分类名称',
                                                     visit_count BIGINT COMMENT '访客数（去重用户数，与文档定义一致）',
                                                     payment_count BIGINT COMMENT '销量（支付件数）',
                                                     payment_amount DECIMAL(12,2) COMMENT '销售额（假设单价*销量，文档核心指标）',
                                                     payment_user_count BIGINT COMMENT '支付买家数（去重）',
                                                     payment_conversion_rate DECIMAL(5,4) COMMENT '支付转化率=支付买家数/访客数（文档公式）',
                                                     ds STRING COMMENT '分区日期'
);


-- 插入数据（按商品聚合核心指标）
INSERT INTO TABLE dws_goods_core_indicators
SELECT
    product_id,
        product_name,
        category_name,


        COUNT(DISTINCT user_id) AS visit_count,
        SUM(CASE WHEN is_order = 1 THEN 1 ELSE 0 END) AS payment_count,
        SUM(CASE WHEN is_order = 1 THEN 100 ELSE 0 END) AS payment_amount,
        COUNT(DISTINCT CASE WHEN is_order = 1 THEN user_id END) AS payment_user_count,
  -- 支付转化率计算（避免除零）
        CASE
            WHEN COUNT(DISTINCT user_id) = 0 THEN 0
            ELSE ROUND(
                        COUNT(DISTINCT CASE WHEN is_order = 1 THEN user_id END) /
                        COUNT(DISTINCT user_id), 4
                )
            END AS payment_conversion_rate ,
        '2025-08-04' AS ds
FROM dwd_user_behavior_detail
WHERE ds = '2025-08-04'
GROUP BY product_id, product_name, category_name;