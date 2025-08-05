
use gd_02;

-- 创建商品维度表，存储商品基础信息及标准化维度
CREATE TABLE dim_product (
                                       product_id STRING COMMENT '商品唯一标识',
                                       product_name STRING COMMENT '商品名称',
                                       category STRING COMMENT '商品分类（如电子数码、服饰鞋帽）',
                                       category_name STRING COMMENT '分类中文名称（与操作文档\'分类筛选\'对应）',
                                       page_section STRING COMMENT '商品页面区域（header/content等）',
                                       create_time DATE COMMENT '商品创建时间',
                                       page_id STRING COMMENT '商品页面ID（关联ods层）',
                                       is_valid TINYINT COMMENT '商品有效性标识（1=有效，0=无效）',
                                       update_time TIMESTAMP COMMENT '维度数据更新时间',
                                       ds STRING COMMENT '原分区字段，标识数据日期（yyyy-MM-dd）'  -- 作为普通字段保留
);


-- 插入数据（从ods层同步并标准化）
INSERT INTO TABLE dim_product
SELECT
    product_id ,
        product_name ,
        category ,
        CASE  -- 分类中文映射，支持文档"分类筛选"功能
            WHEN category = '电子数码' THEN '电子'
            WHEN category = '服饰鞋帽' THEN '服装'
            WHEN category = '生鲜食品' THEN '食品'
            WHEN category = '家居用品' THEN '家居'
            ELSE '图书'
            END AS category_name,
    page_section ,
        create_time,
        page_id,
        1 AS is_valid,
        current_timestamp() AS update_time,
        '2025-08-05' AS ds
FROM ods_product
WHERE ds = '20250805';  -- 从ods层指定日期同步


select * from dim_product;


-- 创建店铺维度表，支持店铺相关分析
CREATE TABLE dim_shop (
                                    page_id STRING COMMENT '店铺页面ID',
                                    page_name STRING COMMENT '店铺名称',
                                    page_type STRING COMMENT '页面类型编码',
                                    page_type_name STRING COMMENT '页面类型中文名称（与文档"形式切换"对应）',
                                    template_id STRING COMMENT '店铺模板ID',
                                    is_valid TINYINT COMMENT '店铺有效性标识',
                                    update_time TIMESTAMP COMMENT '数据更新时间',
                                    ds STRING COMMENT '分区日期'
);

-- 插入数据
INSERT INTO TABLE dim_shop
SELECT
    page_id,
        page_name,
        page_type,
        CASE  -- 页面类型中文映射，便于看板展示
            WHEN page_type = 'home' THEN '首页'
            WHEN page_type = 'activity' THEN '活动页'
            WHEN page_type = 'category' THEN '分类页'
            ELSE '其他页'
            END AS page_type_name,
    template_id,
        1 AS is_valid,
        current_timestamp() AS update_time,
    '2025-08-05' AS ds
FROM ods_shop
WHERE ds = '20250805';


select *
from dim_shop;



