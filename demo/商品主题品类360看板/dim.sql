use gd_04;

-- 创建商品维度表
DROP TABLE IF EXISTS dim_product_info;
CREATE EXTERNAL TABLE dim_product_info (
                                          product_id INT COMMENT '商品ID',
                                          product_name STRING COMMENT '商品名称',
                                          product_category STRING COMMENT '商品类别',
                                          price DECIMAL(10,2) COMMENT '商品价格',
                                          stock INT COMMENT '商品库存'
) COMMENT '商品维度表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dim/dim_product/';

-- 从ODS层同步数据
INSERT OVERWRITE TABLE dim_product_info PARTITION (ds='2025-08-07')
SELECT
    product_id,
    product_name,
    product_category,
    price,
    stock
FROM ods_product_info
WHERE ds='20250807';

select * from dim_product_info;


-- 创建用户维度表
DROP TABLE IF EXISTS dim_user_info;
CREATE EXTERNAL TABLE dim_user_info (
                                       user_id INT COMMENT '用户ID',
                                       user_name STRING COMMENT '用户姓名',
                                       gender STRING COMMENT '性别',
                                       age INT COMMENT '年龄',
                                       register_time STRING COMMENT '注册时间'
) COMMENT '用户维度表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dim/dim_user_info/';

-- 从ODS层同步数据
INSERT OVERWRITE TABLE dim_user_info PARTITION (ds='2025-08-07')
SELECT
    user_id,
    user_name,
    gender,
    age,
    register_time
FROM ods_user_info
WHERE ds='20250807';

select * from dim_user_info;



-- 创建地区维度表
DROP TABLE IF EXISTS dim_region_info;
CREATE EXTERNAL TABLE dim_region_info (
                                         region_id INT COMMENT '地区ID',
                                         region_name STRING COMMENT '地区名称',
                                         parent_region_id INT COMMENT '上级地区ID',
                                         region_level INT COMMENT '地区级别：1-省，2-市，3-区/县'
) COMMENT '地区维度表'
    PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/dim/dim_region_info/';

-- 从ODS层同步数据并补充地区级别
INSERT OVERWRITE TABLE dim_region_info PARTITION (ds='2025-08-07')
SELECT
    region_id,
    region_name,
    parent_region_id,
    CASE
        WHEN parent_region_id IS NULL THEN 1
        ELSE 2
        END AS region_level
FROM ods_region_info
WHERE ds='20250807';


select * from dim_region_info;

