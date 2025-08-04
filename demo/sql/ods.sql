set hive.exec.mode.local.auto=True;
create database if not exists ds_path;
use ds_path;

drop table if exists ods_user_log;
-- 1. 用户行为日志表 (核心表)
CREATE TABLE ods_user_log (
                              log_id BIGINT COMMENT '日志ID',
                              session_id STRING COMMENT '会话ID',
                              user_id BIGINT COMMENT '用户ID',
                              page_id STRING COMMENT '页面ID',
                              page_type STRING COMMENT '页面类型: shop_page/store_item/store_other',
                              refer_page_id STRING COMMENT '来源页面ID',
                              device_type STRING COMMENT '设备类型: wireless/pc',
                              visit_time TIMESTAMP COMMENT '访问时间',
                              stay_duration INT COMMENT '停留时长(秒)',
                              is_order INT COMMENT '是否下单: 0否/1是'
) PARTITIONED BY (dt STRING)

    STORED AS ORC
    location '/warehouse/gd_tms/ods/osd_user_log'
    TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT INTO TABLE ods_user_log PARTITION (dt='2025-07-31')
SELECT
    -- 1. log_id：连续日志ID
    row_number() OVER () AS log_id,
    -- 2. session_id：会话ID（大会话池）
    concat('session_', floor(rand() * 1000000) + 1) AS session_id,
    -- 3. user_id：用户ID（大用户池）
    floor(rand() * 200000) + 1 AS user_id,
    -- 4. page_id：按权重生成页面ID
    CASE
        WHEN rand() < 0.7 THEN concat('product_', floor(rand() * 1000000) + 1)  -- 商品页70%
        WHEN rand() < 0.9 THEN concat('shop_', floor(rand() * 1000000) + 1)     -- 店铺页20%
        ELSE concat('other_', floor(rand() * 100000) + 1)                      -- 其他页10%
        END AS page_id,
    -- 5. page_type：页面类型（与page_id对应）
    CASE
        WHEN rand() < 0.7 THEN 'store_item'
        WHEN rand() < 0.9 THEN 'shop_page'
        ELSE 'store_other'
        END AS page_type,
    -- 6. refer_page_id：30%概率有来源页面
    CASE
        WHEN rand() < 0.3 THEN CASE
                                   WHEN rand() < 0.7 THEN concat('product_', floor(rand() * 1000000) + 1)
                                   WHEN rand() < 0.9 THEN concat('shop_', floor(rand() * 1000000) + 1)
                                   ELSE concat('other_', floor(rand() * 100000) + 1)
            END
        ELSE NULL
        END AS refer_page_id,
    -- 7. device_type：设备类型（无线/PC各50%）
    CASE WHEN rand() < 0.5 THEN 'wireless' ELSE 'pc' END AS device_type,
    -- 8. visit_time：2025-07-30当天随机时间
    from_unixtime(unix_timestamp('2025-07-30 00:00:00') + floor(rand() * 86400)) AS visit_time,
    -- 9. stay_duration：5-300秒随机停留时长
    floor(rand() * 296) + 5 AS stay_duration,
    -- 10. is_order：下单标识（商品页20%，其他页8%）
    CASE
        WHEN (rand() < 0.7 AND rand() < 0.2) THEN 1  -- 商品页下单
        WHEN (rand() >= 0.7 AND rand() < 0.08) THEN 1  -- 非商品页下单
        ELSE 0
        END AS is_order
FROM
    -- 生成1000条数据（通过split+explode）
    (SELECT split(space(999), ' ') AS arr) t
        lateral view explode(t.arr) tmp AS dummy;

select * from ods_user_log;



select count(1) as count
from ods_user_log;


-- 2. 商品页面表 (包含额外页字段)
drop table if exists ods_product;
CREATE TABLE ods_product(
                            page_id STRING COMMENT '页面ID',
                            product_id STRING COMMENT '商品ID',
                            product_name STRING COMMENT '商品名称',
                            category STRING COMMENT '商品类目',
                            page_section STRING COMMENT '页面区域: header/content/sidebar/footer', -- 新增页字段
                            create_time TIMESTAMP COMMENT '创建时间'
) partitioned by (dt STRING)
    STORED AS ORC
    location '/warehouse/gd_tms/ods/ods_product_page'
    tblproperties('orc.compress'='snappy');

-- 3. 店铺页面表
drop table if exists ods_shop;
CREATE TABLE ods_shop (
                          page_id STRING COMMENT '页面ID',
                          page_name STRING COMMENT '页面名称',
                          page_type STRING COMMENT '页面类型: home/activity/category/new',
                          template_id STRING COMMENT '模板ID'
)partitioned by (dt STRING)
    STORED AS ORC
    location '/warehouse/gd_tms/ods/ods_shop_page'
    tblproperties('orc.compress'='snappy');

INSERT INTO TABLE ods_shop PARTITION (dt = '2025-07-31')
SELECT
    -- 生成连续序号的 page_id，格式如 shop_1、shop_2
    concat('shop_', CAST(seq AS STRING)) AS page_id,
    -- 拼接 page_name，从词汇池选前缀 + 固定后缀（分类页/活动页等）
    concat(
        -- 前缀词汇池，让名称更自然
            split('首页,活动,分类,新品,发现,推荐,精选,热门,品牌,专属', ',')[CAST(rand() * 10 AS INT)],
            ' ',
        -- 后缀固定类型，与目标数据匹配
            split('分类页,活动页,首页,新品页', ',')[CAST(rand() * 4 AS INT)]
        ) AS page_name,
    -- 均匀分布 page_type：home/activity/category/new
    split('home,activity,category,new', ',')[CAST(rand() * 4 AS INT)] AS page_type,
    -- 生成 template_id，范围 template_1 ~ template_20
    concat('template_', CAST(FLOOR(rand() * 20) + 1 AS STRING)) AS template_id
FROM (
         -- 生成 1 到 1000 的连续序号
         SELECT pos + 1 AS seq
         FROM (SELECT split(space(999), ' ') AS arr) t
                  LATERAL VIEW posexplode(arr) exploded AS pos, val
     ) num;

select * from ods_shop;


-- 4. 店内路径表 (聚合表)
drop table if exists ods_store_path;
CREATE TABLE ods_store_path (
                                from_page_id STRING COMMENT '来源页面ID',
                                to_page_id STRING COMMENT '去向页面ID',
                                path_count BIGINT COMMENT '路径数量',
                                avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                conversion_rate DOUBLE COMMENT '转化率'
) PARTITIONED BY (dt STRING, device_type STRING)
    STORED AS ORC
    location '/warehouse/gd_tms/ods/ods_store_path'
    tblproperties('orc.compress'='snappy');

INSERT INTO TABLE ods_store_path PARTITION (dt='2025-07-31', device_type)
SELECT
    from_page_id,
    -- 根据来源页面生成去向页面
    CASE
        WHEN from_page_id LIKE 'product_%' THEN
            CASE floor(rand() * 3)
                WHEN 0 THEN concat('product_', floor(rand() * 10000))
                WHEN 1 THEN 'cart_page'
                WHEN 2 THEN 'checkout_page'
                END
        WHEN from_page_id LIKE 'shop_%' THEN
            CASE floor(rand() * 3)
                WHEN 0 THEN concat('category_', floor(rand() * 100))
                WHEN 1 THEN concat('product_', floor(rand() * 10000))
                WHEN 2 THEN 'homepage'
                END
        ELSE
            CASE floor(rand() * 3)
                WHEN 0 THEN concat('shop_', floor(rand() * 1000))
                WHEN 1 THEN concat('activity_', floor(rand() * 100))
                WHEN 2 THEN concat('category_', floor(rand() * 100))
                END
        END AS to_page_id,
    -- 路径数量（10-1000）
    floor(rand() * 991) + 10 AS path_count,
    -- 平均停留时长（5-300秒，保留2位小数）
    round(5 + rand() * 295, 2) AS avg_stay_duration,
    -- 转化率（0.1%-20%，保留4位小数）
    round(0.001 + rand() * 0.199, 4) AS conversion_rate,
    device_type
FROM (
         -- 先生成来源页面和设备类型，避免同层引用
         SELECT
             CASE
                 WHEN floor(rand() * 3) = 0 THEN concat('product_', floor(rand() * 10000))
                 WHEN floor(rand() * 3) = 1 THEN concat('shop_', floor(rand() * 1000))
                 ELSE 'homepage'
                 END AS from_page_id,
             CASE WHEN rand() < 0.6 THEN 'wireless' ELSE 'pc' END AS device_type
         FROM (
                  -- 完全参考之前表的写法：用space+split生成1000条数据
                  SELECT split(space(999), ' ') AS tmp_arr
              ) t1
                  lateral view explode(tmp_arr) tmp AS dummy
     ) t2;

select * from ods_store_path;



-- 5. PC端流量入口表
drop table if exists ods_traffic_source;
CREATE TABLE ods_traffic_source (
                                    source_page_id STRING COMMENT '来源页面ID',
                                    source_type STRING COMMENT '来源类型: direct/search/social',
                                    session_count BIGINT COMMENT '会话数',
                                    avg_session_duration DOUBLE COMMENT '平均会话时长'
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    location '/warehouse/gd_tms/ods/ods_traffic_source'
    tblproperties('orc.compress'='snappy');

INSERT INTO TABLE ods_traffic_source PARTITION (dt='2025-07-31')
SELECT
    -- 来源页面ID（source_前缀+随机数，10%为NULL）
    CASE
        WHEN rand() < 0.1 THEN NULL
        ELSE concat('source_', floor(rand() * 1000))
        END AS source_page_id,
    -- 来源类型（direct/search/social按4:3:3分布）
    CASE
        WHEN rand() < 0.4 THEN 'direct'
        WHEN rand() < 0.7 THEN 'search'
        ELSE 'social'
        END AS source_type,
    -- 会话数（10-5000随机）
    floor(rand() * 4991) + 10 AS session_count,
    -- 平均会话时长（10-610秒，保留2位小数）
    round(10 + rand() * 600, 2) AS avg_session_duration
FROM (
         -- 完全复刻前面表的写法，通过space+split生成1000条数据
         SELECT split(space(999), ' ') AS tmp_arr
     ) t
         lateral view explode(tmp_arr) tmp AS dummy;

select * from ods_traffic_source;



INSERT INTO TABLE ods_product PARTITION (dt='2025-07-30')  -- 固定分区值
SELECT
    -- 1. page_id：生成唯一标识（固定前缀 + 随机数）
    concat('page_', floor(rand() * 1000000)),
    -- 2. product_id：生成唯一标识（固定前缀 + 随机数）
    concat('prod_', floor(rand() * 1000000)),
    -- 3. product_name：随机生成（用 concat 拼接随机数简化）
    concat('prod_', floor(rand() * 1000)),
    -- 4. category：随机选择类目（electronics/clothing/home/books）
    case floor(rand() * 4)
        when 0 then 'electronics'
        when 1 then 'clothing'
        when 2 then 'home'
        when 3 then 'books'
        end,
    -- 5. page_section：随机选择页面区域（header/content/sidebar/footer）
    case floor(rand() * 4)
        when 0 then 'header'
        when 1 then 'content'
        when 2 then 'sidebar'
        when 3 then 'footer'
        end,
    -- 6. create_time：生成当天随机时间（2025-08-05 内的时间戳）
    from_unixtime(unix_timestamp('2025-08-05 00:00:00') + floor(rand() * 86400))
FROM
    -- 生成 100 行数据（利用 lateral view + explode 构造 100 条记录）
    (select split(space(999), ' ') as arr) t
        lateral view explode(t.arr) tmp as dummy;

select * from ods_product where dt = '2025-07-31';


-- 向商品页面表插入2025-07-30的增量数据
INSERT INTO TABLE ods_product PARTITION (dt='2025-07-30')
SELECT
    -- 生成唯一商品页面ID（product_前缀+随机数）
    concat('product_', floor(rand() * 10000)),
    -- 商品ID
    concat('prod_', floor(rand() * 10000)),
    -- 商品名称
    concat(
            case floor(rand() * 4)
                when 0 then '电子'
                when 1 then '服装'
                when 2 then '家居'
                else '图书'
                end,
            '商品_', floor(rand() * 100)
        ),
    -- 商品类目（匹配拉链表category字段）
    case floor(rand() * 4)
        when 0 then 'electronics'
        when 1 then 'clothing'
        when 2 then 'home'
        else 'books'
        end,
    -- 页面区域（随机选择）
    case floor(rand() * 4)
        when 0 then 'header'
        when 1 then 'content'
        when 2 then 'sidebar'
        else 'footer'
        end,
    -- 创建时间（2025-07-30当天随机时间）
    from_unixtime(unix_timestamp('2025-07-30 00:00:00') + floor(rand() * 86400))
FROM
    -- 生成30条商品数据
    (select split(space(299), ' ') as arr) t
        lateral view explode(t.arr) tmp as dummy ;


USE gd;

INSERT INTO TABLE ods_product PARTITION (dt='2025-07-31')  -- 固定分区值
SELECT
    -- 1. page_id：生成唯一标识（固定前缀 + 随机数）
    concat('page_', floor(rand() * 1000000)),
    -- 2. product_id：生成唯一标识（固定前缀 + 随机数）
    concat('prod_', floor(rand() * 1000000)),
    -- 3. product_name：随机生成（用 concat 拼接随机数简化）
    concat('prod_', floor(rand() * 1000)),
    -- 4. category：随机选择类目（electronics/clothing/home/books）
    case floor(rand() * 4)
        when 0 then 'electronics'
        when 1 then 'clothing'
        when 2 then 'home'
        when 3 then 'books'
        end,
    -- 5. page_section：随机选择页面区域（header/content/sidebar/footer）
    case floor(rand() * 4)
        when 0 then 'header'
        when 1 then 'content'
        when 2 then 'sidebar'
        when 3 then 'footer'
        end,
    -- 6. create_time：生成当天随机时间（2025-08-05 内的时间戳）
    from_unixtime(unix_timestamp('2025-7-30 00:00:00') + floor(rand() * 86400))
FROM
    -- 生成 100 行数据（利用 lateral view + explode 构造 100 条记录）
    (select split(space(999), ' ') as arr) t
        lateral view explode(t.arr) tmp as dummy;
