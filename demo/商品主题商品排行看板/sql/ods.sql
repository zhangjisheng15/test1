create database gd_02;

use gd_02;


show tables ;

select * from ods_product;

create temporary table ods_product
(
    page_id      string,
    product_id   string,
    product_name string,
    category     string,
    page_section string,
    create_time  date,
    ds           string
);

create temporary table ods_shop
(
    page_id     string,
    page_name   string,
    page_type   string,
    template_id string,
    ds          string
);


create temporary table ods_store_path
(
    id                int,
    from_page_id      string,
    to_page_id        string,
    path_count        int,
    avg_stay_duration decimal(10, 2),
    conversion_rate   decimal(10, 4),
    device_type       string,
    ds                string
);


create temporary table ods_traffic_source
(
    id                   int,
    source_page_id       string,
    source_type          string,
    session_count        int,
    avg_session_duration decimal(10, 2),
    ds                   string
);


create temporary table ods_user_log
(
    log_id        int,
    session_id    string,
    user_id       int,
    page_id       string,
    page_type     string,
    refer_page_id string,
    device_type   string,
    visit_time    date,
    stay_duration int,
    is_order      tinyint,
    ds            string
);





INSERT INTO TABLE ods_product PARTITION (ds='20250805')
SELECT
    -- 页面ID：product_前缀+序号（1-1000000）
    concat('product_', cast(pos + 1 AS STRING)) AS page_id,
    -- 产品ID：prod_前缀+序号（1-1000000）
    concat('prod_', cast(pos + 1 AS STRING)) AS product_id,
    -- 产品名称：分类前缀+序号
    concat(
            CASE
                WHEN rand() < 0.2 THEN '电子'
                WHEN rand() < 0.4 THEN '服装'
                WHEN rand() < 0.6 THEN '食品'
                WHEN rand() < 0.8 THEN '家居'
                ELSE '图书'
                END,
            '_', cast(pos + 1 AS STRING)
        ) AS product_name,
    -- 分类（与名称前缀对应）
    CASE
        WHEN rand() < 0.2 THEN '电子数码'
        WHEN rand() < 0.4 THEN '服饰鞋帽'
        WHEN rand() < 0.6 THEN '生鲜食品'
        WHEN rand() < 0.8 THEN '家居用品'
        ELSE '图书音像'
        END AS category,
    -- 页面区域（header/content/sidebar/footer按3:4:2:1分布）
    CASE
        WHEN rand() < 0.3 THEN 'header'
        WHEN rand() < 0.7 THEN 'content'  -- 0.3-0.7占40%
        WHEN rand() < 0.9 THEN 'sidebar'  -- 0.7-0.9占20%
        ELSE 'footer'                     -- 剩余10%
        END AS page_section,
    -- 创建时间：2023-01-01至2025-07-31之间的随机时间
    from_unixtime(
                unix_timestamp('2023-01-01 00:00:00') +
                floor(rand() * (unix_timestamp('2025-07-31 23:59:59') - unix_timestamp('2023-01-01 00:00:00')))
        ) AS create_time
FROM (
         -- 生成1000000条数据（通过space生成999999个空格，split后得到100万个数组元素）
         SELECT split(space(999999), ' ') AS arr
     ) t
-- 炸开数组并获取位置索引（pos从0开始，+1后得到1-1000000的序号）
         lateral view posexplode(arr) exploded AS pos, val;


select * from ods_product;



