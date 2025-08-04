

set hive.exec.mode.local.auto=True;
create database if not exists ds_path;
use gd;

-- 1. 页面维度拉链表（整合店铺页、商品详情页、店铺其他页信息）
drop table if exists dim_page_zip;
create table dim_page_zip (
                              page_id string comment '页面唯一ID',
                              page_type string comment '页面类型：shop_page(店铺页)/product_detail_page(商品详情页)/other_page(店铺其他页)',
                              page_subtype string comment '页面子类型：店铺页包含home(首页)/activity(活动页)/category(分类页)/new(新品页)等；商品详情页为product；其他页为subscribe(订阅页)/live(直播页)等',
                              related_id string comment '关联ID：店铺页关联店铺ID，商品页关联product_id',
                              page_name string comment '页面名称：如"店铺首页"/"商品A详情页"',
                              category string comment '商品类目（仅商品详情页有值）',
                              start_date string comment '生效日期（yyyy-MM-dd）',
                              end_date string comment '失效日期（yyyy-MM-dd，9999-12-31表示当前有效）',
                              is_valid tinyint comment '是否有效：1-有效，0-无效'
)
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dim/dim_page_zip'
    tblproperties('orc.compress'='snappy');

-- 1. 每日增量更新页面维度拉链表（整合ods_shop和ods_product）
insert overwrite table dim_page_zip partition(dt='2025-07-30')
select
    nvl(s.page_id, p.page_id) as page_id,
    -- 确定页面类型（shop_page/product_detail_page/other_page）
    case
        when s.page_id is not null then 'shop_page'
        when p.page_id is not null then 'product_detail_page'
        else 'other_page'  -- 兜底处理店铺其他页
        end as page_type,
    -- 确定页面子类型
    case
        when s.page_id is not null then s.page_type  -- 店铺页子类型：home/activity等
        when p.page_id is not null then 'product'    -- 商品详情页子类型
        else 'other'  -- 店铺其他页子类型（需结合业务补充）
        end as page_subtype,
    nvl(s.page_id, p.product_id) as related_id,
    nvl(s.page_name, concat(p.product_name, '详情页')) as page_name,
    p.category as category,
    '2025-07-30' as start_date,
    '9999-12-31' as end_date,
    1 as is_valid
from
    -- 关联店铺页面表（ods_shop）
    (select page_id, page_name, page_type from ods_shop where dt='2025-07-30') s
        full outer join
    -- 关联商品页面表（ods_product）
        (select page_id, product_id, product_name, category from ods_product where dt='2025-07-30') p
    on s.page_id = p.page_id
-- 过滤已失效或无需更新的页面（历史数据处理）
where not exists (
    select 1 from dim_page_zip
    where dt=date_sub('2025-07-30', 1)
      and page_id = nvl(s.page_id, p.page_id)
      and is_valid=1
);

select  * from dim_page_zip;

-- 2. 设备维度拉链表（区分无线端/PC端）
drop table if exists dim_device_zip;
create table dim_device_zip (
                                device_type string comment '设备类型：wireless(无线端)/pc(PC端)',
                                device_desc string comment '设备描述：如"手机无线端"/"电脑PC端"',
                                start_date string comment '生效日期（yyyy-MM-dd）',
                                end_date string comment '失效日期（yyyy-MM-dd，9999-12-31表示当前有效）',
                                is_valid tinyint comment '是否有效：1-有效，0-无效'
)
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dim/dim_device_zip'
    tblproperties('orc.compress'='snappy');

-- 2. 设备维度拉链表（dim_device_zip）关联更新
-- 每日增量更新设备维度拉链表（基于ods_user_log的device_type）
-- 修正后：设备维度拉链表（dim_device_zip）关联更新（解决device_type字段重复问题）
insert overwrite table dim_device_zip partition(dt='2025-07-30')
select
    t.device_type,  -- 明确引用子查询t的device_type
    case
        when t.device_type='wireless' then '手机无线端'
        when t.device_type='pc' then '电脑PC端'
        end as device_desc,
    '2025-07-30' as start_date,
    '9999-12-31' as end_date,
    1 as is_valid
from (
         -- 子查询提取ods_user_log中的设备类型，添加别名t
         select distinct device_type from ods_user_log where dt='2025-07-30'
     ) t
-- 过滤条件中为dim_device_zip添加别名d，明确引用其device_type
where t.device_type not in (
    select d.device_type from dim_device_zip d
    where d.dt=date_sub('2025-07-30', 1) and d.is_valid=1
);


select  * from dim_device_zip;
-- 3. 来源类型维度拉链表（PC端流量入口来源）
drop table if exists dim_source_type_zip;
create table dim_source_type_zip (
                                     source_type string comment '来源类型：direct(直接访问)/search(搜索)/social(社交平台)等',
                                     source_desc string comment '来源描述：如"百度搜索"/"微信分享"/"直接输入网址"',
                                     start_date string comment '生效日期（yyyy-MM-dd）',
                                     end_date string comment '失效日期（yyyy-MM-dd，9999-12-31表示当前有效）',
                                     is_valid tinyint comment '是否有效：1-有效，0-无效'
)
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dim/dim_source_type_zip'
    tblproperties('orc.compress'='snappy');

-- 3. 来源类型维度拉链表（dim_source_type_zip）关联更新
-- 每日增量更新来源类型维度拉链表（基于ods_traffic_source）
insert overwrite table dim_source_type_zip partition(dt='2025-07-30')
select
    t.source_type,
    case
        when t.source_type='direct' then '直接访问（输入网址）'
        when t.source_type='search' then '搜索引擎来源'
        when t.source_type='social' then '社交平台分享'
        else '其他来源'
        end as source_desc,
    '2025-07-30' as start_date,
    '9999-12-31' as end_date,
    1 as is_valid
from (
         select distinct source_type from ods_traffic_source where dt='2025-07-30'
     ) t
where not exists (
    select 1 from dim_source_type_zip d
    where d.dt=date_sub('2025-07-30', 1)
      and d.is_valid=1
      and d.source_type = t.source_type
);

select * from dim_source_type_zip;

-- 4. 用户维度拉链表（访客及下单用户信息）
drop table if exists dim_user_zip;
create table dim_user_zip (
                              user_id bigint comment '用户ID',
                              user_tag string comment '用户标签：如"新访客"/"复购用户"（根据下单行为衍生）',
                              start_date string comment '生效日期（yyyy-MM-dd）',
                              end_date string comment '失效日期（yyyy-MM-dd，9999-12-31表示当前有效）',
                              is_valid tinyint comment '是否有效：1-有效，0-无效'
)
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');


-- 4. 用户维度拉链表（dim_user_zip）关联更新
-- 每日增量更新用户维度拉链表（基于ods_user_log的user_id和下单行为）
insert overwrite table dim_user_zip partition(dt='2025-07-30')
select
    user_id,
    case when max(is_order)=1 then '复购用户' else '新访客' end as user_tag,  -- 基于是否下单标记用户
    '2025-07-30' as start_date,


    '9999-12-31' as end_date,
    1 as is_valid
from ods_user_log
where dt='2025-07-30' and user_id is not null
group by user_id
having user_id not in (
    select user_id from dim_user_zip where dt=date_sub('2025-07-30', 1) and is_valid=1
);

select * from dim_user_zip;
-- 5. 路径流转规则维度拉链表（页面间流转关系）
drop table if exists dim_path_rule_zip;
create table dim_path_rule_zip (
                                   from_page_type string comment '来源页面类型',
                                   to_page_type string comment '去向页面类型',
                                   rule_desc string comment '流转规则描述：如"店铺首页→商品详情页"/"商品详情页→直播页"',
                                   start_date string comment '生效日期（yyyy-MM-dd）',
                                   end_date string comment '失效日期（yyyy-MM-dd，9999-12-31表示当前有效）',
                                   is_valid tinyint comment '是否有效：1-有效，0-无效'
)
    partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dim/dim_path_rule_zip'
    tblproperties('orc.compress'='snappy');

-- 5. 路径流转规则维度拉链表（dim_path_rule_zip）关联更新
-- 每日增量更新路径流转规则（基于ods_store_path的页面类型流转）
insert overwrite table dim_path_rule_zip partition(dt='2025-07-30')
select
    from_page_type,
    to_page_type,
    concat(from_page_type, '→', to_page_type) as rule_desc,
    '2025-07-30' as start_date,
    '9999-12-31' as end_date,
    1 as is_valid
from (
         -- 关联页面维度表获取来源/去向页面类型
         select
             p1.page_type as from_page_type,
             p2.page_type as to_page_type
         from ods_store_path sp
                  join dim_page_zip p1 on sp.from_page_id = p1.page_id and p1.dt='2025-07-30' and p1.is_valid=1
                  join dim_page_zip p2 on sp.to_page_id = p2.page_id and p2.dt='2025-07-30' and p2.is_valid=1
         where sp.dt='2025-07-30'
         group by p1.page_type, p2.page_type
     ) t
where not exists (
    select 1 from dim_path_rule_zip d
    where d.dt=date_sub('2025-07-30', 1)
      and d.is_valid=1
      and d.from_page_type = t.from_page_type
      and d.to_page_type = t.to_page_type
);

select * from dim_path_rule_zip;