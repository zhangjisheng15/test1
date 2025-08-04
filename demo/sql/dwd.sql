
--. 页面访问明细事实表（dwd_traffic_page_visit）首日初始化
set hive.exec.mode.local.auto=True;

USE gd;
--1. 页面访问行为明细表（dwd_page_visit_detail）
create table if not exists dwd_page_visit_detail (
                                                     log_id bigint comment '日志唯一ID',
                                                     session_id string comment '会话ID',
                                                     user_id bigint comment '用户ID',
                                                     page_id string comment '页面ID',
                                                     page_type string comment '页面类型：shop_page(店铺页)/product_detail_page(商品详情页)/other_page(店铺其他页)',
                                                     page_subtype string comment '页面子类型：home(首页)/activity(活动页)/product(商品)/subscribe(订阅页)等',
                                                     page_name string comment '页面名称',
                                                     device_type string comment '设备类型：wireless(无线端)/pc(PC端)',
                                                     refer_page_id string comment '来源页面ID',
                                                     visit_time timestamp comment '访问时间',
                                                     stay_duration int comment '停留时长(秒)',
                                                     is_order int comment '是否下单：0-否/1-是'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dwd/dwd_page_visit_detail'
    tblproperties('orc.compress'='snappy');

-- 首日全量初始化（假设历史数据至2025-07-30）
insert overwrite table dwd_page_visit_detail partition(dt='2025-07-30')
select
    u.log_id,
    u.session_id,
    u.user_id,
    u.page_id,
    -- 映射页面类型（文档中店铺页/商品详情页/其他页分类）
    case
        when u.page_type = 'shop_page' then 'shop_page'
        when u.page_type = 'store_item' then 'product_detail_page'
        when u.page_type = 'store_other' then 'other_page'
        end as page_type,
    -- 补充页面子类型（店铺页子类型如首页/活动页等）
    case
        when u.page_type = 'shop_page' then s.page_type
        when u.page_type = 'store_item' then 'product'
        else 'other'
        end as page_subtype,
    -- 补充页面名称
    case
        when u.page_type = 'shop_page' then s.page_name
        when u.page_type = 'store_item' then concat(p.product_name, '详情页')
        else '其他页面'
        end as page_name,
    u.device_type,
    u.refer_page_id,
    u.visit_time,
    u.stay_duration,
    u.is_order
from ods_user_log u
         left join ods_shop s
                   on u.page_id = s.page_id and u.dt = s.dt and s.dt <= '2025-07-30'
         left join ods_product p
                   on u.page_id = p.page_id and u.dt = p.dt and p.dt <= '2025-07-30'
where u.dt <= '2025-07-30';

-- 2025-07-31日新增数据
insert overwrite table dwd_page_visit_detail partition(dt='2025-07-31')
select
    u.log_id,
    u.session_id,
    u.user_id,
    u.page_id,
    case
        when u.page_type = 'shop_page' then 'shop_page'
        when u.page_type = 'store_item' then 'product_detail_page'
        when u.page_type = 'store_other' then 'other_page'
        end as page_type,
    case
        when u.page_type = 'shop_page' then s.page_type
        when u.page_type = 'store_item' then 'product'
        else 'other'
        end as page_subtype,
    case
        when u.page_type = 'shop_page' then s.page_name
        when u.page_type = 'store_item' then concat(p.product_name, '详情页')
        else '其他页面'
        end as page_name,
    u.device_type,
    u.refer_page_id,
    u.visit_time,
    u.stay_duration,
    u.is_order
from ods_user_log u
         left join ods_shop s
                   on u.page_id = s.page_id and u.dt = s.dt and s.dt = '20250804'
         left join ods_product p
                   on u.page_id = p.page_id and u.dt = p.dt and p.dt = '2025-07-30'
where u.dt = '2025-08-04';

select  * from dwd_page_visit_detail;


--2. 店内路径流转明细表（dwd_instore_path_flow）
-- 路径流
drop table dwd_instore_path_flow;
create table if not exists dwd_instore_path_flow (
                                                     path_id string comment '路径唯一ID',
                                                     from_page_id string comment '来源页面ID',
                                                     from_page_type string comment '来源页面类型',
                                                     to_page_id string comment '去向页面ID',
                                                     to_page_type string comment '去向页面类型',
                                                     path_count bigint comment '路径流转次数',
                                                     device_type string comment '设备类型',
                                                     avg_stay_duration double comment '平均停留时长(秒)',
                                                     conversion_rate double comment '路径转化率'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dwd/dwd_instore_path_flow'
    tblproperties('orc.compress'='snappy');

-- 首日全量初始化
insert overwrite table dwd_instore_path_flow partition(dt='2025-07-30')
select
    concat(sp.from_page_id, '_', sp.to_page_id, '_', sp.dt) as path_id,
    sp.from_page_id,
    -- 关联页面类型（适配来源≠去向场景）
    case
        when s.page_id is not null then 'shop_page'
        when p.page_id is not null then 'product_detail_page'
        else 'other_page'
        end as from_page_type,
    sp.to_page_id,
    case
        when s2.page_id is not null then 'shop_page'
        when p2.page_id is not null then 'product_detail_page'
        else 'other_page'
        end as to_page_type,
    sp.path_count,
    sp.device_type,
    sp.avg_stay_duration,
    sp.conversion_rate
from ods_store_path sp
         left join ods_shop s on sp.from_page_id = s.page_id and s.dt <= '2025-07-30'
         left join ods_product p on sp.from_page_id = p.page_id and p.dt <= '2025-07-30'
         left join ods_shop s2 on sp.to_page_id = s2.page_id and s2.dt <= '2025-07-30'
         left join ods_product p2 on sp.to_page_id = p2.page_id and p2.dt <= '2025-07-30'
where sp.dt <= '2025-07-30';

-- 2025-07-31日新增数据
insert overwrite table dwd_instore_path_flow partition(dt='2025-07-31')
select
    concat(sp.from_page_id, '_', sp.to_page_id, '_', sp.dt) as path_id,
    sp.from_page_id,
    case
        when s.page_id is not null then 'shop_page'
        when p.page_id is not null then 'product_detail_page'
        else 'other_page'
        end as from_page_type,
    sp.to_page_id,
    case
        when s2.page_id is not null then 'shop_page'
        when p2.page_id is not null then 'product_detail_page'
        else 'other_page'
        end as to_page_type,
    sp.path_count,
    sp.device_type,
    sp.avg_stay_duration,
    sp.conversion_rate
from ods_store_path sp
         left join ods_shop s on sp.from_page_id = s.page_id and s.dt = '2025-07-31'
         left join ods_product p on sp.from_page_id = p.page_id and p.dt = '2025-07-31'
         left join ods_shop s2 on sp.to_page_id = s2.page_id and s2.dt = '2025-07-31'
         left join ods_product p2 on sp.to_page_id=p2.page_id and p2.dt = '2025-07-31'
where sp.dt = '2025-07-31';

select * from dwd_instore_path_flow;



-- 3. PC 端流量入口明细表（dwd_pc_traffic_source）
-- 建表语句

drop table dwd_pc_traffic_source;
create table if not exists dwd_pc_traffic_source (
                                                     source_page_id string comment '来源页面ID',
                                                     source_type string comment '来源类型：direct(直接访问)/search(搜索)/social(社交)',
                                                     source_desc string comment '来源描述',
                                                     session_count bigint comment '会话数',
                                                     avg_session_duration double comment '平均会话时长(秒)',
                                                     source_ratio double comment '来源占比(%)'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dwd/dwd_pc_traffic_source'
    tblproperties('orc.compress'='snappy');

-- 首日全量初始化（计算来源占比）
insert overwrite table dwd_pc_traffic_source partition(dt='2025-07-30')
select
    ts.source_page_id,
    ts.source_type,
    case  -- 补充来源描述
        when ts.source_type = 'direct' then '直接访问'
        when ts.source_type = 'search' then '搜索引擎'
        when ts.source_type = 'social' then '社交平台'
        else '其他来源'
        end as source_desc,
    ts.session_count,
    ts.avg_session_duration,
    -- 计算来源占比（适配TOP20及占比图需求）
    round(ts.session_count / total.total_session * 100, 2) as source_ratio
from ods_traffic_source ts
         cross join (
    select sum(session_count) as total_session
    from ods_traffic_source
    where dt <= '2025-07-30'
) total
where ts.dt <= '2025-07-30';

select count(*) from ods_shop where dt = '2025-07-30';
select count(*) from ods_store_path where dt = '2025-07-30';
select count(*) from ods_traffic_source where dt = '2025-07-30';
select count(*) from ods_user_log ;

-- 2025-07-31日新增数据（计算来源占比）
insert overwrite table dwd_pc_traffic_source partition(dt='2025-07-31')
select
    ts.source_page_id,
    ts.source_type,
    case  -- 补充来源描述
        when ts.source_type = 'direct' then '直接访问'
        when ts.source_type = 'search' then '搜索引擎'
        when ts.source_type = 'social' then '社交平台'
        else '其他来源'
        end as source_desc,
    ts.session_count,
    ts.avg_session_duration,
    round(ts.session_count / total.total_session * 100, 2) as source_ratio
from ods_traffic_source ts
         cross join (
    select sum(session_count) as total_session
    from ods_traffic_source
    where dt = '2025-07-31'
) total
where ts.dt = '2025-07-31';



select * from dwd_pc_traffic_source;
