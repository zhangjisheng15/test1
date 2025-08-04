
--1. 页面访问指标汇总表（dws_page_visit_stats）
--用途：按页面类型、设备类型汇总访问指标，支撑 “页面访问排行”（按访客数 / 浏览量排序）。
set hive.exec.mode.local.auto=True;
use gd;

create table if not exists dws_page_visit_stats (
                                                    page_type string comment '页面类型：shop_page(店铺页)/product_detail_page(商品详情页)/other_page(店铺其他页)',
                                                    page_subtype string comment '页面子类型：home(首页)/activity(活动页)/product(商品)/subscribe(订阅页)等',
                                                    page_id string comment '页面ID',
                                                    page_name string comment '页面名称',
                                                    device_type string comment '设备类型：wireless(无线端)/pc(PC端)',
                                                    visitor_count bigint comment '访客数（去重用户数）',
                                                    pv bigint comment '浏览量（页面访问次数）',
                                                    avg_stay_duration double comment '平均停留时长(秒)',
                                                    order_user_count bigint comment '下单买家数（去重下单用户）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dws/dws_page_visit_stats'
    tblproperties('orc.compress'='snappy');

-- 汇总历史全量数据（截至2025-07-30），支撑多时间维度排行分析
insert overwrite table dws_page_visit_stats partition(dt='2025-07-30')
select
    page_type,
    page_subtype,
    page_id,
    page_name,
    device_type,
    count(distinct user_id) as visitor_count,  -- 访客数（去重）
    count(log_id) as pv,  -- 浏览量
    avg(stay_duration) as avg_stay_duration,  -- 平均停留时长
    count(distinct case when is_order=1 then user_id end) as order_user_count  -- 下单买家数
from dwd_page_visit_detail
where dt <= '2025-07-30'
group by page_type, page_subtype, page_id, page_name, device_type;

-- 2025-07-31日新增数据，按天增量处理
insert overwrite table dws_page_visit_stats partition(dt='2025-07-31')
select
    page_type,
    page_subtype,
    page_id,
    page_name,
    device_type,
    count(distinct user_id) as visitor_count,  -- 访客数（去重）
    count(log_id) as pv,  -- 浏览量
    avg(stay_duration) as avg_stay_duration,  -- 平均停留时长
    count(distinct case when is_order=1 then user_id end) as order_user_count
from dwd_page_visit_detail
where dt = '2025-07-31'
group by page_type, page_subtype, page_id, page_name, device_type;


select  * from dws_page_visit_stats;

--2. 店内路径流转汇总表（dws_instore_path_stats）
--用途：汇总页面间流转指标，支撑 “店内路径” 分析（来源 / 去向页面、路径次数等），适配 “数据加总 > 总数据”“来源≠去向” 场景。
create table if not exists dws_instore_path_stats (
                                                      from_page_id string comment '来源页面ID',
                                                      from_page_type string comment '来源页面类型',
                                                      to_page_id string comment '去向页面ID',
                                                      to_page_type string comment '去向页面类型',
                                                      device_type string comment '设备类型',
                                                      path_count bigint comment '路径流转总次数',
                                                      avg_stay_duration double comment '平均停留时长(秒)',
                                                      conversion_rate double comment '路径转化率（下单数/访问数）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dws/dws_instore_path_stats'
    tblproperties('orc.compress'='snappy');

-- 汇总历史路径数据，保留“来源≠去向”“数据加总>总数据”特性
insert overwrite table dws_instore_path_stats partition(dt='2025-07-30')
select
    from_page_id,
    from_page_type,
    to_page_id,
    to_page_type,
    device_type,
    sum(path_count) as path_count,  -- 汇总路径次数（不查重）
    avg(avg_stay_duration) as avg_stay_duration,
    avg(conversion_rate) as conversion_rate
from dwd_instore_path_flow
where dt <= '2025-07-30'
group by from_page_id, from_page_type, to_page_id, to_page_type, device_type;

-- 2025-07-31日新增数据，按天增量处理
insert overwrite table dws_instore_path_stats partition(dt='2025-07-31')
select
    from_page_id,
    from_page_type,
    to_page_id,
    to_page_type,
    device_type,
    sum(path_count) as path_count,  -- 汇总路径次数（不查重）
    avg(avg_stay_duration) as avg_stay_duration,
    avg(conversion_rate) as conversion_rate
from dwd_instore_path_flow
where dt = '2025-07-31'
group by from_page_id, from_page_type, to_page_id, to_page_type, device_type;

select  * from dws_instore_path_stats;
--3. 无线端入店承接汇总表（dws_wireless_entry_stats）
--用途：专门汇总无线端入店指标，支撑 “无线入店与承接” 分析（进店页面、访客数、下单数等）。
create table if not exists dws_wireless_entry_stats (
                                                        entry_page_id string comment '进店页面ID',
                                                        entry_page_type string comment '进店页面类型',
                                                        entry_page_name string comment '进店页面名称',
                                                        visitor_count bigint comment '无线端访客数',
                                                        order_user_count bigint comment '无线端下单买家数',
                                                        conversion_rate double comment '无线端下单转化率（下单数/访客数）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dws/dws_wireless_entry_stats'
    tblproperties('orc.compress'='snappy');

-- 汇总无线端历史入店数据，支撑无线端分析
insert overwrite table dws_wireless_entry_stats partition(dt='2025-07-30')
select
    refer_page_id as entry_page_id,
    case when refer_page_id is null then page_type else page_type end as entry_page_type,
    case when refer_page_id is null then page_name else page_name end as entry_page_name,
    count(distinct user_id) as visitor_count,
    count(distinct case when is_order=1 then user_id end) as order_user_count,
    round(
                    count(distinct case when is_order=1 then user_id end) / count(distinct user_id) * 100, 2
        ) as conversion_rate
from dwd_page_visit_detail
where device_type='wireless' and dt <= '2025-07-30'
group by refer_page_id, page_type, page_name;

-- 2025-07-31日新增数据，按天增量处理
insert overwrite table dws_wireless_entry_stats partition(dt='2025-07-31')
select
    refer_page_id as entry_page_id,
    case when refer_page_id is null then page_type else page_type end as entry_page_type,
    case when refer_page_id is null then page_name else page_name end as entry_page_name,
    count(distinct user_id) as visitor_count,
    count(distinct case when is_order=1 then user_id end) as order_user_count,
    round(
                    count(distinct case when is_order=1 then user_id end) / count(distinct user_id) * 100, 2
        ) as conversion_rate
from dwd_page_visit_detail
where device_type='wireless' and dt = '2025-07-31'
group by refer_page_id, page_type, page_name;

select  * from dws_wireless_entry_stats;
--4. PC端来源汇总表（dws_pc_source_stats）
create table if not exists dws_pc_source_stats (
                                                   source_page_id string comment '来源页面ID',
                                                   source_type string comment '来源类型：direct(直接访问)/search(搜索)/social(社交)',
                                                   source_desc string comment '来源描述',
                                                   session_count bigint comment '会话数',
                                                   avg_session_duration double comment '平均会话时长(秒)',
                                                   source_ratio double comment '来源占比(%)',
                                                   rank_num int comment '来源页面排名（1-20）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/dws/dws_pc_source_stats'
    tblproperties('orc.compress'='snappy');

-- 汇总PC端历史来源数据，计算排名（支撑TOP20）
insert overwrite table dws_pc_source_stats partition(dt='2025-07-30')
select
    source_page_id,
    source_type,
    source_desc,
    session_count,
    avg_session_duration,
    source_ratio,
    row_number() over(order by session_count desc) as rank_num  -- 按会话数排序取TOP20
from dwd_pc_traffic_source
where dt <= '2025-07-30'
order by session_count desc
limit 20;  -- 仅保留TOP20来源页面


-- 2025-07-31日新增数据，按天增量处理
insert overwrite table dws_pc_source_stats partition(dt='2025-07-31')
select
    source_page_id,
    source_type,
    source_desc,
    session_count,
    avg_session_duration,
    source_ratio,
    row_number() over(order by session_count desc) as rank_num  -- 按会话数排序取TOP20
from dwd_pc_traffic_source
where dt <= '2025-07-31'
order by session_count desc
limit 20;  -- 仅保留TOP20来源页面

select  * from dws_pc_source_stats;
