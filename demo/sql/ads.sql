
set hive.exec.mode.local.auto=True;
create database if not exists ds_path;
use gd;
--1. 无线端入店承接指标表（ads_wireless_entry_kpi）
--对应指标：无线端进店页面及访客数、下单买家数（支持日 / 7 天 / 30 天维度）

create table if not exists ads_wireless_entry_kpi (
                                                      entry_page_id string comment '进店页面ID',
                                                      entry_page_type string comment '页面类型：shop_page/product_detail_page/other_page',
                                                      entry_page_name string comment '页面名称（如"首页"/"直播页"）',
    -- 多时间维度指标
                                                      visitor_count_1d bigint comment '当日访客数',
                                                      visitor_count_7d bigint comment '7日累计访客数',
                                                      visitor_count_30d bigint comment '30日累计访客数',
                                                      order_user_count_1d bigint comment '当日下单买家数',
                                                      order_user_count_7d bigint comment '7日累计下单买家数',
                                                      conversion_rate_1d double comment '当日转化率（下单数/访客数，%）',
    -- 辅助分析字段
                                                      entry_rank int comment '按当日访客数排名'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/ads/ads_wireless_entry_kpi'
    tblproperties('orc.compress'='snappy');

--1. 无线端入店承接指标表
insert overwrite table ads_wireless_entry_kpi partition(dt='2025-07-30')
select
    entry_page_id,
    entry_page_type,
    entry_page_name,
    -- 当日数据
    sum(case when dt='2025-07-30' then visitor_count else 0 end) as visitor_count_1d,
    -- 7日累计（2025-07-24至2025-07-30）
    sum(case when dt >= date_sub('2025-07-30',6) then visitor_count else 0 end) as visitor_count_7d,
    -- 30日累计（2025-07-01至2025-07-30）
    sum(case when dt >= date_sub('2025-07-30',29) then visitor_count else 0 end) as visitor_count_30d,
    sum(case when dt='2025-07-30' then order_user_count else 0 end) as order_user_count_1d,
    sum(case when dt >= date_sub('2025-07-30',6) then order_user_count else 0 end) as order_user_count_7d,
    round(
                    sum(case when dt='2025-07-30' then order_user_count else 0 end)
                    / sum(case when dt='2025-07-30' then visitor_count else 1 end) * 100, 2
        ) as conversion_rate_1d,
    row_number() over(order by sum(case when dt='2025-07-30' then visitor_count else 0 end) desc) as entry_rank
from dws_wireless_entry_stats
where dt <= '2025-07-30'
group by entry_page_id, entry_page_type, entry_page_name;

select  * from ads_wireless_entry_kpi;

--2. 页面访问排行（按访客数）表（ads_page_rank_by_visitor）
--对应指标：店铺页 / 商品详情页 / 店铺其他页按访客数排行

create table if not exists ads_page_rank_by_visitor (
                                                        page_type string comment '页面类型：shop_page/product_detail_page/other_page',
                                                        page_subtype string comment '子类型：home(首页)/activity(活动页)/product(商品)/subscribe(订阅页)等',
                                                        page_id string comment '页面ID',
                                                        page_name string comment '页面名称',
                                                        visitor_count bigint comment '访客数（去重）',
                                                        order_user_count bigint comment '下单买家数',
                                                        rank_in_type int comment '在同页面类型中的访客数排名（1为最高）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/ads/ads_page_rank_by_visitor'
    tblproperties('orc.compress'='snappy');

--2. 页面访问排行（按访客数）表
insert overwrite table ads_page_rank_by_visitor partition(dt='2025-07-30')
select
    page_type,
    page_subtype,
    page_id,
    page_name,
    visitor_count,
    order_user_count,
    row_number() over(partition by page_type order by visitor_count desc) as rank_in_type
from dws_page_visit_stats
where dt='2025-07-30';

select * from ads_page_rank_by_visitor;

--3. 页面访问排行（按浏览量）表（ads_page_rank_by_pv）
--对应指标：按浏览量排行，展示访客数、平均停留时长
create table if not exists ads_page_rank_by_pv (
                                                   page_type string comment '页面类型：shop_page/product_detail_page/other_page',
                                                   page_id string comment '页面ID',
                                                   page_name string comment '页面名称',
                                                   pv bigint comment '浏览量',
                                                   visitor_count bigint comment '访客数',
                                                   avg_stay_duration double comment '平均停留时长（秒）',
                                                   rank_in_type int comment '在同页面类型中的浏览量排名（1为最高）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/ads/ads_page_rank_by_pv'
    tblproperties('orc.compress'='snappy');

--3. 页面访问排行（按浏览量）表
insert overwrite table ads_page_rank_by_pv partition(dt='2025-07-30')
select
    page_type,
    page_id,
    page_name,
    pv,
    visitor_count,
    avg_stay_duration,
    row_number() over(partition by page_type order by pv desc) as rank_in_type
from dws_page_visit_stats
where dt='2025-07-30';

select * from ads_page_rank_by_pv;

--4. 店内路径流转指标表（ads_instore_path_kpi）
--对应指标：访客店内流转路径（支持来源 / 去向数据加总 > 总数据、来源≠去向）
create table if not exists ads_instore_path_kpi (
                                                    device_type string comment '设备类型：wireless/pc',
                                                    from_page_id string comment '来源页面ID',
                                                    from_page_type string comment '来源页面类型',
                                                    from_page_name string comment '来源页面名称',
                                                    to_page_id string comment '去向页面ID',
                                                    to_page_type string comment '去向页面类型',
                                                    to_page_name string comment '去向页面名称',
                                                    path_count bigint comment '路径流转次数',
                                                    path_ratio double comment '该路径占总路径的比例（%）',
                                                    avg_stay_duration double comment '平均停留时长（秒）'
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/ads/ads_instore_path_kpi'
    tblproperties('orc.compress'='snappy');

--4. 店内路径流转指标表
insert overwrite table ads_instore_path_kpi partition(dt='2025-07-30')
select
    s.device_type,
    s.from_page_id,
    s.from_page_type,
    -- 从关联表中获取来源页面名称，无匹配时显示NULL
    p1.page_name as from_page_name,
    s.to_page_id,
    s.to_page_type,
    -- 从关联表中获取去向页面名称，无匹配时显示NULL
    p2.page_name as to_page_name,
    s.path_count,
    round(s.path_count / total.total_path * 100, 2) as path_ratio,
    s.avg_stay_duration
from dws_instore_path_stats s
-- 关联来源页面的名称表
         left join dws_page_visit_stats p1
                   on s.from_page_id = p1.page_id
                       and p1.dt = '2025-07-30'  -- 确保时间匹配
-- 关联去向页面的名称表
         left join dws_page_visit_stats p2
                   on s.to_page_id = p2.page_id
                       and p2.dt = '2025-07-30'  -- 确保时间匹配
-- 关联总路径次数（用于计算占比）
         cross join (
    select sum(path_count) as total_path
    from dws_instore_path_stats
    where dt = '2025-07-30'
) total
where s.dt = '2025-07-30';


select * from ads_instore_path_kpi;
--5. PC 端流量入口 TOP20 表（ads_pc_source_top20_kpi）
--对应指标：PC 端来源页面 TOP20 及占比图

-- 创建PC端流量入口TOP20表（修正表结构注释及字段兼容性）
create table if not exists ads_pc_source_top20_kpi (
                                                       source_page_id string comment '来源页面ID',
                                                       source_type string comment '来源类型：direct(直接访问)/search(搜索)/social(社交)/other(其他)', -- 补充other类型兼容所有场景
                                                       source_desc string comment '来源描述（如"百度搜索"/"微博分享"/"未知来源"）', -- 补充未知来源描述
                                                       session_count bigint comment '会话数（访问次数）',
                                                       source_ratio double comment '来源占比（%，保留2位小数）',
                                                       visitor_count bigint comment '带来的访客数（去重用户数）',
                                                       rank_num int comment '排名（1-20，按会话数降序）' -- 明确排名规则
) partitioned by (dt string)
    stored as orc
    location '/warehouse/gd_tms/ads/ads_pc_source_top20_kpi'
    tblproperties(
        'orc.compress'='snappy',
        'comment'='PC端流量入口TOP20分析表，支撑来源页面排行及占比分析' -- 补充表级注释
        );

-- 插入PC端流量入口TOP20数据（修正排序逻辑及空值处理）
INSERT OVERWRITE TABLE ads_pc_source_top20_kpi PARTITION(dt='2025-07-30')
SELECT
    source_page_id,
    -- 处理空值，确保来源类型不为null
    CASE WHEN source_type IS NULL THEN 'other' ELSE source_type END AS source_type,
    -- 处理空值，补充默认描述
    CASE WHEN source_desc IS NULL THEN '未知来源' ELSE source_desc END AS source_desc,
    session_count,
    -- 确保占比保留2位小数，空值补0
    ROUND(NVL(source_ratio, 0), 2) AS source_ratio,
    -- 访客数逻辑替换：使用默认值0（因原表无此列）
    0 AS visitor_count,  -- 原表无visitor_count，用默认值替代
    rank_num
FROM (
         -- 子查询中按排名升序取前20，确保排名连续
         SELECT
             source_page_id,
             source_type,
             source_desc,
             session_count,
             source_ratio,
             rank_num
         FROM dws_pc_source_stats
         WHERE dt='2025-07-30'
         ORDER BY rank_num ASC
         LIMIT 20
     ) t;


select * from ads_pc_source_top20_kpi;
