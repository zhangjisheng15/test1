create database if not exists tms;
use tms;

-- 小区维度表
drop table if exists dim_complex_full;
create external table dim_complex_full(
  `id` bigint comment '小区ID',
  `complex_name` string comment '小区名称',
  `courier_emp_ids` array<string> comment '负责快递员IDS',
  `province_id` bigint comment '省份ID',
  `province_name` string comment '省份名称',
  `city_id` bigint comment '城市ID',
  `city_name` string comment '城市名称',
  `district_id` bigint comment '区（县）ID',
  `district_name` string comment '区（县）名称'
) comment '小区维度表'
  partitioned by (`dt` string comment '统计日期')
  stored as orc
  location '/warehouse/tms/dim/dim_complex_full'
  tblproperties('orc.compress'='snappy');

-- 插入数据（修正后的查询）
insert overwrite table dim_complex_full
    partition (dt = '2025-07-12')
select
    complex_info.id as id,
    complex_info.complex_name,
    complex_courier.courier_emp_ids,
    complex_info.province_id,
    dic_prov.name as province_name,
    complex_info.city_id,
    dic_city.name as city_name,
    complex_info.district_id,
    complex_info.district_name
from (
    select
        id,
        complex_name,
        province_id,
        city_id,
        district_id,
        district_name
    from ods_base_complex
    where ds = '20250719'
        and is_deleted = '0'
) complex_info
-- 关联省份信息
join (
    select
        id,
        name
    from ods_base_region_info
    where ds = '20200623'
        and is_deleted = '0'
) dic_prov
on complex_info.province_id = dic_prov.id
-- 关联城市信息
join (
    select
        id,
        name
    from ods_base_region_info
    where ds = '20200623'
        and is_deleted = '0'
) dic_city
on complex_info.city_id = dic_city.id
-- 左关联快递员信息
left join (
    select
        complex_id,
        collect_set(cast(courier_emp_id as string)) as courier_emp_ids
    from ods_express_courier_complex
    where ds = '20230105'
        and is_deleted = '0'
    group by complex_id
) complex_courier
on complex_info.id = complex_courier.complex_id;

-- 验证数据
select * from dim_complex_full limit 10;

