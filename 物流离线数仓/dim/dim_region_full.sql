create database if not exists tms01;
use tms;

--地区维度表

drop table if exists dim_region_full;
create external table dim_region_full(
  `id` bigint COMMENT '地区ID',
  `parent_id` bigint COMMENT '上级地区ID',
  `name` string COMMENT '地区名称',
  `dict_code` string COMMENT '编码（行政级别）',
  `short_name` string COMMENT '简称'
) comment '地区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_region_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_region_full
    partition (dt = '2025-07-12')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info
where ds = '20200623'
  and is_deleted = '0';

select * from dim_region_full;