
create database if not exists tms01;
use tms;

-- 用户维度表
drop table if exists dim_user_zip;
create external table dim_user_zip(
                                      `id` bigint COMMENT '用户地址信息ID',
                                      `login_name` string COMMENT '用户名称',
                                      `nick_name` string COMMENT '用户昵称',
                                      `passwd` string COMMENT '用户密码',
                                      `real_name` string COMMENT '用户姓名',
                                      `phone_num` string COMMENT '手机号',
                                      `email` string COMMENT '邮箱',
                                      `user_level` string COMMENT '用户级别',
                                      `birthday` string COMMENT '用户生日',
                                      `gender` string COMMENT '性别 M男,F女',
                                      `start_date` string COMMENT '起始日期',
                                      `end_date` string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');

-- 3）首日装载
insert overwrite table dim_user_zip
    partition (dt = '2025-07-12')
select after.id,
       after.login_name,
       after.nick_name,
       after.passwd,
       after.real_name,
       after.phone_num,
       after.email,
       after.user_level,
       date_add('1970-01-01', cast(after.birthday as int)) birthday,
       after.gender,
       date_format(from_utc_timestamp(
                           cast(after.create_time as bigint), 'UTC'),
                   'yyyy-MM-dd') start_date,
       '9999-12-31' end_date
from ods_user_info after
where ds = '20230105'
  and after.is_deleted = '0';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip
    partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-13', -1)) end_date,
       if(rk = 1, end_date, date_add('2025-07-13', -1)) dt
from (
         select
             id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
         from (
                  -- 子查询 1：从拉链表取历史数据（这里原条件 dt='2025-07-13' 可能有问题，首日装载 dt 是 2025-07-12，按需调整，假设是取已有分区数据）
                  select
                      id,
                      login_name,
                      nick_name,
                      passwd,
                      real_name,
                      phone_num,
                      email,
                      user_level,
                      birthday,
                      gender,
                      start_date,
                      end_date
                  from dim_user_zip
                  where dt = '2025-07-12'  -- 结合首日装载逻辑，调整为已存在的分区，比如首日的 2025-07-12
                  union all  -- 用 union all 更高效，且明确是合并结果，union 会去重，这里不需要
                  -- 子查询 2：从 ods 取增量数据
                  select
                      inc_inner.id,
                      inc_inner.login_name,
                      inc_inner.nick_name,
                      inc_inner.passwd,  -- 原错误处修正，确保列对应
                      inc_inner.real_name,
                      inc_inner.phone_num,
                      inc_inner.email,
                      inc_inner.user_level,
                      cast(date_add('1970-01-01', cast(inc_inner.birthday as int)) as string) birthday,
                      inc_inner.gender,
                      '2025-07-13' start_date,
                      '9999-12-31' end_date
                  from (
                           select
                               after.id,
                               after.login_name,
                               after.nick_name,
                               after.passwd,
                               after.real_name,
                               after.phone_num,
                               after.email,
                               after.user_level,
                               after.birthday,
                               after.gender,
                               row_number() over (partition by after.id order by ds desc) rn
                           from ods_user_info after
                           where ds = '20230105'
                             and after.is_deleted = '0'
                       ) inc_inner
                  where inc_inner.rn = 1
              ) full_info
     ) final_info;

select * from dim_user_zip;

create database if not exists tms01;
use tms01;

-- 13. 中转域出库事务事实表
drop table if exists dwd_bound_outbound_inc;
create external table dwd_bound_outbound_inc(
                                                `id` bigint COMMENT '中转记录ID',
                                                `order_id` bigint COMMENT '订单ID',
                                                `org_id` bigint COMMENT '机构ID',
                                                `outbound_time` string COMMENT '出库时间',
                                                `outbound_emp_id` bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_bound_outbound_inc'
    tblproperties('orc.compress' = 'snappy');

-- 13.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_outbound_inc
    partition (dt)
select id,
       order_id,
       org_id,
       outbound_time,
       outbound_emp_id,
       '2025-07-12'dt
from ods_order_org_bound
where ds = '20230105';

select * from dwd_bound_outbound_inc;

-- 13.2 每日装载
insert overwrite table dwd_bound_outbound_inc
    partition (dt = '2023-01-11')
select id,
       order_id,
       org_id,
       outbound_time,
       outbound_emp_id
from ods_order_org_bound
where ds = '20230105'
  and outbound_time is null
  and outbound_time is not null
  and is_deleted = '0';

select * from dwd_bound_outbound_inc;

create database if not exists tms01;
use tms01;


-- 12. 中转域分拣事务事实表
drop table if exists dwd_bound_sort_inc;
create external table dwd_bound_sort_inc(
                                            `id` bigint COMMENT '中转记录ID',
                                            `order_id` bigint COMMENT '订单ID',
                                            `org_id` bigint COMMENT '机构ID',
                                            `sort_time` string COMMENT '分拣时间',
                                            `sorter_emp_id` bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_bound_sort_inc'
    tblproperties('orc.compress' = 'snappy');

-- 12.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_sort_inc
    partition (dt)
select id,
       order_id,
       org_id,
       sort_time,
       sorter_emp_id,
       '2025-07-12' dt
from ods_order_org_bound
where ds = '20230105';

-- 12.2 每日装载
insert overwrite table dwd_bound_sort_inc
    partition (dt = '2023-01-11')
select id,
       order_id,
       org_id,
       sort_time,
       sorter_emp_id
from ods_order_org_bound
where ds = '20230105'
  and sort_time is null
  and sort_time is not null
  and is_deleted = '0';

select * from dwd_bound_sort_inc;

create database if not exists tms01;
use tms01;

-- 3. 交易域取消运单事务事实表
drop table if exists dwd_trade_order_cancel_detail_inc;
create external table dwd_trade_order_cancel_detail_inc(
                                                           `id` bigint comment '运单明细ID',
                                                           `order_id` string COMMENT '运单ID',
                                                           `cargo_type` string COMMENT '货物类型ID',
                                                           `cargo_type_name` string COMMENT '货物类型名称',
                                                           `volume_length` bigint COMMENT '长cm',
                                                           `volume_width` bigint COMMENT '宽cm',
                                                           `volume_height` bigint COMMENT '高cm',
                                                           `weight` decimal(16,2) COMMENT '重量 kg',
                                                           `cancel_time` string COMMENT '取消时间',
                                                           `order_no` string COMMENT '运单号',
                                                           `status` string COMMENT '运单状态',
                                                           `status_name` string COMMENT '运单状态名称',
                                                           `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                           `collect_type_name` string COMMENT '取件类型名称',
                                                           `user_id` bigint COMMENT '用户ID',
                                                           `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                           `receiver_province_id` string COMMENT '收件人省份id',
                                                           `receiver_city_id` string COMMENT '收件人城市id',
                                                           `receiver_district_id` string COMMENT '收件人区县id',
                                                           `receiver_name` string COMMENT '收件人姓名',
                                                           `sender_complex_id` bigint COMMENT '发件人小区id',
                                                           `sender_province_id` string COMMENT '发件人省份id',
                                                           `sender_city_id` string COMMENT '发件人城市id',
                                                           `sender_district_id` string COMMENT '发件人区县id',
                                                           `sender_name` string COMMENT '发件人姓名',
                                                           `cargo_num` bigint COMMENT '货物个数',
                                                           `amount` decimal(16,2) COMMENT '金额',
                                                           `estimate_arrive_time` string COMMENT '预计到达时间',
                                                           `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '交易域取消运单事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_order_cancel_detail_inc'
    tblproperties('orc.compress' = 'snappy');

-- 3.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;

select * from dwd_trade_order_cancel_detail_inc;

insert overwrite table dwd_trade_order_cancel_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(cancel_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) cancel_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'
        and status = '60050') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20190610'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20190610'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20190610'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

-- 3.2 每日装载
with cancel_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 cancel_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') cancel_time
                from ods_order_info
                where ds = '20230105'
                  and status = '60050'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20190610'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '2025-07-12'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105') cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105'
              ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trade_order_cancel_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_info.cancel_time,
       order_info.order_no,
       cancel_info.status,
       cancel_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance
from cancel_info
         join order_info
              on cancel_info.id = order_info.order_id;

select * from dwd_trade_order_cancel_detail_inc;

-- 设置本地模式和动态分区
set hive.exec.dynamic.partition.mode=nonstrict;

-- 创建数据库和表（保持不变）
create database if not exists tms01;
use tms01;

drop table if exists dwd_trade_order_detail_inc;
create external table dwd_trade_order_detail_inc(
                                                    `id` bigint comment '运单明细ID',
                                                    `order_id` string COMMENT '运单ID',
                                                    `cargo_type` string COMMENT '货物类型ID',
                                                    `cargo_type_name` string COMMENT '货物类型名称',
                                                    `volume_length` bigint COMMENT '长cm',
                                                    `volume_width` bigint COMMENT '宽cm',
                                                    `volume_height` bigint COMMENT '高cm',
                                                    `weight` decimal(16,2) COMMENT '重量 kg',
                                                    `order_time` string COMMENT '下单时间',
                                                    `order_no` string COMMENT '运单号',
                                                    `status` string COMMENT '运单状态',
                                                    `status_name` string COMMENT '运单状态名称',
                                                    `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                    `collect_type_name` string COMMENT '取件类型名称',
                                                    `user_id` bigint COMMENT '用户ID',
                                                    `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                    `receiver_province_id` string COMMENT '收件人省份id',
                                                    `receiver_city_id` string COMMENT '收件人城市id',
                                                    `receiver_district_id` string COMMENT '收件人区县id',
                                                    `receiver_name` string COMMENT '收件人姓名',
                                                    `sender_complex_id` bigint COMMENT '发件人小区id',
                                                    `sender_province_id` string COMMENT '发件人省份id',
                                                    `sender_city_id` string COMMENT '发件人城市id',
                                                    `sender_district_id` string COMMENT '发件人区县id',
                                                    `sender_name` string COMMENT '发件人姓名',
                                                    `cargo_num` bigint COMMENT '货物个数',
                                                    `amount` decimal(16,2) COMMENT '金额',
                                                    `estimate_arrive_time` string COMMENT '预计到达时间',
                                                    `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '交易域订单明细事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_order_detail_inc'
    tblproperties('orc.compress' = 'snappy');

-- 修正后的INSERT语句
insert overwrite table dwd_trade_order_detail_inc
    partition (dt)
select
    cargo.id,
    order_id,
    cargo_type,
    dic_for_cargo_type.name               as cargo_type_name,
    volume_length,
    volume_width,
    volume_height,
    weight,
    order_time,
    order_no,
    status,
    dic_for_status.name                   as status_name,
    collect_type,
    dic_for_collect_type.name             as collect_type_name,
    user_id,
    receiver_complex_id,
    receiver_province_id,
    receiver_city_id,
    receiver_district_id,
    receiver_name,
    sender_complex_id,
    sender_province_id,
    sender_city_id,
    sender_district_id,
    sender_name,
    cargo_num,
    amount,
    estimate_arrive_time,
    distance,
    date_format(to_date(order_time), 'yyyy-MM-dd') as dt  -- 确保日期格式正确
from (
         select
             id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) as order_time
         from ods_order_cargo
         where ds = '20230105'
           and is_deleted = '0'
     ) cargo
         join (
    select
        id,
        order_no,
        status,
        collect_type,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        concat(substr(receiver_name, 1, 1), '*') as receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        concat(substr(sender_name, 1, 1), '*') as sender_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance
    from ods_order_info
    where ds = '20230105'
      and is_deleted = '0'
) info
              on cargo.order_id = info.id
         left join (
    select
        id,
        name
    from ods_base_dic
    where ds = '20220708'
      and is_deleted = '0'
) dic_for_cargo_type
                   on cargo.cargo_type = dic_for_cargo_type.id  -- 直接使用ID，避免类型转换
         left join (
    select
        id,
        name
    from ods_base_dic
    where ds = '20220708'
      and is_deleted = '0'
) dic_for_status
                   on info.status = dic_for_status.id  -- 直接使用ID，避免类型转换
         left join (
    select
        id,
        name
    from ods_base_dic
    where ds = '20220708'
      and is_deleted = '0'
) dic_for_collect_type
                   on info.collect_type = dic_for_collect_type.id;  -- 修正为dic_for_collect_type

-- 查询验证
select * from dwd_trade_order_detail_inc limit 10;


create database if not exists tms01;
use tms01;

-- 交易域运单累积快照事实表
drop table if exists  dwd_trade_order_process_inc;
create external table dwd_trade_order_process_inc(
                                                     `id` bigint comment '运单明细ID',
                                                     `order_id` string COMMENT '运单ID',
                                                     `cargo_type` string COMMENT '货物类型ID',
                                                     `cargo_type_name` string COMMENT '货物类型名称',
                                                     `volume_length` bigint COMMENT '长cm',
                                                     `volume_width` bigint COMMENT '宽cm',
                                                     `volume_height` bigint COMMENT '高cm',
                                                     `weight` decimal(16,2) COMMENT '重量 kg',
                                                     `order_time` string COMMENT '下单时间',
                                                     `order_no` string COMMENT '运单号',
                                                     `status` string COMMENT '运单状态',
                                                     `status_name` string COMMENT '运单状态名称',
                                                     `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                     `collect_type_name` string COMMENT '取件类型名称',
                                                     `user_id` bigint COMMENT '用户ID',
                                                     `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                     `receiver_province_id` string COMMENT '收件人省份id',
                                                     `receiver_city_id` string COMMENT '收件人城市id',
                                                     `receiver_district_id` string COMMENT '收件人区县id',
                                                     `receiver_name` string COMMENT '收件人姓名',
                                                     `sender_complex_id` bigint COMMENT '发件人小区id',
                                                     `sender_province_id` string COMMENT '发件人省份id',
                                                     `sender_city_id` string COMMENT '发件人城市id',
                                                     `sender_district_id` string COMMENT '发件人区县id',
                                                     `sender_name` string COMMENT '发件人姓名',
                                                     `payment_type` string COMMENT '支付方式',
                                                     `payment_type_name` string COMMENT '支付方式名称',
                                                     `cargo_num` bigint COMMENT '货物个数',
                                                     `amount` decimal(16,2) COMMENT '金额',
                                                     `estimate_arrive_time` string COMMENT '预计到达时间',
                                                     `distance` decimal(16,2) COMMENT '距离，单位：公里',
                                                     `start_date` string COMMENT '开始日期',
                                                     `end_date` string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_order_process'
    tblproperties('orc.compress' = 'snappy');

-- 2）首日装载（2025-07-12）
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_process_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_status.name              collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       end_date                              dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance,
             if(status = '60080' or
                status = '60999',
                concat(substr(update_time, 1, 10)),
                '9999-12-31')                               end_date
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 9.2 每日装载
with tmp
         as
         (select id,
                 order_id,
                 cargo_type,
                 cargo_type_name,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight,
                 order_time,
                 order_no,
                 status,
                 status_name,
                 collect_type,
                 collect_type_name,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 start_date,
                 end_date
          from dwd_trade_order_process_inc
          where dt = '2025-07-12'
          union
          select cargo.id,
                 order_id,
                 cargo_type,
                 dic_for_cargo_type.name               cargo_type_name,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight,
                 order_time,
                 order_no,
                 status,
                 dic_for_status.name                   status_name,
                 collect_type,
                 dic_for_collect_type.name             collect_type_name,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_for_payment_type.name             payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 date_format(order_time, 'yyyy-MM-dd') start_date,
                 '9999-12-31'                          end_date
          from (select id,
                       order_id,
                       cargo_type,
                       volume_length,
                       volume_width,
                       volume_height,
                       weight,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                    substr(create_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
                from ods_order_cargo
                where ds = '20230105') cargo
                   join
               (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*') receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')   sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                       distance
                from ods_order_info
                where ds = '20230105') info
               on cargo.order_id = info.id
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_cargo_type
               on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on info.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_collect_type
               on info.collect_type = cast(dic_for_cargo_type.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_payment_type
               on info.payment_type = cast(dic_for_payment_type.id as string)),
     inc
         as
         (select without_type_name.id,
                 status,
                 payment_type,
                 dic_for_payment_type.name payment_type_name
          from (select id,
                       status,
                       payment_type
                from (select id,
                             status,
                             payment_type,
                             row_number() over (partition by id order by ds desc) rn
                      from ods_order_info
                      where ds = '20230105'
                        and is_deleted = '0'
                     ) inc_origin
                where rn = 1) without_type_name
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_payment_type
               on without_type_name.payment_type = cast(dic_for_payment_type.id as string)
         )
insert overwrite table dwd_trade_order_process_inc
partition(dt)
select tmp.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       inc.status,
       status_name,
       collect_type,
       collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       inc.payment_type,
       inc.payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       start_date,
       if(inc.status = '60080' or
          inc.status = '60999',
          '2025-07-13', tmp.end_date) end_date,
       if(inc.status = '60080' or
          inc.status = '60999',
          '2025-07-13', tmp.end_date) dt
from tmp
         left join inc
                   on tmp.order_id = inc.id;

select count(*)
from dwd_trade_order_process_inc;


create database if not exists tms01;
use tms01;
drop table if exists dwd_trade_pay_suc_detail_inc;
create external table dwd_trade_pay_suc_detail_inc(
                                                      `id` bigint comment '运单明细ID',
                                                      `order_id` string COMMENT '运单ID',
                                                      `cargo_type` string COMMENT '货物类型ID',
                                                      `cargo_type_name` string COMMENT '货物类型名称',
                                                      `volume_length` bigint COMMENT '长cm',
                                                      `volume_width` bigint COMMENT '宽cm',
                                                      `volume_height` bigint COMMENT '高cm',
                                                      `weight` decimal(16,2) COMMENT '重量 kg',
                                                      `payment_time` string COMMENT '支付时间',
                                                      `order_no` string COMMENT '运单号',
                                                      `status` string COMMENT '运单状态',
                                                      `status_name` string COMMENT '运单状态名称',
                                                      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                      `collect_type_name` string COMMENT '取件类型名称',
                                                      `user_id` bigint COMMENT '用户ID',
                                                      `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                      `receiver_province_id` string COMMENT '收件人省份id',
                                                      `receiver_city_id` string COMMENT '收件人城市id',
                                                      `receiver_district_id` string COMMENT '收件人区县id',
                                                      `receiver_name` string COMMENT '收件人姓名',
                                                      `sender_complex_id` bigint COMMENT '发件人小区id',
                                                      `sender_province_id` string COMMENT '发件人省份id',
                                                      `sender_city_id` string COMMENT '发件人城市id',
                                                      `sender_district_id` string COMMENT '发件人区县id',
                                                      `sender_name` string COMMENT '发件人姓名',
                                                      `payment_type` string COMMENT '支付方式',
                                                      `payment_type_name` string COMMENT '支付方式名称',
                                                      `cargo_num` bigint COMMENT '货物个数',
                                                      `amount` decimal(16,2) COMMENT '金额',
                                                      `estimate_arrive_time` string COMMENT '预计到达时间',
                                                      `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '交易域支付成功事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_pay_suc_detail_inc'
    tblproperties('orc.compress' = 'snappy');

select * from dwd_trade_pay_suc_detail_inc;

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       payment_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(payment_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) payment_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 2.2 每日装载
with pay_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 payment_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') payment_time
                from ods_order_info
                where ds = '20230105'
                  and status = '60010'
                  and status = '60020'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and status = '60010'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105') cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105') info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trade_pay_suc_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       pay_info.payment_time,
       order_info.order_no,
       pay_info.status,
       pay_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       pay_info.payment_type,
       pay_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance

from pay_info
         join order_info
              on pay_info.id = order_info.order_id;

select * from dwd_trade_pay_suc_detail_inc;

create database if not exists tms01;
use tms01;

-- 6. 物流域转运完成事务事实表
drop table if exists dwd_trans_bound_finish_detail_inc;
create external table dwd_trans_bound_finish_detail_inc(
                                                           `id` bigint comment '运单明细ID',
                                                           `order_id` string COMMENT '运单ID',
                                                           `cargo_type` string COMMENT '货物类型ID',
                                                           `cargo_type_name` string COMMENT '货物类型名称',
                                                           `volume_length` bigint COMMENT '长cm',
                                                           `volume_width` bigint COMMENT '宽cm',
                                                           `volume_height` bigint COMMENT '高cm',
                                                           `weight` decimal(16,2) COMMENT '重量 kg',
                                                           `bound_finish_time` string COMMENT '转运完成时间',
                                                           `order_no` string COMMENT '运单号',
                                                           `status` string COMMENT '运单状态',
                                                           `status_name` string COMMENT '运单状态名称',
                                                           `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                           `collect_type_name` string COMMENT '取件类型名称',
                                                           `user_id` bigint COMMENT '用户ID',
                                                           `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                           `receiver_province_id` string COMMENT '收件人省份id',
                                                           `receiver_city_id` string COMMENT '收件人城市id',
                                                           `receiver_district_id` string COMMENT '收件人区县id',
                                                           `receiver_name` string COMMENT '收件人姓名',
                                                           `sender_complex_id` bigint COMMENT '发件人小区id',
                                                           `sender_province_id` string COMMENT '发件人省份id',
                                                           `sender_city_id` string COMMENT '发件人城市id',
                                                           `sender_district_id` string COMMENT '发件人区县id',
                                                           `sender_name` string COMMENT '发件人姓名',
                                                           `payment_type` string COMMENT '支付方式',
                                                           `payment_type_name` string COMMENT '支付方式名称',
                                                           `cargo_num` bigint COMMENT '货物个数',
                                                           `amount` decimal(16,2) COMMENT '金额',
                                                           `estimate_arrive_time` string COMMENT '预计到达时间',
                                                           `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '物流域转运完成事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_bound_finish_detail_inc'
    tblproperties('orc.compress' = 'snappy');

-- 6.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_bound_finish_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       bound_finish_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(bound_finish_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) bound_finish_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'
        and status <> '60030'
        and status <> '60020'
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trans_bound_finish_detail_inc;

-- 6.2 每日装载
with bound_finish_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 bound_finish_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') bound_finish_time

                from ods_order_info
                where ds = '20230105'
                  and status = '60050'
                  and status = '60040'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '2025-07-12'
           and (status = '60050' or
                status = '60040' or
                status = '60030' or
                status = '60020')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105'
              ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105'
              ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trans_bound_finish_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       bound_finish_info.bound_finish_time,
       order_info.order_no,
       bound_finish_info.status,
       bound_finish_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       bound_finish_info.payment_type,
       bound_finish_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance
from bound_finish_info
         join order_info
              on bound_finish_info.id = order_info.order_id;

select  * from dwd_trans_bound_finish_detail_inc;

create database if not exists tms01;
use tms01;

-- 7. 物流域派送成功事务事实表
drop table if exists dwd_trans_deliver_suc_detail_inc;
create external table dwd_trans_deliver_suc_detail_inc(
                                                          `id` bigint comment '运单明细ID',
                                                          `order_id` string COMMENT '运单ID',
                                                          `cargo_type` string COMMENT '货物类型ID',
                                                          `cargo_type_name` string COMMENT '货物类型名称',
                                                          `volume_length` bigint COMMENT '长cm',
                                                          `volume_width` bigint COMMENT '宽cm',
                                                          `volume_height` bigint COMMENT '高cm',
                                                          `weight` decimal(16,2) COMMENT '重量 kg',
                                                          `deliver_suc_time` string COMMENT '派送成功时间',
                                                          `order_no` string COMMENT '运单号',
                                                          `status` string COMMENT '运单状态',
                                                          `status_name` string COMMENT '运单状态名称',
                                                          `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                          `collect_type_name` string COMMENT '取件类型名称',
                                                          `user_id` bigint COMMENT '用户ID',
                                                          `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                          `receiver_province_id` string COMMENT '收件人省份id',
                                                          `receiver_city_id` string COMMENT '收件人城市id',
                                                          `receiver_district_id` string COMMENT '收件人区县id',
                                                          `receiver_name` string COMMENT '收件人姓名',
                                                          `sender_complex_id` bigint COMMENT '发件人小区id',
                                                          `sender_province_id` string COMMENT '发件人省份id',
                                                          `sender_city_id` string COMMENT '发件人城市id',
                                                          `sender_district_id` string COMMENT '发件人区县id',
                                                          `sender_name` string COMMENT '发件人姓名',
                                                          `payment_type` string COMMENT '支付方式',
                                                          `payment_type_name` string COMMENT '支付方式名称',
                                                          `cargo_num` bigint COMMENT '货物个数',
                                                          `amount` decimal(16,2) COMMENT '金额',
                                                          `estimate_arrive_time` string COMMENT '预计到达时间',
                                                          `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '物流域派送成功事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_deliver_suc_detail_inc'
    tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_deliver_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(deliver_suc_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) deliver_suc_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'


        and status <> '60030'
        and status <> '60020'
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 7.2 每日装载
with deliver_suc_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 deliver_suc_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') deliver_suc_time
                from ods_order_info
                where ds = '20230105'
                  and status = '60050'
                  and status ='60040'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '2025-07-12'
           and (status = '60050' or
                status = '60040' or
                status = '60030' or
                status = '60020'
             )
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105') cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105'
              ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trans_deliver_suc_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_info.deliver_suc_time,
       order_info.order_no,
       deliver_suc_info.status,
       deliver_suc_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       deliver_suc_info.payment_type,
       deliver_suc_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance
from deliver_suc_info
         join order_info
              on deliver_suc_info.id = order_info.order_id;

select * from dwd_trans_deliver_suc_detail_inc;


create database if not exists tms01;
use tms01;
-- 5. 物流域发单事务事实表
drop table if exists dwd_trans_dispatch_detail_inc;
create external table dwd_trans_dispatch_detail_inc(
                                                       `id` bigint comment '运单明细ID',
                                                       `order_id` string COMMENT '运单ID',
                                                       `cargo_type` string COMMENT '货物类型ID',
                                                       `cargo_type_name` string COMMENT '货物类型名称',
                                                       `volumn_length` bigint COMMENT '长cm',
                                                       `volumn_width` bigint COMMENT '宽cm',
                                                       `volumn_height` bigint COMMENT '高cm',
                                                       `weight` decimal(16,2) COMMENT '重量 kg',
                                                       `dispatch_time` string COMMENT '发单时间',
                                                       `order_no` string COMMENT '运单号',
                                                       `status` string COMMENT '运单状态',
                                                       `status_name` string COMMENT '运单状态名称',
                                                       `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                       `collect_type_name` string COMMENT '取件类型名称',
                                                       `user_id` bigint COMMENT '用户ID',
                                                       `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                       `receiver_province_id` string COMMENT '收件人省份id',
                                                       `receiver_city_id` string COMMENT '收件人城市id',
                                                       `receiver_district_id` string COMMENT '收件人区县id',
                                                       `receiver_name` string COMMENT '收件人姓名',
                                                       `sender_complex_id` bigint COMMENT '发件人小区id',
                                                       `sender_province_id` string COMMENT '发件人省份id',
                                                       `sender_city_id` string COMMENT '发件人城市id',
                                                       `sender_district_id` string COMMENT '发件人区县id',
                                                       `sender_name` string COMMENT '发件人姓名',
                                                       `payment_type` string COMMENT '支付方式',
                                                       `payment_type_name` string COMMENT '支付方式名称',
                                                       `cargo_num` bigint COMMENT '货物个数',
                                                       `amount` decimal(16,2) COMMENT '金额',
                                                       `estimate_arrive_time` string COMMENT '预计到达时间',
                                                       `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '物流域发单事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_dispatch_detail_inc'
    tblproperties('orc.compress' = 'snappy');



-- 5.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_dispatch_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(dispatch_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) dispatch_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 5.2 每日装载
with dispatch_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 dispatch_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') dispatch_time

                from ods_order_info
                where ds = '20230105'
                  and status = '60040'
                  and status = '60050'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '2025-07-12'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105') cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105') info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trans_dispatch_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_info.dispatch_time,
       order_info.order_no,
       dispatch_info.status,
       dispatch_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       dispatch_info.payment_type,
       dispatch_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance
from dispatch_info
         join order_info
              on dispatch_info.id = order_info.order_id;

select * from dwd_trans_dispatch_detail_inc;

create database if not exists tms01;
use tms01;

-- 4. 物流域揽收事务事实表
drop table if exists dwd_trans_receive_detail_inc;
create external table dwd_trans_receive_detail_inc(
                                                      `id` bigint comment '运单明细ID',
                                                      `order_id` string COMMENT '运单ID',
                                                      `cargo_type` string COMMENT '货物类型ID',
                                                      `cargo_type_name` string COMMENT '货物类型名称',
                                                      `volume_length` bigint COMMENT '长cm',
                                                      `volume_width` bigint COMMENT '宽cm',
                                                      `volume_height` bigint COMMENT '高cm',
                                                      `weight` decimal(16,2) COMMENT '重量 kg',
                                                      `receive_time` string COMMENT '揽收时间',
                                                      `order_no` string COMMENT '运单号',
                                                      `status` string COMMENT '运单状态',
                                                      `status_name` string COMMENT '运单状态名称',
                                                      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                      `collect_type_name` string COMMENT '取件类型名称',
                                                      `user_id` bigint COMMENT '用户ID',
                                                      `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                      `receiver_province_id` string COMMENT '收件人省份id',
                                                      `receiver_city_id` string COMMENT '收件人城市id',
                                                      `receiver_district_id` string COMMENT '收件人区县id',
                                                      `receiver_name` string COMMENT '收件人姓名',
                                                      `sender_complex_id` bigint COMMENT '发件人小区id',
                                                      `sender_province_id` string COMMENT '发件人省份id',
                                                      `sender_city_id` string COMMENT '发件人城市id',
                                                      `sender_district_id` string COMMENT '发件人区县id',
                                                      `sender_name` string COMMENT '发件人姓名',
                                                      `payment_type` string COMMENT '支付方式',
                                                      `payment_type_name` string COMMENT '支付方式名称',
                                                      `cargo_num` bigint COMMENT '货物个数',
                                                      `amount` decimal(16,2) COMMENT '金额',
                                                      `estimate_arrive_time` string COMMENT '预计到达时间',
                                                      `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '物流域揽收事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_receive_detail_inc'
    tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_receive_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(receive_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) receive_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 4.2 每日装载
with receive_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 receive_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') receive_time
                from ods_order_info
                where ds = '20230105'
                  and status = '60020'
                  and status = '60030'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105') cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105'
              ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trans_receive_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_info.receive_time,
       order_info.order_no,
       receive_info.status,
       receive_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       receive_info.payment_type,
       receive_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance
from receive_info
         join order_info
              on receive_info.id = order_info.order_id;

select * from dwd_trans_receive_detail_inc;

create database if not exists tms01;
use tms01;

-- 8. 物流域签收事务事实表
drop table if exists dwd_trans_sign_detail_inc;
create external table dwd_trans_sign_detail_inc(
                                                   `id` bigint comment '运单明细ID',
                                                   `order_id` string COMMENT '运单ID',
                                                   `cargo_type` string COMMENT '货物类型ID',
                                                   `cargo_type_name` string COMMENT '货物类型名称',
                                                   `volume_length` bigint COMMENT '长cm',
                                                   `volume_width` bigint COMMENT '宽cm',
                                                   `volume_height` bigint COMMENT '高cm',
                                                   `weight` decimal(16,2) COMMENT '重量 kg',
                                                   `sign_time` string COMMENT '签收时间',
                                                   `order_no` string COMMENT '运单号',
                                                   `status` string COMMENT '运单状态',
                                                   `status_name` string COMMENT '运单状态名称',
                                                   `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                   `collect_type_name` string COMMENT '取件类型名称',
                                                   `user_id` bigint COMMENT '用户ID',
                                                   `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                   `receiver_province_id` string COMMENT '收件人省份id',
                                                   `receiver_city_id` string COMMENT '收件人城市id',
                                                   `receiver_district_id` string COMMENT '收件人区县id',
                                                   `receiver_name` string COMMENT '收件人姓名',
                                                   `sender_complex_id` bigint COMMENT '发件人小区id',
                                                   `sender_province_id` string COMMENT '发件人省份id',
                                                   `sender_city_id` string COMMENT '发件人城市id',
                                                   `sender_district_id` string COMMENT '发件人区县id',
                                                   `sender_name` string COMMENT '发件人姓名',
                                                   `payment_type` string COMMENT '支付方式',
                                                   `payment_type_name` string COMMENT '支付方式名称',
                                                   `cargo_num` bigint COMMENT '货物个数',
                                                   `amount` decimal(16,2) COMMENT '金额',
                                                   `estimate_arrive_time` string COMMENT '预计到达时间',
                                                   `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '物流域签收事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_sign_detail_inc'
    tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_sign_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       sign_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(sign_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where ds = '20230105'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) sign_time
      from ods_order_info
      where ds = '20230105'
        and is_deleted = '0'
        and status <> '60030'
        and status <> '60020'
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where ds = '20220708'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 8.2 每日装载
with sign_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 sign_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') sign_time
                from ods_order_info
                where ds = '20230105'
                  and status = '60050'
                  and status = '60040'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where ds = '20220708'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '2025-07-12'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time
               from ods_order_cargo
               where ds = '20230105'
              ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where ds = '20230105') info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where ds = '20220708'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table dwd_trans_sign_detail_inc
partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       sign_info.sign_time,
       order_info.order_no,
       sign_info.status,
       sign_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       sign_info.payment_type,
       sign_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance
from sign_info
         join order_info
              on sign_info.id = order_info.order_id;

select * from dwd_trans_sign_detail_inc;

create database if not exists tms01;
use tms01;

-- 10. 物流域运输事务事实表
drop table if exists dwd_trans_trans_finish_inc;
create external table dwd_trans_trans_finish_inc(
                                                    `id` bigint comment '运输任务ID',
                                                    `shift_id` bigint COMMENT '车次ID',
                                                    `line_id` bigint COMMENT '路线ID',
                                                    `start_org_id` bigint COMMENT '起始机构ID',
                                                    `start_org_name` string COMMENT '起始机构名称',
                                                    `end_org_id` bigint COMMENT '目的机构ID',
                                                    `end_org_name` string COMMENT '目的机构名称',
                                                    `order_num` bigint COMMENT '运单个数',
                                                    `driver1_emp_id` bigint COMMENT '司机1ID',
                                                    `driver1_name` string COMMENT '司机1名称',
                                                    `driver2_emp_id` bigint COMMENT '司机2ID',
                                                    `driver2_name` string COMMENT '司机2名称',
                                                    `truck_id` bigint COMMENT '卡车ID',
                                                    `truck_no` string COMMENT '卡车号牌',
                                                    `actual_start_time` string COMMENT '实际启动时间',
                                                    `actual_end_time` string COMMENT '实际到达时间',
                                                    `estimate_end_time` string COMMENT '预估到达时间',
                                                    `actual_distance` decimal(16,2) COMMENT '实际行驶距离',
                                                    `finish_dur_sec` bigint COMMENT '运输完成历经时长：秒'
) comment '物流域运输事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_trans_finish_inc'
    tblproperties('orc.compress' = 'snappy');

-- 10.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt)
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time estimate_end_time,
       actual_distance,
       finish_dur_sec,
       '2025-07-12' as dt
from (select id,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
             truck_id,
             md5(truck_no)                                                                      truck_no,
             actual_start_time,
             actual_end_time,
             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd')                                                                dt
      from ods_transport_task
      where ds = '20230105'
        and is_deleted = '0'
      ) info
         left join
     (select id,
             estimated_time
      from dim_shift_full
      where dt = '2025-07-12') dim_tb
     on info.shift_id = dim_tb.id;

-- 10.2 每日装载
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt = '2025-07-13')
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time estimate_end_time,
       actual_distance,
       finish_dur_sec
from (select id,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
             truck_id,
             md5(truck_no)                                                                      truck_no,
             actual_start_time,
             actual_end_time,
             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec
      from ods_transport_task
      where ds = '20230105'
        and is_deleted = '0') info
         left join
     (select id,
             estimated_time      from dim_shift_full
      where dt = '2025-07-12') dim_tb
     on info.shift_id = dim_tb.id;

select * from dwd_trans_trans_finish_inc;


