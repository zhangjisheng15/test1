use tms;


drop table dim_truck_full;
create external table dim_truck_full
(
    id                        STRING,
    team_id                   STRING,
    team_name                 STRING,
    team_no                   STRING,
    org_id                    STRING,
    org_name                  STRING,
    manager_emp_id            STRING,
    truck_no                  STRING,
    truck_model_id            STRING,
    truck_model_name          STRING,
    truck_model_type          STRING,
    truck_model_type_name     STRING,
    truck_model_no            STRING,
    truck_brand               STRING,
    truck_brand_name          STRING,
    truck_weight              STRING,
    load_weight               STRING,
    total_weight              STRING,
    eev                       STRING,
    boxcar_len                STRING,
    boxcar_wd                 STRING,
    boxcar_hg                 STRING,
    max_speed                 STRING,
    oil_vol                   STRING,
    device_gps_id             STRING,
    engine_no                 STRING,
    license_registration_date STRING,
    license_last_check_date   STRING,
    license_expire_date       STRING,
    is_enabled                STRING
) partitioned by (ds string)
    stored as orc
    location '/warehouse/tms/dim/dim_truck_full'
    tblproperties (
        'orc.compress' = 'SNAPPY',
        'external.table.purge' = 'true');
with tk as (
    select
        id,
        team_id,
        truck_no,
        truck_model_id,
        device_gps_id,
        engine_no,
        license_registration_date,
        license_last_check_date,
        license_expire_date,
        picture_url,
        is_enabled
    from ods_truck_info where ds = '20230105' and is_deleted= '0'
),tm as (
    select
        id,
        name,
        team_no,
        org_id,
        manager_emp_id
    from ods_truck_team where ds = '20230105' and is_deleted= '0'
),og as (
    select
        id,
        org_name
    from ods_base_organ where ds = '20250719' and is_deleted= '0'
),bc as (
    select
        id,
        name
    from ods_base_dic where ds = '20250719' and is_deleted= '0'
),td as (
    select
        id,
        model_name,
        model_type,
        model_no,
        brand,
        truck_weight,
        load_weight,
        total_weight,
        eev,
        boxcar_len,
        boxcar_wd,
        boxcar_hg,
        max_speed,
        oil_vol
    from ods_truck_model where ds = '20220618' and is_deleted= '0'
)
insert overwrite table dim_truck_full partition(ds='2025-07-12')
select
    tk.id,
    tk.team_id,
    tm.name,
    tm.team_no,
    tm.org_id,
    og.org_name,
    tm.manager_emp_id,
    tk.truck_no,
    td.id,
    td.model_name,
    td.model_type,
    mtp.name,
    td.model_no,
    td.brand,
    bd.name,
    td.truck_weight,
    td.load_weight,
    td.total_weight,
    td.eev,
    td.boxcar_len,
    td.boxcar_wd,
    td.boxcar_hg,
    td.max_speed,
    td.oil_vol,
    tk.device_gps_id,
    tk.engine_no,
    tk.license_registration_date,
    tk.license_last_check_date,
    tk.license_expire_date,
    tk.is_enabled
from tk left join tm on tk.team_id = tm.id
        left join og on tm.org_id = og.id
        left join td on tk.truck_model_id = td.id
        left join bc mtp on td.model_type = mtp.id
        left join bc bd
                  on td.brand = bd.id;
select * from dim_truck_full;