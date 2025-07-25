

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms.{tableName}")



# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()
    # 构建SQL语句，修正字段别名以匹配Hive表结构

    select_sql = f"""
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
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)

# 4. 主函数（示例调用）
if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '2025-07-12'

    # 执行插入操作
    execute_hive_insert(target_date, 'dim_truck_full')

