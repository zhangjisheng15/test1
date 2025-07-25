

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
    execute_hive_insert(target_date, 'dim_activity_full')



