

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
    execute_hive_insert(target_date, 'dwd_bound_outbound_inc')


