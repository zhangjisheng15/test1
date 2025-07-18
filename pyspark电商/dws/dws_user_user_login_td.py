

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
    spark.sql("USE gmall")
    return spark


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")

# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    select_sql = f"""
    select u.id                                          as               user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) as login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                  as     login_count_td
    from (
         select id,
                create_time
         from dim_user_zip
         where ds = '99991231'
     ) u
         left join
     (
         select user_id,
                max(ds)  login_date_last,
                count(*) login_count_td
         from dwd_user_login_inc
         group by user_id
     ) l
     on u.id = l.user_id;
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

    spark.sql("select * from gmall.dws_user_user_login_td limit 10").show()


if __name__ == "__main__":
    target_date = '2025-07-01'

    execute_hive_insert(target_date,"dws_user_user_login_td")

