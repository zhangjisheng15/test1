

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
    execute_hive_insert(target_date, 'dim_user_zip')



