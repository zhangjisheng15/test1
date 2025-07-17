from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
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


    # select_sql = """
    # select data.id,
    #    concat(substr(data.name, 1, 1), '*')                name,
    #    if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
    #       concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
    #    if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
    #       concat('*@', split(data.email, '@')[1]), null)   email,
    #    data.user_level,
    #    data.birthday,
    #    data.gender,
    #    data.create_time,
    #    data.operate_time,
    #    '20220608'                                        start_date,
    #    '99991231'                                        end_date
    # from ods_user_info data
    # where ds = '20220608';
    # """

    select_sql = f"""
    select id,
       name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       start_date,
       if(rn = 2, date_sub('20250625', 1), end_date)  as   end_date,
       if(rn = 1, '99991231', date_sub('20250625', 1)) as dt
from (
         select id,
                name,
                phone_num,
                email,
                user_level,
                birthday,
                gender,
                create_time,
                operate_time,
                start_date,
                end_date,
                row_number() over (partition by id order by start_date desc) rn
         from (
                  select id,
                         name,
                         phone_num,
                         email,
                         user_level,
                         birthday,
                         gender,
                         create_time,
                         operate_time,
                         start_date,
                         end_date
                  from dim_user_zip
                  where ds = '99991231'
                  union
                  select id,
                         concat(substr(name, 1, 1), '*')                name,
                         if(phone_num regexp
                            '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                            concat(substr(phone_num, 1, 3), '*'), null) phone_num,
                         if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
                            concat('*@', split(email, '@')[1]), null)   email,
                         user_level,
                         birthday,
                         gender,
                         create_time,
                         operate_time,
                         '20250625'                                   start_date,
                         '99991231'                                   end_date
                  from (
                           select data.id,
                                  data.name,
                                  data.phone_num,
                                  data.email,
                                  data.user_level,
                                  data.birthday,
                                  data.gender,
                                  data.create_time,
                                  data.operate_time,
                                  row_number() over (partition by data.id order by ds desc) rn
                           from ods_user_info data
                           where ds = '20250625'
                       ) t1
                  where rn = 1
              ) t2
     ) t3;
    """

    print(f"开始执行SQL插入,分区日期:{partition_date}")
    df1 = spark.sql(select_sql)

    df_with_partition = df1.withColumn("ds",lit(partition_date))

    print(f"SQL执行完成,分区{partition_date}操作成功")
    df_with_partition.show()

    select_to_hive(df_with_partition,tableName,partition_date)

    spark.sql("SELECT * FROM gmall.dim_user_zip LIMIT 5").show()


if __name__ == "__main__":
    target_date = '2025-06-25'

    execute_hive_insert(target_date,"dim_user_zip")

