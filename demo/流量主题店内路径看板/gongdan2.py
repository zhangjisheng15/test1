import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sqlalchemy.dialects.mssql.information_schema import columns

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 连接数据库（示例：Hive或MySQL）
engine = create_engine('hive://192.168.179.140:10000/gmall')

# 加载用户行为数据（近90天）
analysis_date = dt.date(2025,7,30)
start_date = analysis_date - dt.timedelta(days=90)

user_action_sql = f"""
SELECT a.user_id, a.page_id, a.action_type, a.action_time, p.page_type, p.page_name
FROM ods_user_action a
LEFT JOIN dim_page p ON a.page_id = p.page_id
WHERE a.dt BETWEEN '{start_date}' AND '{analysis_date}'
"""
user_action = pd.read_sql(user_action_sql,engine)
user_action['action_time'] = pd.to_datetime(user_action['action_time'])

# 加载用户分层数据
segment_sql = f"""
SELECT user_id, customer_segment
FROM dws_customer_segment
WHERE dt = '{analysis_date}'
"""

customer_segment = pd.read_sql(segment_sql,engine)


class UserPathAnalyzer:
    def __init__(self,user_action_df):
        self.df = user_action_df

    def get_first_page(self):
        """计算用户首次访问页面类型"""
        # 按用户分组，取最早访问记录
        self.df = self.df.sort_values(['user_id','action_time'])
        first_page = self.df.groupby('user_id').first().reset_index()
        return first_page[['user_id','page_type','page_name']].rename(
            columns={'page_type':'first_page_type','page_name':'first_page_name'}
        )







