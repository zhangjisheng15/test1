import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 连接数据库（示例：Hive或MySQL）
engine = create_engine('hive://192.168.179.140:10000/gmall')

# 加载用户行为数据（近90天）
analysis_date = dt.date(2025, 7, 30)
start_date = analysis_date - dt.timedelta(days=90)

user_action_sql = f"""
SELECT a.user_id, a.page_id, a.action_type, a.action_time, p.page_type, p.page_name
FROM ods_user_action a
LEFT JOIN dim_page p ON a.page_id = p.page_id
WHERE a.dt BETWEEN '{start_date}' AND '{analysis_date}'
"""
user_action = pd.read_sql(user_action_sql, engine)
user_action['action_time'] = pd.to_datetime(user_action['action_time'])  # 转换时间格式

# 加载用户分层数据
segment_sql = f"""
SELECT user_id, customer_segment
FROM dws_customer_segment
WHERE dt = '{analysis_date}'
"""
customer_segment = pd.read_sql(segment_sql, engine)


class UserPathAnalyzer:
    def __init__(self, user_action_df):
        self.df = user_action_df

    def get_first_page(self):
        """计算用户首次访问页面类型"""
        # 按用户分组，取最早访问记录
        self.df = self.df.sort_values(['user_id', 'action_time'])
        first_page = self.df.groupby('user_id').first().reset_index()
        return first_page[['user_id', 'page_type', 'page_name']].rename(
            columns={'page_type': 'first_page_type', 'page_name': 'first_page_name'}
        )

    def get_action_stats(self):
        """统计用户行为指标（访问次数、加购、支付）"""
        action_stats = self.df.groupby('user_id').agg(
            total_visit=('page_id', 'count'),
            cart_count=('action_type', lambda x: (x == '加购').sum()),
            pay_count=('action_type', lambda x: (x == '下单').sum())
        ).reset_index()
        return action_stats

    def get_main_page(self):
        """计算用户主要访问页面类型（访问次数最多）"""
        page_visit = self.df.groupby(['user_id', 'page_type']).size().reset_index(name='visit_count')
        # 按用户分组，取访问次数最多的页面类型
        page_visit['rank'] = page_visit.groupby('user_id')['visit_count'].rank(ascending=False, method='first')
        main_page = page_visit[page_visit['rank'] == 1][['user_id', 'page_type']].rename(
            columns={'page_type': 'main_page_type'})
        return main_page

    def merge_features(self):
        """合并所有路径特征"""
        first_page = self.get_first_page()
        action_stats = self.get_action_stats()
        main_page = self.get_main_page()

        # 合并特征
        path_features = first_page.merge(action_stats, on='user_id', how='left')
        path_features = path_features.merge(main_page, on='user_id', how='left')
        # 填充空值（如无主要页面则标记为“未知”）
        path_features['main_page_type'] = path_features['main_page_type'].fillna('未知')
        return path_features


# 生成用户路径特征
path_analyzer = UserPathAnalyzer(user_action)
user_path_features = path_analyzer.merge_features()
# 关联用户分层
user_path_with_segment = user_path_features.merge(customer_segment, on='user_id', how='left')


class PathAnalysis看板:
    def __init__(self, path_df):
        self.df = path_df

    def calculate_conversion(self):
        """计算转化率指标"""
        # 加购转化率 = 加购次数 / 总访问次数
        self.df['conversion_rate'] = (self.df['cart_count'] / self.df['total_visit']).fillna(0) * 100
        # 支付率 = 支付次数 / 加购次数（避免除零）
        self.df['pay_rate'] = np.where(
            self.df['cart_count'] > 0,
            (self.df['pay_count'] / self.df['cart_count']) * 100,
            0
        )
        return self.df

    def segment_analysis(self):
        """按用户分层聚合分析"""
        self.calculate_conversion()
        # 按分层和首次访问页面聚合
        segment_agg = self.df.groupby(['customer_segment', 'first_page_type', 'main_page_type']).agg(
            user_count=('user_id', 'nunique'),
            avg_conversion=('conversion_rate', 'mean'),
            avg_pay_rate=('pay_rate', 'mean')
        ).reset_index()

        # 计算各分层总用户数，用于计算占比
        total_users = self.df.groupby('customer_segment')['user_id'].nunique().reset_index(name='total_segment_users')
        segment_agg = segment_agg.merge(total_users, on='customer_segment')
        segment_agg['proportion'] = (segment_agg['user_count'] / segment_agg['total_segment_users']) * 100  # 占比

        return segment_agg.round(2)

    def visualize(self, analysis_result):
        """可视化看板核心图表"""
        # 1. 各分层用户首次访问页面分布
        plt.figure(figsize=(15, 8))
        sns.barplot(
            data=analysis_result,
            x='first_page_type',
            y='proportion',
            hue='customer_segment'
        )
        plt.title('不同分层用户的首次访问页面占比')
        plt.xlabel('首次访问页面类型')
        plt.ylabel('占比（%）')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('首次访问页面分布.png')
        plt.show()

        # 2. 主要页面类型转化率对比
        plt.figure(figsize=(15, 8))
        sns.barplot(
            data=analysis_result,
            x='main_page_type',
            y='avg_conversion',
            hue='customer_segment'
        )
        plt.title('不同分层用户在主要页面的加购转化率')
        plt.xlabel('主要访问页面类型')
        plt.ylabel('加购转化率（%）')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('主要页面转化率.png')
        plt.show()


# 生成看板数据并可视化
kanban = PathAnalysis看板(user_path_with_segment)
analysis_result = kanban.segment_analysis()
kanban.visualize(analysis_result)

# 保存结果到ADS层表
analysis_result.to_sql(
    'ads_path_analysis',
    engine,
    schema='gmall',
    if_exists='append',
    index=False
)