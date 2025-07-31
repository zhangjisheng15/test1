import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体为 SimHei（支持中文）
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题
sns.set_style("whitegrid")


class RFMCustomerValueModel:
    """RFM客户价值模型类"""

    def __init__(self, data_path=None, analysis_date=None):
        """
        初始化模型
        :param data_path: 交易数据路径
        :param analysis_date: 分析日期，默认为数据中最大日期+1天
        """
        self.data = None
        self.rfm_data = None
        self.segmented_data = None
        self.data_path = data_path
        self.analysis_date = analysis_date

    def load_data(self, data=None):
        """
        加载交易数据
        :param data: 可以直接传入DataFrame，若为None则从data_path加载
        """
        if data is not None:
            self.data = data
        elif self.data_path:
            # 假设数据格式包含：user_id, order_id, order_time, amount
            self.data = pd.read_csv(self.data_path)

            # 转换日期格式
            self.data['order_time'] = pd.to_datetime(self.data['order_time'])

            # 检查必要的列是否存在
            required_columns = ['user_id', 'order_id', 'order_time', 'amount']
            missing_columns = [col for col in required_columns if col not in self.data.columns]
            if missing_columns:
                raise ValueError(f"数据缺少必要的列: {missing_columns}")

        print(f"成功加载数据，共 {len(self.data)} 条交易记录，{self.data['user_id'].nunique()} 个用户")
        return self

    def calculate_rfm(self):
        """计算RFM指标"""
        if self.data is None:
            raise ValueError("请先加载数据")

        # 确定分析日期
        if self.analysis_date is None:
            self.analysis_date = self.data['order_time'].max() + dt.timedelta(days=1)

        # 计算RFM指标
        self.rfm_data = self.data.groupby('user_id').agg({
            'order_time': lambda x: (self.analysis_date - x.max()).days,  # R: 最近消费距今天数
            'order_id': 'nunique',  # F: 消费频率
            'amount': 'sum'  # M: 消费总金额
        }).rename(columns={
            'order_time': 'R',
            'order_id': 'F',
            'amount': 'M'
        })

        print("RFM指标计算完成")
        return self

    def analyze_rfm_distribution(self):
        """分析RFM指标分布"""
        if self.rfm_data is None:
            raise ValueError("请先计算RFM指标")

        # 绘制RFM分布直方图
        plt.figure(figsize=(15, 10))

        plt.subplot(3, 1, 1)
        sns.histplot(self.rfm_data['R'], kde=True)
        plt.title('R(最近消费距今天数)分布')

        plt.subplot(3, 1, 2)
        sns.histplot(self.rfm_data['F'], kde=True)
        plt.title('F(消费频率)分布')

        plt.subplot(3, 1, 3)
        sns.histplot(self.rfm_data['M'], kde=True)
        plt.title('M(消费总金额)分布')

        plt.tight_layout()
        plt.savefig('rfm_distribution.png')
        plt.show()

        # 输出RFM统计描述
        print("RFM指标统计描述:")
        print(self.rfm_data.describe())

        return self

    def segment_customers(self, r_bins=None, f_bins=None, m_bins=None):
        """
        客户分层
        :param r_bins: R指标的分箱阈值，默认使用四分位数
        :param f_bins: F指标的分箱阈值，默认使用四分位数
        :param m_bins: M指标的分箱阈值，默认使用四分位数
        """
        if self.rfm_data is None:
            raise ValueError("请先计算RFM指标")

        # 复制RFM数据用于添加评分
        self.segmented_data = self.rfm_data.copy()

        # 确定分箱阈值 - 对于R，值越小越好；对于F和M，值越大越好
        if r_bins is None:
            # 使用5分位数，生成6个边界值(形成5个区间)
            r_bins = list(self.rfm_data['R'].quantile([0.2, 0.4, 0.6, 0.8]))
            r_bins = [float('-inf')] + r_bins + [float('inf')]  # 最终得到6个边界值

        if f_bins is None:
            f_bins = list(self.rfm_data['F'].quantile([0.2, 0.4, 0.6, 0.8]))
            f_bins = [float('-inf')] + f_bins + [float('inf')]

        if m_bins is None:
            m_bins = list(self.rfm_data['M'].quantile([0.2, 0.4, 0.6, 0.8]))
            m_bins = [float('-inf')] + m_bins + [float('inf')]

        # 计算RFM评分 (1-5分，R值越小评分越高，F和M值越大评分越高)
        # 6个边界值形成5个区间，对应5个标签
        self.segmented_data['R_score'] = pd.cut(
            self.segmented_data['R'],
            bins=r_bins,
            labels=[5, 4, 3, 2, 1]  # 5个标签，与5个区间对应
        ).astype(int)

        self.segmented_data['F_score'] = pd.cut(
            self.segmented_data['F'],
            bins=f_bins,
            labels=[1, 2, 3, 4, 5]  # 5个标签，与5个区间对应
        ).astype(int)

        self.segmented_data['M_score'] = pd.cut(
            self.segmented_data['M'],
            bins=m_bins,
            labels=[1, 2, 3, 4, 5]  # 5个标签，与5个区间对应
        ).astype(int)

        # 计算RFM总分
        self.segmented_data['RFM_total'] = self.segmented_data['R_score'] + self.segmented_data['F_score'] + \
                                           self.segmented_data['M_score']

        # 客户分层
        def segment_customer(row):
            r, f, m = row['R_score'], row['F_score'], row['M_score']

            # 高价值客户：RFM评分均较高
            if r >= 4 and f >= 4 and m >= 4:
                return '高价值客户'
            # 潜力客户：最近消费较近，消费频率中等，消费金额较高
            elif r >= 3 and f >= 2 and f <= 3 and m >= 3:
                return '潜力客户'
            # 一般客户：各项指标均中等
            elif r >= 2 and f >= 2 and m >= 2:
                return '一般客户'
            # 低价值客户：其他情况
            else:
                return '低价值客户'

        self.segmented_data['customer_segment'] = self.segmented_data.apply(segment_customer, axis=1)

        print("客户分层完成")
        return self

    def analyze_segments(self):
        """分析客户分层结果"""
        if self.segmented_data is None:
            raise ValueError("请先进行客户分层")

        # 各分层客户数量统计
        segment_counts = self.segmented_data['customer_segment'].value_counts()
        print("\n各分层客户数量:")
        print(segment_counts)

        # 各分层客户占比
        segment_percent = self.segmented_data['customer_segment'].value_counts(normalize=True) * 100
        print("\n各分层客户占比(%):")
        print(segment_percent.round(2))

        # 绘制客户分层饼图
        plt.figure(figsize=(10, 6))
        plt.pie(segment_counts, labels=segment_counts.index, autopct='%1.1f%%', startangle=90)
        plt.title('客户分层占比')
        plt.axis('equal')
        plt.savefig('customer_segment_pie.png')
        plt.show()

        # 各分层RFM指标均值对比
        segment_rfm_mean = self.segmented_data.groupby('customer_segment')[['R', 'F', 'M']].mean()
        print("\n各分层RFM指标均值:")
        print(segment_rfm_mean.round(2))

        # 绘制各分层RFM指标对比图
        plt.figure(figsize=(15, 5))

        plt.subplot(1, 3, 1)
        sns.barplot(x=segment_rfm_mean.index, y=segment_rfm_mean['R'])
        plt.title('各分层R指标均值(越小越好)')
        plt.xticks(rotation=45)

        plt.subplot(1, 3, 2)
        sns.barplot(x=segment_rfm_mean.index, y=segment_rfm_mean['F'])
        plt.title('各分层F指标均值(越大越好)')
        plt.xticks(rotation=45)

        plt.subplot(1, 3, 3)
        sns.barplot(x=segment_rfm_mean.index, y=segment_rfm_mean['M'])
        plt.title('各分层M指标均值(越大越好)')
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig('segment_rfm_comparison.png')
        plt.show()

        return self

    def save_results(self, output_path='rfm_results.csv'):
        """保存分析结果"""
        if self.segmented_data is None:
            raise ValueError("请先完成客户分层")

        self.segmented_data.to_csv(output_path)
        print(f"分析结果已保存至 {output_path}")
        return self


def generate_sample_data(num_users=1000, num_orders=5000, start_date='2024-01-01', end_date='2025-07-31'):
    """生成样本交易数据用于测试"""
    print("生成样本交易数据...")

    # 生成用户ID
    user_ids = np.random.randint(10000, 99999, size=num_users)

    # 生成订单数据
    data = []
    for _ in range(num_orders):
        user_id = np.random.choice(user_ids)
        order_id = f"ORD{np.random.randint(100000, 999999)}"

        # 生成随机订单日期
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        date = start + (end - start) * np.random.random()

        # 生成随机订单金额
        amount = round(np.random.lognormal(3, 0.8), 2)  # 偏态分布的金额数据

        data.append([user_id, order_id, date, amount])

    # 创建DataFrame
    df = pd.DataFrame(data, columns=['user_id', 'order_id', 'order_time', 'amount'])

    # 为了让数据更真实，让某些用户消费更频繁
    frequent_users = np.random.choice(user_ids, size=int(num_users * 0.2), replace=False)
    frequent_orders = []

    for _ in range(int(num_orders * 0.3)):
        user_id = np.random.choice(frequent_users)
        order_id = f"ORD{np.random.randint(100000, 999999)}"
        date = start + (end - start) * np.random.random()
        amount = round(np.random.lognormal(3.5, 0.7), 2)  # 高频用户消费金额略高
        frequent_orders.append([user_id, order_id, date, amount])

    df = pd.concat([df, pd.DataFrame(frequent_orders, columns=df.columns)], ignore_index=True)

    return df


if __name__ == "__main__":
    # 生成样本数据
    sample_data = generate_sample_data()

    # 创建并运行RFM模型
    rfm_model = RFMCustomerValueModel()
    (rfm_model
     .load_data(sample_data)
     .calculate_rfm()
     .analyze_rfm_distribution()
     .segment_customers()
     .analyze_segments()
     .save_results())

    print("\nRFM客户价值模型构建完成！")
