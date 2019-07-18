# encoding=utf-8
"""
择时能力计算

核心公式
(股票占净值比 - 权益类指数占基准权重)* 权益类指数向后3个月区间收益率


"""

from functions import ToolClass
from function.season_label import interval_profit_index
import pandas as pd
import os

PATH = os.path.dirname(os.path.realpath(__file__)) + '/'


ZZ800 = '000906'
ZZQZ = 'h11001'

CLASS_WEIGHT = {
    '股票型': (0.8, 0.2),
    '激进配置型': (0.7, 0.3),
    '标准配置型': (0.6, 0.4),
    '保守配置型': (0.2, 0.8),
    '灵活配置型': (0.5, 0.5),
    '沪港深股票型': (0.8, 0.2),
    '沪港深配置型': (0.5, 0.5)
}


class ZeShi(ToolClass):
    """择时算法类"""

    def __init__(self):
        super(ZeShi, self).__init__()

    def get_fund_df(self, report_date: str):
        """
        输入报告期自动判断分类年报
        :param report_date:报告期
        :return:
        """
        class_reportdate = str(int(report_date[:4]) -1) + '1231'

        sql = f"""
        select cpdm,ejfl from t_fund_classify_his where rptdate = '{class_reportdate}'
        and ejfl in (
        '股票型',
        '激进配置型',
        '标准配置型',
        '保守配置型',
        '灵活配置型',
        '沪港深股票型',
        '沪港深配置型'
        )

        """

        sql_res = self._get_db_data(sql, db='pra')
        df = pd.DataFrame(sql_res, columns=['基金代码', '二级分类'])

        return df

    def get_fund_stcoknav(self, codes: list, report_date: str):
        """
        查询基金的股票占比情况
        :param codes: 查询基金 list
        :param report_date: 报告期
        :return:
        """
        df_res = None
        code_len = len(codes)
        # print(code_len)
        i = 0
        while i < code_len:
            start_index = i
            end_index = i + 500
            # print(start_index)
            # print(end_index)
            # print('*' * 50)
            if end_index >= code_len:
                code_in = codes[start_index:]
            else:
                code_in = codes[start_index:end_index]
            code_in_str = str(tuple(code_in)) if len(code_in) > 1 else f"'{code_in[0]}'"
            sql = f"""
            
            select f16_1090,f5_1104,f14_1104 from wind.tb_object_1104 left join wind.tb_object_1090 on f15_1104 = f2_1090 where
             f14_1104 = '{report_date}'
             and f16_1090 in {code_in_str}
    
            """
            sql_res = self._get_db_data(sql, db='teader')
            df = pd.DataFrame(sql_res, columns=['基金代码', '股票占比', '报告期'])
            df_res = df.append(df_res)
            i = end_index
        return df_res

    def add_weight(self,df:pd.DataFrame):
        """
        添加权重
        :param df:带二级分类的fund表
        :return:
        """
        df = df.copy()
        df['权重'] = df['二级分类'].apply(lambda x:CLASS_WEIGHT[x])
        return df

    def get_index_return(self,df:pd.DataFrame,reportdate):
        """
        添加指标向后三个月的收益
        :param df:
        :return:
        """

        def func01(t: str):
            """
            简单处理报告日期
            :param t:
            :return:
            """
            if t.endswith('0331'):
                start_t = t[:4] + '0401'
                end_t = t[:4] + '0630'
            elif t.endswith('0630'):
                start_t = t[:4] + '0701'
                end_t = t[:4] + '0930'
            elif t.endswith('0930'):
                start_t = t[:4] + '1001'
                end_t = t[:4] + '1231'
            else:
                start_t = str(int(t[:4]) + 1) + '0101'
                end_t = str(int(t[:4]) + 1) + '0331'
            return start_t, end_t

        start_t, end_t = func01(reportdate)

        zz800_un,zz800_year = interval_profit_index(ZZ800,start_t,end_t)
        zzqz_un,zzqz_year = interval_profit_index(ZZQZ,start_t,end_t)

        df= df.copy()
        df['权益基准向后三个月收益'] = zz800_un
        df['非权益基准向后三个月收益'] = zzqz_un
        return df


    def get_chose_ability(self,df:pd.DataFrame):
        """
        计算择时能力
        :param df:
        :return:
        """

        df = df.copy()

        def func01(x):
            """
            计算权益类
            :param x:
            :return:
            """
            stock_nav = x['股票占比']
            stock_index_weight = x['权重'][0]
            stock_return = x['权益基准向后三个月收益']

            res = (stock_nav-stock_index_weight) * stock_return

            return res

        def func02(x):
            """计算非权益类"""
            bond_nav = 1 - x['股票占比']
            bond_index_weight = x['权重'][1]
            bond_return = x['非权益基准向后三个月收益']

            res = (bond_nav - bond_index_weight) * bond_return

            return res

        df['权益类能力'] = df.apply(lambda x:func01(x),axis = 1)
        df['非权益类能力'] = df.apply(lambda x:func02(x),axis = 1)
        df['result'] = df['权益类能力'] + df['非权益类能力']
        return df


    def run(self, reportdate):
        """
        主函数
        :param reportdate:报告期
        :return:
        """
        # 获取要跑的fund
        df_fund = self.get_fund_df(reportdate)
        # print(df_fund)
        fund_list = df_fund['基金代码'].tolist()
        # 取得要跑的基金代码的股票占比
        df_fund_stock = self.get_fund_stcoknav(fund_list, reportdate)
        # print(df_fund_stock)
        df_fund = pd.merge(df_fund, df_fund_stock, how='left', on='基金代码')
        # 根据fund的二级分类添加权益类和非权益类的权重
        df_fund = self.add_weight(df_fund)
        # 获取指标的后三月收益
        df_fund = self.get_index_return(df_fund,reportdate)
        # 去空
        df_fund.dropna(subset=['股票占比'],inplace=True)

        df_fund = self.get_chose_ability(df_fund)
        return df_fund


if __name__ == '__main__':
    zs = ZeShi()
    start_date = '20050630'
    end_date = '20190331'
    time_list = pd.date_range(start_date,end_date,freq='3M').strftime('%Y%m%d').tolist()
    for t in time_list:
        print(t)
        df = zs.run(t)
        df.to_excel(f"{PATH}output/{t}.xlsx")

    # zs.run('20180331')
