# encoding=utf-8
"""周收益率相关查询"""

from sql import sql_oracle
from time_tool import Time_tool
import logging
import math

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

from WindPy import w
w.start()



class Week_return:
    """周收益率相关类
    对外接口：
    zhoubodong：波动率
        :param code: 代码:str
        :param start_date:起始日期 :str
        :param end_date: 结束日期:str
        :return: float

    max_down_fund：最大回测
        :param code: 代码:str
        :param start_date:起始日期 :str
        :param end_date: 结束日期:str
        :return: float


    performance_stability_fof：业绩稳定
        :param code: fof 代码:str
        :param start_date:起始日期 :str
        :param end_date: 结束日期:str
        :return: float


    compute_alpha2:计算alpha
        :param code: 代码:str
        :param start_date:起始日期 :str
        :param end_date: 结束日期:str
        :return: float

    abs_return:获取绝对收益率
        :param code: 代码:str
        :param start_date:起始日期 :str
        :param end_date: 结束日期:str
        :return: float

    performance_stability:业绩稳定，code
        :param code: 代码:str
        :param start_date:起始日期 :str
        :param end_date: 结束日期:str
        :return: float


    """

    def __init__(self):
        self.cu = sql_oracle.cu
        self.cu_pra = sql_oracle.cu_pra_sel
        self.t = Time_tool()
        self.init_table()
        self.init_funcs()


        self.year_x = 52

    def init_table(self):
        """初始化各种数据库表名"""
        self.fof_member_info = 't_fof_member_info'

    def init_funcs(self):
        """初始化各种外挂方法"""
        classes = [Compute_alpha]
        for class_ in classes:
            class_(self).add_func()

    def set_year_x(self, x):
        """设置计算年化时的系数"""
        self.year_x = x

    def _get_winning(self, df):
        """
        计算胜率
        :param df:超额收益
        :return: 胜率
        """
        # 超额收益大于0的个数/总个数

        res = len(df.loc[df['超额收益'] > 0]) / len(df)
        return res

    def get_week_return_year(self, df):
        """根据传入的周收益表计算年化收益"""
        # 周收益平均*系数，默认是52
        return_mena = df['超额收益'].mean()
        res = return_mena * self.year_x
        return res

    def get_week_return(self, code: str, start_date: str, end_date: str, types: str = 'fund', jz: str = 'biwi'):
        """周超额收益查询"""

        # 首先计算出时间序列
        time_df = self.t.get_week_trade_days(start_date, end_date)
        pre_day = self.t.get_pre_week_trade_day(start_date)
        time_list = list(time_df['日期'])
        # 对于第一周的计算 需要用到上一周的数据，所有向列表头部插入了一个日期
        time_list.insert(0, pre_day)

        if types == 'fund':
            df = self.get_week_return_fund(code, time_list, jz)

        else:
            df = self.get_week_return_fund_fof(code, time_list, jz)
        return df

    def get_week_return_fund(self, code: str, time_list: list, jz: str = 'biwi'):
        """
        fund 周收益查询
        :param code: 基金代码
        :param time_list: 时间序列
        :param js: 基准
        :return:
        """

        df01 = self.get_week_value_fund(code, time_list)

        if jz == 'biwi':

            df02 = self.get_week_close_biwi(code, time_list)


        else:
            df02 = self.get_week_close_biwi(code, time_list)

        # 超额收益计算 基金收益率-指数收益率
        df = pd.merge(df01, df02, on='日期', how='left')
        df['超额收益'] = df['周收益率_x'] - df['周收益率_y']

        # 去重
        df.dropna(inplace=True)
        # time_index = df01.index.values
        # return_value = df01['周收益率'].values - df02['周收益率'].values
        # df = pd.DataFrame(columns=['时间', '超额收益'])
        # df['时间'] = time_index
        # df['超额收益'] = return_value
        df.set_index('日期', inplace=True)
        df = df[['超额收益']]

        return df

    def get_week_return_fund_fof(self, code: str, time_list: list, jz: str = 'zz800'):
        """
        fund 周收益查询 fof 版
        :param code: 基金代码
        :param time_list: 时间序列
        :param js: 基准
        :return:
        """

        df01 = self.get_week_value_fund_fof(code, time_list)

        if jz == 'zz800':
            # 中证800
            index_code = '000906'
            df02 = self.get_week_close_zz800(index_code, time_list)
        else:
            df02 = self.get_week_close_wind(jz, time_list)

        # 超额收益计算 基金收益率-指数收益率
        df = pd.merge(df01, df02, on='日期', how='left')

        df['超额收益'] = df['周收益率_x'] - df['周收益率_y']

        # 去空
        df.dropna(inplace=True)
        # time_index = df01.index.values
        # return_value = df01['周收益率'].values - df02['周收益率'].values
        # df = pd.DataFrame(columns=['时间', '超额收益'])
        # df['时间'] = time_index
        # df['超额收益'] = return_value
        df.set_index('日期', inplace=True)
        df = df[['超额收益']]

        return df

    def get_week_value_fund(self, code: str, time_list: list):
        """
        获取周净值,算周收益，fund
        :param code: 基金代码
        :param time_list: 每个交易周中的最后一个交易日，含有起始日期的上一个交易周的最后一个交易日
        :return: df index 日期，columns收益率
        """
        t1 = time_list[0]
        t2 = time_list[-1]

        sql_week_value_fund = f"""
        select f13_1101,f21_1101 from wind.tb_object_1101 left join wind.tb_object_1090  on f14_1101 = f2_1090
        where f16_1090 = '{code}' and (f13_1101 >= '{t1}' and f13_1101 <='{t2}')
        order by f13_1101
        """
        sql_res = self.cu.execute(sql_week_value_fund).fetchall()

        df = pd.DataFrame(sql_res, columns=['日期', '复权单位净值'])
        df.set_index('日期', inplace=True)
        # 筛选出需要的日期
        df = df.reindex(time_list)

        # df02 = df['复权单位净值'].pct_change()
        df['上周复权单位净值'] = df['复权单位净值'].shift()
        df['周收益率'] = (df['复权单位净值'] - df['上周复权单位净值']) / df['上周复权单位净值']
        # df = df[['周收益率']]
        # 去重，其索引，后期merge会用到日期字段
        df.dropna(inplace=True)
        df.reset_index(inplace=True)

        return df

    def get_week_value_fund_fof(self, code: str, time_list: list):
        """
        获取周净值,算周收益，fof
        :param code: 基金代码
        :param time_list: 每个交易周中的最后一个交易日，含有起始日期的上一个交易周的最后一个交易日
        :return: df index 日期，columns收益率
        """
        t1 = time_list[0]
        t2 = time_list[-1]

        # sql_week_value_fund = f"""
        # select f13_1101,f21_1101 from wind.tb_object_1101 left join wind.tb_object_1090  on f14_1101 = f2_1090
        # where f16_1090 = '{code}' and (f13_1101 >= '{t1}' and f13_1101 <='{t2}')
        # order by f13_1101
        # """

        sql_week_value_fund = f"""
         select tradedate,closeprice from t_fof_value_info where fundid = '{code}' 
         and tradedate >= '{t1}' and tradedate <= '{t2}'
         order by tradedate
        """
        print(sql_week_value_fund)
        sql_res = self.cu_pra.execute(sql_week_value_fund).fetchall()

        df = pd.DataFrame(sql_res, columns=['日期', '复权单位净值'])
        # print(df)
        df.set_index('日期', inplace=True)
        # 筛选出需要的日期
        df = df.reindex(time_list)

        # df02 = df['复权单位净值'].pct_change()
        df['上周复权单位净值'] = df['复权单位净值'].shift()
        df['周收益率'] = (df['复权单位净值'] - df['上周复权单位净值']) / df['上周复权单位净值']
        # df = df[['周收益率']]
        # 去重，其索引，后期merge会用到日期字段
        df.dropna(inplace=True)
        df.reset_index(inplace=True)
        # print(df)
        return df


    def get_week_close_biwi(self, code: str, time_list: list):
        """
        取基准，算周收益，biwi

        这里要做异常处理，基准不能有空！基准含有空直接报错！
        :param code: 基金代码
        :param time_list: 每个交易周中的最后一个交易日，含有起始日期的上一个交易周的最后一个交易日
        :return:
        """
        t1 = time_list[0]
        t2 = time_list[-1]

        sql_code = code + 'BI.WI'
        sql_week_close_bibw = f"""
        select  trade_dt,s_dq_close from wind.chinamutualfundbenchmarkeod 
        where s_info_windcode = '{sql_code}' and (trade_dt >= '{t1}' and trade_dt <='{t2}')
        order by trade_dt 
        """

        sql_res = self.cu.execute(sql_week_close_bibw).fetchall()
        assert sql_res, f'{code}基准查询结果为空,请改变基准'
        df = pd.DataFrame(sql_res, columns=['日期', '收盘价'])

        assert df.iloc[0][0] == t1, f'{code}基准查询结果含有空值,请改变基准'

        df.set_index('日期', inplace=True)
        # 筛选出需要的日期
        df = df.reindex(time_list)
        # 计算收益率  close2-close1/close1
        df['周收益率'] = df['收盘价'].pct_change()
        # 去空值
        df.dropna(inplace=True)
        # df = df[['周收益率']]
        # 去索引，后面merge 会用到日期字段
        df.reset_index(inplace=True)
        return df


    def get_week_close_zz800(self,index_code:str,time_list:list):
        """
        取基准，算周收益，biwi

        这里要做异常处理，基准不能有空！基准含有空直接报错！
        :param code: 基金代码
        :param time_list: 每个交易周中的最后一个交易日，含有起始日期的上一个交易周的最后一个交易日
        :return:
        """
        t1 = time_list[0]
        t2 = time_list[-1]
        sql_week_close_zz800 = f"""
        select f2_1425,f7_1425 from wind.tb_object_1425  left join wind.tb_object_1090  on f1_1425 = f2_1090
        where f16_1090 = '{index_code}'
        and (f2_1425 >='{t1}' and f2_1425 <= '{t2}')
        and f4_1090 = 'S'
        order by f2_1425
        """
        # print(sql_week_close_zz800)
        sql_res = self.cu.execute(sql_week_close_zz800).fetchall()
        assert sql_res, f'{code}基准查询结果为空,请改变基准'
        df = pd.DataFrame(sql_res, columns=['日期', '收盘价'])

        assert df.iloc[0][0] == t1, f'{code}基准查询结果含有空值,请改变基准'

        df.set_index('日期', inplace=True)
        # 筛选出需要的日期
        df = df.reindex(time_list)
        # 计算收益率  close2-close1/close1
        df['周收益率'] = df['收盘价'].pct_change()
        # 去空值

        df.dropna(inplace=True)
        # df = df[['周收益率']]
        # 去索引，后面merge 会用到日期字段
        df.reset_index(inplace=True)
        return df

    def get_week_close_wind(self,index_code:str,time_list:list):
        """
        从万德抓取数据
        :param index_code:
        :param time_list:
        :return:
        """

        t1 = time_list[0]
        t2 = time_list[-1]
        b = w.wsd(index_code, "close", t1, t2, "PriceAdj=B")
        # print(b.Times)
        data_dict = {
            '日期': list(map(lambda x: x.strftime('%Y%m%d'), b.Times)),
            '收盘价': b.Data[0]
        }

        df = pd.DataFrame(data_dict)
        assert df.empty is False, f'{code}基准查询结果为空,请改变基准'


        assert df.iloc[0][0] == t1, f'{code}基准查询结果含有空值,请改变基准'

        df.set_index('日期', inplace=True)
        # 筛选出需要的日期
        df = df.reindex(time_list)
        # 计算收益率  close2-close1/close1
        df['周收益率'] = df['收盘价'].pct_change()
        # 去空值

        df.dropna(inplace=True)
        # df = df[['周收益率']]
        # 去索引，后面merge 会用到日期字段
        df.reset_index(inplace=True)
        return df



    def unpack_fof(self, fof,start_date,end_date):

        """解包fof"""
        sql_unpakc_fof = f"""
        select memberfundid,weight from {self.fof_member_info} 
        where fundid = '{fof}'

        """
        sql_res = self.cu_pra.execute(sql_unpakc_fof).fetchall()
        df = pd.DataFrame(sql_res, columns=['X', 'P'])


        return df

    def get_fund_price(self, code, start_date, end_date):
        """计算波动和回测时用到的sql查询方法"""
        sql = '''
        select
        f13_1101 as 截止日期, f21_1101 as 复权单位净值 
        from
        wind.tb_object_1101
        left join wind.tb_object_1090
        on f2_1090 = f14_1101
        where 
        F16_1090= '%(code)s'
        and
        F13_1101 >= '%(start_date)s'
        and
        f13_1101 <= '%(end_date)s'
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
        fund_price = pd.DataFrame(self.cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
        return fund_price

    # ************ 下面是对外的接口 *************************

    def performance_stability(self, code: str, start_date: str, end_date: str, types: str = 'fund', jz: str = 'biwi'):
        """
        计算顺率
        :param code:代码
        :param start_date:开始日期
        :param end_date: 结束日期
        :param types: 代码类型，默认 fund
        :param jz: 指数类型，默认 biwi
        :return:
        """
        if types == 'fund':
            df_return = self.get_week_return(code, start_date, end_date, types, jz)
            res = self._get_winning(df_return)
        elif types == 'fof':
            # 这里的js是自己重新定义的，理想状态是在外面的js定义好
            jz = 'zz800'
            df_return = self.get_week_return(code, start_date, end_date, types, jz)

            res = self._get_winning(df_return)
        else:
            res = 0.0

        return res

    def zhoubodong(self, code='163807', start_date='20190101', end_date='20190225'):
        fund_price = self.get_fund_price(code, start_date, end_date)
        fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

        fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
        fund_price2.dropna(axis=0, inplace=True)
        fund_price2.reset_index(drop=True, inplace=True)

        zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))
        return zhou_bodong

    # 计算最大回测
    def max_down_fund(self, code='163807', start_date='20150528', end_date='20190225'):
        # 输出单只基金的最大回撤，返回一个float数值
        # 提取复权净值
        fund_price = self.get_fund_price(code, start_date, end_date)
        fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)
        price_list = fund_price2['复权单位净值'].tolist()
        i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
        if i == 0:
            max_down_value = 0
        else:
            j = np.argmax(price_list[:i])  # 开始位置
            max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
        return -max_down_value

    def performance_stability_fof(self, fof: str, start_date: str, end_date: str):
        """
        业绩稳定性
        :param code: 基金代码
        :param start_date: 起始时间
        :param end_date: 结束时间
        :param types: 代码类型，默认 fund基金
        :param jz: 基准指标，默认 biwi
        :return:
        """
        # 先计算时间列表
        time_df = self.t.get_week_trade_days(start_date, end_date)
        pre_day = self.t.get_pre_week_trade_day(start_date)
        time_list = list(time_df['日期'])
        time_list.insert(0, pre_day)
        # 再解包产品集D，得到x1,x2,x3和 p1,p2,p3
        df_D = self.unpack_fof(fof,start_date,end_date)
        x_list = df_D['X'].values
        p_list = df_D['P'].values
        # print('x_list:',x_list)
        # print('p_list:',p_list)
        # 计算每个x的胜率
        win_list = []
        for x in x_list:
            df_week_return = self.get_week_return(x, start_date, end_date)
            winning = self._get_winning(df_week_return)
            win_list.append(winning)
        # 对上面的结果做加权平均
        print('win_lilst:',win_list)
        res = np.average(win_list, weights=p_list)
        return res

    def abs_return(self, fof: str, start_date: str, end_date: str):
        """获取绝对收益率"""
        print('待开发')
        return 0.0
        pass



class Compute_alpha:
    """计算alpha"""

    def __init__(self, wr: Week_return):
        self.wr = wr
        self.t = self.wr.t

    def add_func(self):
        """添加方法到wr中去"""
        self.wr.compute_alpha2 = self.compute_alpha2

    def compute_alpha2(self,code, start_date, end_date):
        """
        计算每周收益率
        单基金的复权单位净值 计算周收益率
        与 行业基准的复权收盘价收益率 做线性回归
        求得 alpha2
        :param code:
        :param start_date:
        :param end_date:
        :return:
        """

        trade_dates = self.t.get_week_trade_days(start_date, end_date)
        wr = self.wr.get_week_return(code, start_date, end_date)
        time_df = self.t.get_week_trade_days(start_date, end_date)

        pre_day = self.t.get_pre_week_trade_day(start_date)
        time_list = list(time_df['日期'])
        time_list.insert(0, pre_day)
        df01 = self.wr.get_week_value_fund(code, time_list)
        df02 = self.wr.get_week_close_biwi(code, time_list)

        df01.set_index('日期')
        df01 = df01['周收益率']
        df01 = pd.DataFrame(df01)
        df01.columns = ['基金周收益率']

        df02.set_index('日期')
        df02 = df02['周收益率']
        df02 = pd.DataFrame(df02)
        df02.columns = ['指数周收益率']

        market = df01.join(df02)
        market.columns = ['基金收益率', '市场组合收益率']
        df = market
        df = df.dropna(axis=0, how='any')
        result = self.compute_alpha(df)
        logging.info('code: {}, from {} to {}'.format(code, start_date, end_date))
        logging.info('alpha2: {}'.format(result))
        return result

    def compute_alpha(self,df):
        X = np.array(df['市场组合收益率']).reshape(df['市场组合收益率'].shape[0], 1)
        regr = LinearRegression()
        regr.fit(X, df['基金收益率'])
        a = regr.intercept_ * 52
        return a





year_index_query = Week_return()

zhoubodong = year_index_query.zhoubodong
max_down_fund = year_index_query.max_down_fund
performance_stability_fof = year_index_query.performance_stability_fof
performance_stability = year_index_query.performance_stability
compute_alpha2 = year_index_query.compute_alpha2
abs_return = year_index_query.abs_return




if __name__ == '__main__':
    wr = Week_return()
    start_date = '20151220'
    end_date = '20181231'

    # code = '20I502434BFOF2'
    code = '048574593FFOF2'
    # code = '15FD60FOF1'
    # code = '15FD60FOF1'
    # code = '15FD60FOF1'

    # wr.get_week_return_fund()


    # print('code:',code)
    # ps = performance_stability_fof(code,start_date,end_date)
    # print(ps)
    # code = '15FD60FOF1'
    # print('code:',code)
    # ps = performance_stability(code,start_date,end_date)
    # print(ps)
    # alpha = wr.compute_alpha2(code,start_date,end_date)
    # print(alpha)

    win01 = wr.performance_stability(code,start_date,end_date,types='fof')
    print(win01)
    # a = wr.get_week_return('202801', start_date, end_date)
    # print(a)
    # res = wr.get_week_return_year(a)
    # print('年化收益率', res)
    # winning = wr.get_winning(a)
    # print('胜率：', winning)
    #
    # aaa = wr.performance_stability('202801',start_date,end_date)
    # print(aaa)
