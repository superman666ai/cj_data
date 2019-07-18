import math
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

from .sql import sql_oracle
from .time_tool import Time_tool

# 季度标签
class SeasonLabel(object):
    def __init__(self):
        self.cu_wind = sql_oracle.cu
        self.cu_pra = sql_oracle.cu_pra_sel
        self.time_tool = Time_tool()
        self.init_funcs()

    def init_funcs(self):
        '''初始化各种外挂方法'''
        classes = [LeverageRatio, StandardDeviation, MaxDrawDown, IntervalProfit]
        for class_ in classes:
            class_(self).add_func()

    def gen_time_list(self, start_date, end_date, s_type='daily', pre_flag=True):
        '''生成交易日序列
        s_type: daily, 日频; weekly， 周频
        pre_flag: True， 需要计算收益率的标签在取样时需要往前多取一个交易日
        '''
        t_list = []
        if s_type == 'daily':
            t_df = self.time_tool.get_trade_days(start_date, end_date)
            t_list = list(t_df.iloc[:, 0])
            if pre_flag == True:
                pre_day = self.time_tool.get_pre_trade_day(start_date)
                t_list.insert(0, pre_day)
        return t_list

    def get_fund_price(self, code, start_date='', end_date='', time_list=[]):
        '''计算波动和回测时用到的sql查询方法'''
        if time_list:
            start_date = time_list[0]
            end_date = time_list[-1]
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
        order by f13_1101
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
        fund_price = pd.DataFrame(self.cu_wind.execute(sql).fetchall(), columns=['日期', '复权单位净值'])
        fund_price.set_index('日期', inplace=True)
        if fund_price.empty:
            raise Exception('lact of fund value')
        if time_list:
            # 基金净值数据是按照周频发布
            # 基金成立在时间段中间，导致获取净值数据不足
            if len(time_list) - fund_price.shape[0] > 50:
                raise Exception('lack of data, timelist>>fund_price.shape[0]')
            fund_price = fund_price.reindex(time_list)
        return fund_price

    def get_fund_price_fof(self, code, start_date='', end_date='', time_list=[]):
        '''fof 基金计算波动和回测时用到的sql查询方法'''
        if time_list:
            start_date = time_list[0]
            end_date = time_list[-1]
        sql = '''
        select
        tradedate, closeprice
        from
        t_fof_value_info 
        where 
        fundid = '%(code)s'
        and
        tradedate >= '%(start_date)s'
        and
        tradedate <= '%(end_date)s'
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
        fund_price = pd.DataFrame(self.cu_pra.execute(sql).fetchall(), columns=['日期', '复权单位净值'])
        fund_price.set_index('日期', inplace=True)
        if fund_price.empty:
            raise Exception('lact of fund value')
        if time_list:
            if len(time_list) - fund_price.shape[0] > 30:
                raise Exception('lack of data')
            fund_price = fund_price.reindex(time_list)
        return fund_price

    def get_fund_price_index(self, code, start_date='', end_date='', time_list=[]):
        if time_list:
            start_date = time_list[0]
            end_date = time_list[-1]
        sql =f'''
        select f2_1425,f7_1425 from wind.tb_object_1425  left join wind.tb_object_1090  on f1_1425 = f2_1090
        where f16_1090 = '{code}'
        and (f2_1425 >='{start_date}' and f2_1425 <= '{end_date}')
        and f4_1090 = 'S'
        order by f2_1425
        '''
        fund_price = pd.DataFrame(self.cu_wind.execute(sql).fetchall(), columns=['日期', '复权单位净值'])
        fund_price.set_index('日期', inplace=True)
        if fund_price.empty:
            raise Exception('lact of fund value')
        if time_list:
            if len(time_list) - fund_price.shape[0] > 30:
                raise Exception('lack of data')
            fund_price = fund_price.reindex(time_list)
        return fund_price
# 杠杆率标签
class LeverageRatio(object):
    def __init__(self, season_lable: SeasonLabel):
        self.season_lable = season_lable
        self.cu_wind = season_lable.cu_wind
        self.cu_pra = season_lable.cu_pra

    def add_func(self):
        self.season_lable.leverage_ratio = self.leverage_ratio

    def leverage_ratio(self):
        self.get_data_from_sql()

    def get_data_from_sql(self):
        '''test for lvereage ratio'''
        dates_str = str(tuple(['20171231', '20181231']))
        sql_kind='''
        select F16_1090 as 基金代码,
        F14_1104 as 截止时间,
        F5_1104 as 股票占比,
        F11_1104 as 现金占比,
        F13_1104 as 其他资产占比,
        F32_1104 as 债券市值占比,
        F45_1104 as 权证占比,
        F52_1104 as 基金占比,
        F55_1104 as 货币市场工具占比
        from wind.TB_OBJECT_1090 left join wind.TB_OBJECT_1104
        on F15_1104 = F2_1090
        where
        F14_1104 in %(date)s

        '''%{'date':dates_str}
        print(sql_kind)
        sql_res = self.cu_wind.execute(sql_kind).fetchall()
        res = pd.DataFrame(sql_res)
        print('in test alpha 0 ')
        print(res)

# 波动率标签
class StandardDeviation(object):
    def __init__(self, season_lable: SeasonLabel):
        self.season_lable = season_lable
        self.get_fund_price_fof = season_lable.get_fund_price_fof
        self.get_fund_price = season_lable.get_fund_price

    def add_func(self):
        self.season_lable.standard_deviation = self.standard_deviation
        self.season_lable.compute_standard_deviation = self.compute_standard_deviation

    def standard_deviation(self, code, start_date, end_date, fof=False):
        '''计算波动率, 判断是否为fof基金走两个分支'''
        time_list = self.season_lable.gen_time_list(start_date, end_date, s_type='daily', pre_flag=False)
        if not fof:
            # fund_price = self.get_fund_price(code, start_date, end_date)
            fund_price = self.get_fund_price(code, time_list=time_list)
        else:
            fund_price = self.get_fund_price_fof(code, time_list=time_list)

        zhou_bodong = self.compute_standard_deviation(fund_price)
        return zhou_bodong

    def compute_standard_deviation(self, df):
        ''' 计算波动率，dataframe.columnes=['日期', '复权单位净值’], 返回numpy.float64'''
        df2 = df.sort_values(by=['日期']).reset_index(drop=True)
        df2['fund_return'] = df2.复权单位净值.diff() / df2.复权单位净值.shift(1)
        df2.dropna(axis=0, inplace=True)
        df2.reset_index(drop=True, inplace=True)
        bodong = df2.fund_return.std() * (math.sqrt(250))
        return bodong

# 最大回撤标签
class MaxDrawDown(object):
    def __init__(self, season_lable: SeasonLabel):
        self.season_lable = season_lable
        self.get_fund_price_fof = season_lable.get_fund_price_fof
        self.get_fund_price = season_lable.get_fund_price
        self.get_fund_price_index = season_lable.get_fund_price_index

    def add_func(self):
        self.season_lable.max_draw_down = self.max_draw_down
        self.season_lable.compute_max_draw_down = self.compute_max_draw_down

        self.season_lable.max_draw_down_index = self.max_draw_down_index

        return

    def max_draw_down(self, code, start_date, end_date, fof=False):
        time_list = self.season_lable.gen_time_list(start_date, end_date, s_type='daily', pre_flag=False)
        if not fof:
            # fund_price = self.get_fund_price(code, start_date, end_date)
            fund_price = self.get_fund_price(code, time_list=time_list)
        else:
            fund_price = self.get_fund_price_fof(code, time_list=time_list)
        mdd = self.compute_max_draw_down(fund_price)
        return mdd

    def max_draw_down_index(self, code, start_date, end_date):
        time_list = self.season_lable.gen_time_list(start_date, end_date, s_type='daily', pre_flag=False)

        fund_price = self.get_fund_price_index(code, time_list=time_list)
        mdd = self.compute_max_draw_down(fund_price)
        return mdd
    def compute_max_draw_down(self, df):
        df2 = df.sort_values(by=['日期']).reset_index(drop=True)
        df.columns = ['复权单位净值']
        price_list = df2['复权单位净值'].tolist()
        i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
        if i == 0:
            max_down_value = 0
        else:
            j = np.argmax(price_list[:i])  # 开始位置
            max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
        return -max_down_value

# 区间收益标签
class IntervalProfit(object):
    def __init__(self, season_lable: SeasonLabel):
        self.season_lable = season_lable
        self.get_fund_price = season_lable.get_fund_price
        self.get_fund_price_fof = season_lable.get_fund_price_fof

    def add_func(self):
        self.season_lable.interval_profit = self.interval_profit
        self.season_lable.interval_profit_index = self.interval_profit_index
        return

    def interval_profit(self, code, start_date, end_date, fof=False):
        time_list = self.season_lable.gen_time_list(start_date, end_date, s_type='daily', pre_flag=True)
        start_date = start_date if time_list[1] == start_date else time_list[0]
        end_date = time_list[-1]
        if not fof:
            s_price_dt = self.get_fund_price(code, start_date, start_date, [])
            e_price_dt = self.get_fund_price(code, end_date, end_date, [])
        else:
            s_price_dt = self.get_fund_price_fof(code, start_date, start_date, [])
            e_price_dt = self.get_fund_price_fof(code, end_date, end_date, [])
        s_price = s_price_dt.iat[0, 0]
        e_price = e_price_dt.iat[0, 0]
        pft, annual_pft = self.compute_interva_profit(s_price, e_price, len(time_list))
        return pft, annual_pft

    def interval_profit_index(self,code,start_date,end_date):
        """计算指标的绝对收益率"""
        # 找出[起始，终止] 前闭后闭 的所有交易日，如果pre_flag为True 会自动往前去一个交易日
        time_list = self.season_lable.gen_time_list(start_date, end_date, s_type='daily', pre_flag=True)
        # 重新定义起始时间
        start_date = start_date if time_list[1] == start_date else time_list[0]

        sql =f"""
        select f2_1425,f7_1425 from wind.tb_object_1425  left join wind.tb_object_1090  on f1_1425 = f2_1090
        where f16_1090 = '{code}'
        and (f2_1425 >='{start_date}' and f2_1425 <= '{end_date}')
        and f4_1090 = 'S'
        order by f2_1425

        """
        sql_res = self.season_lable.cu_wind.execute(sql).fetchall()

        s_close = sql_res[0][1]
        e_close = sql_res[-1][1]

        pft, annual_pft = self.compute_interva_profit(s_close, e_close, len(time_list))
        return pft,annual_pft
    def compute_interva_profit(self, s_price, e_price, day_cnt):
        '''计算收益率，返回年化和非年化两种结果'''
        itv_pft = e_price / s_price - 1
        annualized_itv_pft = itv_pft / day_cnt * math.sqrt(250)
        return itv_pft, annualized_itv_pft

# 超额收益alpha
class AlphaCategroy(object):
    '''提供3种接口进行计算，目前实现两种
    type=2 基金基准进行计算 基金基准不存在改取同类基准
    type=0 同类基准
    type=1 同类平均基准(未实现)
    '''
    def __init__(self, season_lable: SeasonLabel):
        self.season_lable = season_lable

    def add_func(self):
        self.season_lable.compute_alpha_categroy = self.compute_alpha_categroy
        return

    def compute_alpha_categroy(self, code, start_date, end_date, run_type=2, fof=False):
        ''' mainly process
        '''
        time_list = self.season_lable.gen_time_list(start_date, end_date, s_type='weekly', pre_flag=True)
        if run_type == 2:
            fund_value = self.season_lable.get_fund_price(code, time_list=time_list)
            # 取不到 基金标准 换用同类标准
            try:
                fund_index_value = self.season_lable.get_fund_price_biwi(code, time_list=time_list)
                fund_index_rate = fund_index_value.pct_change()
                assert False
            except Exception as err:
                print(err)
                ejfl_type = self.season_lable.get_ejfl_type(code, start_date, end_date)
                print('type: {}'.format(ejfl_type))
                fund_index_rate = self.season_lable.get_market(ejfl_type, time_list)
        elif run_type == 0:
            pass
        print(fund_value)
        print(fund_index_value)
        fund_rate = fund_value.pct_change()
        fund_rate.reset_index(drop=True, inplace=True)
        market = fund_rate.join(fund_index_rate)
        market.columns = ['基金收益率', '市场组合收益率']
        market = market.dropna(axis=0, how='any')
        rst = self.compute_alpha(market)
        print(rst)
        return rst

    def compute_alpha(self, df):
        '''
        计算超额收益，取算数平均，返回年化结果
        '''
        df['超额收益'] = df['基金收益率'] - df['市场组合收益率']
        temp = df['超额收益'].sum() / df['超额收益'].size
        ### todo 将week_return 里数据移植过来，添加fund_value与index_value对比
        return temp * 52



tmp_local_season_label = SeasonLabel()
# 获取杠杆率数据
leverage_ratio = tmp_local_season_label.leverage_ratio
# 获取波动率数据
standard_deviation = tmp_local_season_label.standard_deviation
# 计算波动率
max_draw_down = tmp_local_season_label.max_draw_down
max_draw_down_index = tmp_local_season_label.max_draw_down_index
# 计算区间收益
interval_profit = tmp_local_season_label.interval_profit
# 计算指标的区间收益
interval_profit_index = tmp_local_season_label.interval_profit_index

if __name__ == '__main__':
    print('hello world')
    tmp_obj = SeasonLabel()
    tmp_obj.leverage_ratio()
