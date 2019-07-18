#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/8 13:55
# @Author  : GaoJian
# @File    : func_tools.py


import os
import cx_Oracle
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta


def single(cls):
    """单例"""
    _instance = {}

    def warpper(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return warpper


def to_abspath(path):
    """相对路径转绝对路径"""
    return os.path.abspath(path)


def list_to_sql_list(lists: list):
    """
    list转sql可用的list
    :param lists: list：[1,2,3,4,5]
    :return: '1','2''3','4','5'
    """

    lists = list(map(lambda x: f"''{x}''", lists))
    res = ','.join(lists)
    return res


def nan_cvt(x, dig: int = 4):
    """
    处理df数值，nan转为py中的None，小数默认保留小数点后4位
    :param x: 数值
    :param dig: 保留位数
    :return:
    """
    return None if str(x) == 'nan' else round(x, dig)


class ToolClass:
    """根据7因子打标签"""

    def __init__(self):
        """初始化"""
        # 记录数据库连接数
        self.count_db = 0
        self.connecting = False

    def __del__(self):
        """退出同时关闭数据库"""

        self.connecting = False
        self.close_db()

    def connect_db(self):
        """连接数据库"""
        if self.count_db == 0:
            [userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf',
                                                                 'wind']
            self.fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
            self.cu = self.fund_db.cursor()

            # pra库
            [userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
            self.fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
            self.cu_pra_sel = self.fund_db_pra.cursor()

            self.count_db += 1

    def close_db(self):
        """断开数据库"""
        if self.count_db == 1 and self.connecting == False:
            self.cu.close()
            self.cu_pra_sel.close()

            self.fund_db.close()
            self.fund_db_pra.close()

            self.count_db -= 1

    def _get_db_data(self, sql: str, db: str = 'reader'):
        """专用的短链接读取数据库数据的方法"""
        self.connect_db()
        self.connecting = True
        cu = self.cu if db == 'reader' else self.cu_pra_sel
        sql_res = cu.execute(sql).fetchall()
        self.connecting = False
        self.close_db()
        return sql_res

    def _commit_db(self, sql: str, rec: list, db: str = 'reader'):
        """提交事务用"""
        try:
            self.connect_db()
            self.connecting = True
            cu = self.cu if db == 'reader' else self.cu_pra_sel
            fund_db_pra = self.fund_db if db == 'reader' else self.fund_db_pra

            cu.executemany(sql, rec)
            fund_db_pra.commit()

            self.connecting = False
            self.close_db()
        except:
            return 'fail'
        return 'ok'


class GetValue(ToolClass):
    """获取净值的方法"""

    def __init__(self):
        super(GetValue, self).__init__()
        self.offset = 5
        self.time_name = 'f1_1010'
        self.time_table = 'wind.tb_object_1010'

    def set_offset(self, week: int):
        self.offset = week

    def get_all_trade_days(self, start_date: str, end_date: str):
        """
        获取全交易日
        :param start_date: 起始日期
        :param end_date: 截止日期
        :return:df
        """
        assert end_date >= start_date, '截止日必须大于起始日'

        sql_get_trade_days = f"""
                select {self.time_name} from {self.time_table}
                where {self.time_name} <= '{end_date}' and {self.time_name} >='{start_date}'
                order by {self.time_name} asc 
                """

        sql_res = self._get_db_data(sql_get_trade_days, db='reader')
        df_time = pd.DataFrame(sql_res, columns=['交易日期'])
        return df_time

    def get_values(self, code: str, start_date: str, end_date: str):
        """
        查询净值，自定填补
        :param code:
        :param start_date:
        :param end_date:
        :return: df columns 交易日期，复权单位净值，收益率
        """
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        start_new = start_dt - timedelta(weeks=self.offset)

        start_str = start_new.strftime('%Y%m%d')

        sql = f"""
                     select
                f13_1101 as 交易日期, f21_1101 as 复权单位净值 
                from
                wind.tb_object_1101
                left join wind.tb_object_1090
                on f2_1090 = f14_1101
                where 
                F16_1090= '{code}'
                and
                F13_1101 <=  '{end_date}'
                and
                F13_1101 >= '{start_str}'
                and f4_1090 = 'J'
                order by f13_1101 desc
        """

        sql_res = self._get_db_data(sql, db='reader')
        df = pd.DataFrame(sql_res, columns=['交易日期', '复权单位净值'])
        df_time = self.get_all_trade_days(start_str, end_date)

        df = pd.merge(df_time, df, how='left', on='交易日期')
        df.fillna(method='ffill', inplace=True)

        df['收益率'] = df['复权单位净值'].pct_change()
        df = df.loc[df['交易日期'] >= start_date][:]
        return df




def gen_time_list(st_date='20100101', ed_date='20181231', rq_flag=False):
    rpts = {}
    if rq_flag:
        rpts = {'任期': [st_date, ed_date]}
    for j in range(int(st_date[0:4]), int(ed_date[0:4]) + 1, 1):
        if j == int(st_date[0:4]):
            rpts[str(j)] = [st_date, str(j) + '1231']
        elif j == int(ed_date[0:4]):
            rpts[str(j)] = [str(j) + '0101', ed_date]
        else:
            rpts[str(j)] = [str(j) + '0101', str(j) + '1231']
    return rpts


def gen_season_report_date(start_date:str, end_date:str):
    """
    输入开始结束日期  生成含有的报告期
    :param start:
    :param end:
    :return:
    """
    r_list = []
    season_report = ['0331', '0630', '0930', '1231']
    s_int, e_int = int(start_date[0:4]), int(end_date[0:4])
    for year in range(s_int, e_int + 1):
        r_list += [str(year) + date for date in season_report]
    r_list = [i for i in r_list if i >= start_date and i <= end_date]
    # print(r_list)
    return r_list


# 日期问题
# 取出所有的交易日
sql = '''
                 SELECT
                 F1_1010
                 FROM wind.TB_OBJECT_1010
                 WHERE
                 F1_1010 < '20251231'
                 ORDER BY
                 F1_1010 DESC
                 '''
from sql_con import sql_oracle
trade_dates = pd.DataFrame(sql_oracle.cu.execute(sql).fetchall(), columns=['交易日期'])



# 找出离start_date最近的交易日
def if_trade(start_date):
    """生成指定日期距离最近的交易日期
        功能
        --------
        生成指定日期距离最近的交易日期
        参数
        --------
        输入日期，格式为字符格式，如'20171220'
        返回值
        --------
        返回一个具体的日期，返回格式为字符格式，如'20181011'。如输入日期当天为交易日期，
        则返回当天；否则往前遍历至最近交易日。
        参看
        --------
        无关联函数。
        需要在函数外将交易日期列表存好，需要用trade_dates变量存交易日期，数据格式为'20180120',
        只需要交易日一列即可，trade_dates为dataframe类型，列名需要为'交易日期'。
        示例
        --------
        >>>a = if_trade('20181229')
        >>>a
        '20181228'
        """
    while True:  # 这是一个死循环，只有当break的时候跳出循环，如果不对就会一直循环下去。在本例中，是交易日时跳出循环，不是交易日的时候会出现indexerror
        # 然后又进行判断是否是交易日，直到是交易日跳出循环，返回start_date
        try:
            start_date = trade_dates.loc[trade_dates['交易日期'] == start_date].values[0][0]
            break
        except IndexError:
            # print(start_date, '日期非交易日，前推到最近一交易日')
            temp_date = pd.to_datetime(start_date).date()  # 转换成datetime里面的date格式，如果没有后面的.date那么就是包含具体小时分钟的
            start_date = (temp_date + relativedelta(days=-1)).strftime('%Y%m%d')  # 前一天的日期后又变为'年月日'的格式
    return start_date


# 找出几个月前或几周前的交易日期
def date_gen(days=None, months=None, years=None, end=None):
    """生成指定日期距离最近的交易日期
        功能
        --------
        生成指定日期之前几年/月/日距离最近的交易日期

        参数
        --------
        days:需要往前多少个日历日，格式为int，可以为具体数字，也可为变量
        months:需要往前多少个月份，格式为int，可以为具体数字，也可为变量
        years:需要往前多少个年份，格式为int，可以为具体数字，也可为变量
        end:截止日期，格式为'20171220'，以该截止日期往前距离XX日、XX月、XX年后最近的交易日期
        参数需要写全，如days=3。days、months、years必须输入一个，end也是必要参数

        返回值
        --------
        返回一个具体的日期，返回格式为字符格式，如'20181011'。

        参看
        --------
        if_trade(start_date)：关联函数。

        示例
        --------
        >>>a = date_gen(days = 3,end = '20181220')
        >>>a
        '20181217'

        """
    def none(par):
        if not par:  # 意思是如果par为None时
            par = 0
        return par

    [days, months, years] = [none(days), none(months), none(years)]
    end = pd.to_datetime(end)
    start_date = (end - relativedelta(days=days, months=months, years=years)).strftime('%Y%m%d')
    start_date = if_trade(start_date)  # relativedelta表示时间的移动，之前是以移动一天，现在是移动一个月或者一年之类的
    return start_date







gv = GetValue()

get_values_auto_fill = gv.get_values

if __name__ == '__main__':

    # path = 'dddd'

    # print(to_abspath(path))

    report_list = gen_season_report_date('20180101', '20181210')