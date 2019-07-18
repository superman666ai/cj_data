#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/3 11:16
# @Author  : GaoJian
# @File    : utils_relative_maxback.py


import pandas as pd
import os
import sys
import numpy as np
import cx_Oracle
import time
from functools import reduce
from datetime import datetime, timedelta
from dateutil import rrule
import datetime


from scipy import stats
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing
from scipy.optimize import minimize
from copy import deepcopy



#连接投研数据库
[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
cursor = fund_db.cursor()



# 连接数据库
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
start_time = time.time()
cu = fund_db.cursor()


# 日期问题
# 取出所有的交易日


def find_trade_dates():
    """
    取出所有交易日
    :return:
    """
    sql = '''
                     SELECT
                     F1_1010
                     FROM wind.TB_OBJECT_1010
                     WHERE
                     F1_1010 < '20251231'
                     ORDER BY
                     F1_1010 DESC
                     '''
    trade_dates = pd.DataFrame(cu.execute(sql).fetchall(), columns=['交易日期'])
    return trade_dates

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
            # 所有交易日

            trade_dates = find_trade_dates()
            start_date = trade_dates.loc[trade_dates['交易日期'] == start_date].values[0][0]
            break
        except IndexError:
            # print(start_date, '日期非交易日，前推到最近一交易日')
            temp_date = pd.to_datetime(start_date).date()  # 转换成datetime里面的date格式，如果没有后面的.date那么就是包含具体小时分钟的
            start_date = (temp_date + relativedelta(days=-1)).strftime('%Y%m%d')  # 前一天的日期后又变为'年月日'的格式
    return start_date

###
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



def opendate(code):
	sql = """
	    select 
	    t0.F22_1099 
	    from wind.TB_OBJECT_1099 t0 
	    LEFT OUTER JOIN wind.TB_OBJECT_1090 t1 ON F2_1090 = F1_1099 
	    where  t1.F16_1090 = '%s' """ %(code)
	print(code)
	data1 = cu.execute(sql).fetchall()[0]

	return data1[0]



###
#################################################################################################
# 取市场组合基准
#################################################################################################


# 取市场指数数据
def market_fetch(stock_index):
    """
    功能
    --------
    得到某A股指数的全部收盘价数据

    参数
    --------
    stock_index:指数代码，字符格式，如'000300'，不带后面的后缀

    返回值
    --------
    返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

    参看
    --------
    无关联函数

    示例
    --------
    >>>market_fetch('000300').head()
            000300
日期
20020104  1316.455
20020107  1302.084
20020108  1292.714
20020109  1272.645
20020110  1281.261
     """
    sql1 = '''
    SELECT
      T0.F2_1425 日期,
      T0.F7_1425 AS 复权收盘价
      FROM
        wind.TB_OBJECT_1425 T0
      LEFT JOIN wind.TB_OBJECT_1090 T1 ON T1.F2_1090 = T0.F1_1425
      WHERE
        T1.F16_1090 = '%(index_code)s'
      AND T1.F4_1090 = 'S'
      ORDER BY
        T0.F2_1425
    ''' % {'index_code': stock_index}
    market = pd.DataFrame(cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
    market.index = market['日期']
    del market['日期']
    market.columns = [stock_index]
    return market


def market_fetch_Hongkong(stock_index):
    """
       功能
       --------
       得到某港股指数的全部收盘价数据

       参数
       --------
       stock_index:指数代码，字符格式，如'HSI.HI'

       返回值
       --------
       返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

       参看
       --------
       无关联函数

       示例
       --------
       >>>market_fetch_Hongkong('HSI.HI').head()
               HSI.HI
日期
19640731  100.00
19640831   98.81
19640930  101.21
19641130  101.42
19641231  101.45
        """
    sql1 = '''
    SELECT
      T0.G2_1038 日期,
      T0.G7_1038 AS 收盘价
      FROM
        wind.GB_OBJECT_1038 T0
      LEFT JOIN wind.GB_OBJECT_1001 T1 ON T1.G1_1001 = T0.G1_1038
      WHERE
        T1.G16_1001 = '%(index_code)s'
      ORDER BY
        T0.G2_1038
    ''' % {'index_code': stock_index}
    market = pd.DataFrame(cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
    market.index = market['日期']
    del market['日期']
    market.columns = [stock_index]
    return market


def market_fetch_CBA(stock_index):
    """
        功能
        --------
        得到某中债指数的全部收盘价数据

        参数
        --------
        stock_index:指数代码，字符格式，如'CBA00111'，不带后面的后缀

        返回值
        --------
        返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

        参看
        --------
        无关联函数

        示例
        --------
        >>>market_fetch_CBA('CBA00111').head()
            CBA00111
日期
20020104   99.9642
20020107   99.9349
20020108  100.2792
20020109  100.4043
20020110  100.4195
         """
    sql1 = '''
        SELECT
          T0.F2_1655 日期,
          T0.F3_1655 AS 复权收盘价
          FROM
            wind.TB_OBJECT_1655 T0
          LEFT JOIN wind.TB_OBJECT_1090 T1 ON T1.F2_1090 = T0.F1_1655
          WHERE
            T1.F16_1090 = '%(index_code)s'
          AND T1.F4_1090 = 'S'
          ORDER BY
            T0.F2_1655
        ''' % {'index_code': stock_index}
    market = pd.DataFrame(cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
    market.index = market['日期']
    del market['日期']
    market.columns = [stock_index]
    return market


def get_market_wind(code):
    """
        功能
        --------
        从wind客户端提取指数的从2015年1月以来的全部收盘价数据，一些美股的指数和全球指数数据库中没有，需要从wind客户端取数据

        参数
        --------
        code:指数代码，字符格式，如'000300'，不带后面的后缀

        返回值
        --------
        返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

        参看
        --------
        无关联函数

        示例
        --------
        >>>get_market_wind("892400.MI").head()
            892400.MI
20150105  408.010446
20150106  404.051019
20150107  406.057610
20150108  413.842395
20150109  411.687899
         """
    end_date = datetime.datetime.now() + timedelta(days=-1)
    end_date = end_date.strftime('%Y%m%d')

    df = w.wsd(code, "close", "2015-01-01", end_date, "", "Currency=CNY", usedf=True)[1]
    df.index = pd.Series(df.index).apply(lambda x: str(x)[:4] + str(x)[5:7] + str(x)[8:10])
    df.columns = [code]
    return df



###
def get_track_index(stock_index):
    """
    功能
    --------
    找到某指数的收盘价数据,对于不同类型的指数，有不同的数据来源，此函数将之前定义的函数综合起来

    参数
    --------
    stock_index:基金代码，字符格式，如'000001

    返回值
    --------
    返回一个dataframe,index为日期，有一列为指数收盘价，列名为指数代码

    参看
    --------
    market_fetch()：从数据库取A股指数的收盘价数据
    get_market_wind()：从wind客户端取任意指数数据
    market_fetch_CBA()：从数据库取中债指数收盘价数据
    market_fetch_Hongkong()：从数据库取港股指数收盘价数据

    示例
    --------
    >>>get_track_index('000300.SH').head()
           000300
日期
20020104  1316.455
20020107  1302.084
20020108  1292.714
20020109  1272.645
20020110  1281.261
     """
    i = stock_index
    if i == None:
        market = market_fetch('h11009')
    elif i[-3:] in ['.SH', '.SZ', 'CSI', '.MI']:
        if i == 'RMS.MI':
            market = get_market_wind(i)
        else:
            stock = i[0:6]
            market = market_fetch(stock)
    elif i[-3:] == '.CS':
        stock = i[0:8]
        market = market_fetch_CBA(stock)
    elif i[-3:] == '.HI':
        market = market_fetch_Hongkong(i)
    else:
        market = get_market_wind(i)
    if market.empty:
        market = get_market_wind(i)
        print(str(i) + '指数没有取到,使用客户端取数')
    return market







def find_net_value():
    """
    查询基金的日净值收盘价
    :return:
    """
    end_date = datetime.datetime.now() + timedelta(days=-1)
    end_date = end_date.strftime('%Y%m%d')
    sql = '''
            SELECT
            F16_1090 AS 基金代码,
            日期,
            复权单位净值
            from
            (
            SELECT
            F13_1101 AS 日期,
            F21_1101 AS 复权单位净值,
            F14_1101
            FROM
            wind.TB_OBJECT_1101
            )
            LEFT JOIN wind.TB_OBJECT_1090  ON F2_1090 = F14_1101
            where 日期 > '%(date)s'
            ''' % {'date': date_gen(years=4, end=end_date)}
    net_value = pd.DataFrame(cu.execute(sql).fetchall(), columns=['基金代码', '日期', '基金净值收盘价'])
    net_value_group = net_value.groupby('基金代码')
    return net_value_group




def netvalue(code):
    """
    功能
    --------
    找到某只基金的前4年的全部日净值数据

    参数
    --------
    code:基金代码，字符格式，如'000001'

    返回值
    --------
    返回一个dataframe,列名为['基金代码', '日期', '基金净值收盘价']

    参看
    --------
    无关联函数

    示例
    --------
    >>>a = netvalue('000001')
    >>>a.head()
        基金代码        日期   基金净值收盘价
2629609  000001  20150202  5.904500
2629822  000001  20150203  5.996300
2629993  000001  20150204  5.972100
2699687  000001  20150205  5.943200
2626758  000001  20150206  5.865900
     """
    try:
        net_value_group = find_net_value()
        temp_value = net_value_group.get_group(code).sort_values(by='日期', ascending=True)
    except KeyError:
        temp_value = pd.DataFrame(columns=['基金代码', '日期', '基金净值收盘价'])
    return temp_value

