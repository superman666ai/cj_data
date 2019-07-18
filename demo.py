#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/8 15:33
# @Author  : GaoJian
# @File    : demo.py



import pandas as pd
import os
import sys
import numpy as np
import cx_Oracle
import time
from functools import reduce
from datetime import datetime,timedelta
from dateutil import rrule
import datetime


from scipy import stats
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing
from scipy.optimize import minimize
from copy import deepcopy
w.start()
import time
import math
'''思路：1、取全市场基金的每周五的净值，算周收益率，算出同类平均市场组合 2、算每一个分类下的市场组合 3、算每个基金的业绩基准市场组合
3、根据前面算的净值数据和市场组合数据来计算各种指标
4、在每个分类下进行排名，然后打标签
使用说明：修改输入输出路径,将所需文件（分类表、同业存单最新评级、历史星级）放入输入路径'''
start_time = time.time()
# 连接数据库
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
start_time = time.time()
cu = fund_db.cursor()
# 为避免连接Oracle乱码问题
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'

###################################################################################################
# 所有的输入
path0 = os.path.dirname(os.path.realpath(__file__)) + '/'

path = 'G:/3、python源文件/Fund return/输出结果/业绩标签/打标签结果' # 输出的路径
path1 = 'G:/3、python源文件/Fund return/输出结果/业绩标签/标签输入'  # 输入的路径
path = path0+'输出'#输出的路径
path1 = path0+'输入文件'#输入的路径

data2 = pd.read_excel(path1 + '/分类-0213.xlsx', dtype=str)
rating = pd.read_excel(path1+'/同业存单最新评级信息.xlsx',dtype=str)
star = pd.read_excel(path1+"/历史星级.xlsx",dtype = {'基金代码':str})

######################################################################################################
#准备工作
######################################################################################################
end_date=datetime.datetime.now()+timedelta(days=-1)
end_date=end_date.strftime('%Y%m%d')
print('更新日期：'+str(end_date))

net0 = data2[['基金代码', '基金简称','一级分类','二级分类']].sort_values(by='基金代码', ascending=True).reset_index(drop=True)

data_act = data2[(data2.是否指数基金!='是')&(data2.成立日<end_date)][['基金代码', '基金简称','二级分类']]
data_pass =data2[(data2.是否指数基金=='是')&(data2.成立日<end_date)&(data2.是否ETF !='ETF联接')][['基金代码', '基金简称','二级分类']]

net_act = data_act[data_act.二级分类.isin(['标准配置型','可转债型','环球股票','普通债券型','股票型','纯债型','灵活配置型', '激进配置型','激进债券型',
                                      '保守配置型', '沪港深股票型','沪港深配置型',' 纯债型'])].reset_index(drop=True)
net_act_plus = data_act[data_act.二级分类.isin(['标准配置型','可转债型','环球股票','普通债券型','股票型','纯债型','灵活配置型', '激进配置型','激进债券型',
                                           '保守配置型', '沪港深股票型','沪港深配置型',' 纯债型','货币型'])].reset_index(drop=True)
net_pass = data_pass[data_pass.二级分类.isin(['标准配置型','可转债型','环球股票','普通债券型','股票型','纯债型','灵活配置型', '激进配置型','激进债券型',
                                       '保守配置型', '沪港深股票型','沪港深配置型',' 纯债型'])].reset_index(drop=True)

# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/3 14:28
# @Author  : GaoJian
# @File    : relative_maxback.py

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

# 连接数据库
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
start_time = time.time()
cu = fund_db.cursor()

# 投研平台库
[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
fund_db_pra = cx_Oracle.connect(user=userNamepif, password=passwordpif, dsn=hostIPpif + '/' + dbNamepif)
cu_pra = fund_db_pra.cursor()


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


def find_net_value(code, start_date, end_date, type):
    """
    查询基金的日净值收盘价
    :param code:
    :param start_date:
    :param end_date:
    :return:
    """
    if type == "fund":
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
                where wind.TB_OBJECT_1090.F16_1090='%(code)s' AND 日期 >= '%(start_date)s'AND 日期 <= '%(end_date)s'
                ''' % {'code': code, 'start_date': start_date, "end_date": end_date}

        net_value = pd.DataFrame(cu.execute(sql).fetchall(), columns=['基金代码', '日期', '基金净值收盘价'])


    elif type == "fof":
        sql = """
                SELECT
                fundid ,
                tradedate,
                closeprice
                from
                t_fof_value_info

                where fundid='%(code)s' AND tradedate >= '%(start_date)s'AND tradedate <= '%(end_date)s'
                """ % {'code': code, 'start_date': start_date, "end_date": end_date}

        net_value = pd.DataFrame(cu_pra.execute(sql).fetchall(), columns=['基金代码', '日期', '基金净值收盘价'])
    else:
        return pd.DataFrame([], columns=['基金代码', '日期', '基金净值收盘价'])

    return net_value


def find_tag_value(tag=None):
    """
    查询某指数的日净值
    :param tag:
    :param start_date:
    :param end_date:
    :return:
    """
    i = tag
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
    # print(market)
    return market





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
trade_dates = pd.DataFrame(cu.execute(sql).fetchall(), columns=['交易日期'])

# 根据当前日期判断最新季报日期
# 每季度截止15个工作日内披露季报，经过测算，16个工作日内全部基金的季报披露完，保险起见，设置大于等于17个工作日为阈值，即17个工作日之后认为季报全部披露
if end_date[4:6] in ['01','02','03']:
    report = str(int(end_date[0:4])-1)+'1231'
    day = len(trade_dates[(trade_dates.交易日期<=end_date)&(trade_dates.交易日期>report)])
    if day >= 17:
        reportdate = report
    else:
        reportdate = str(int(end_date[0:4])-1)+'0930'
elif end_date[4:6] in ['04','05','06']:
    report = end_date[0:4] + '0331'
    day = len(trade_dates[(trade_dates.交易日期 <= end_date) & (trade_dates.交易日期 > report)])
    if day >= 17:
        reportdate = report
    else:
        reportdate = str(int(end_date[0:4]) - 1) + '1231'
elif end_date[4:6] in ['07','08','09']:
    report = end_date[0:4] + '0630'
    day = len(trade_dates[(trade_dates.交易日期 <= end_date) & (trade_dates.交易日期 > report)])
    if day >= 17:
        reportdate = report
    else:
        reportdate = end_date[0:4] + '0331'
else:
    report = end_date[0:4] + '0930'
    day = len(trade_dates[(trade_dates.交易日期 <= end_date) & (trade_dates.交易日期 > report)])
    if day >= 17:
        reportdate = report
    else:
        reportdate = end_date[0:4] + '0630'

print('使用季报日期:'+str(reportdate))

#当前文件名称
thisfilename = os.path.basename(__file__)

#当前算法id
algid = '0000000003'

#本批次号(即报告期日期)
batchno = reportdate

#当天日期
todaydate = datetime.datetime.now().strftime('%Y%m%d')

#连接投研数据库
[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
cursor = fund_db.cursor()

#insertsql =  "INSERT INTO fund_tag_info( fundId, tagId, tagValue, endableFlag ) VALUES(:fundId, :tagId, :tagValue, :endableFlag)"
sql_tag_insert = "INSERT INTO fund_tag_info( fundId, tagId, tagValue, algid, batchno, endableFlag, createUser, createDate ) VALUES(:fundId, :tagId, :tagValue, :algid, :batchno, :endableFlag, :createuser, :createDate)"
sql_index_insert =  "INSERT INTO fund_index_info( fundId, indexCode, indexValue, reportdate, algid, batchno,  createUser, createDate ) VALUES(:fundId, :indexCode, :indexValue,:reportdate, :algid, :batchno, :createuser, :createDate)"
rec = []

rec_d = [batchno, algid]
def DBDelete(sql, rec):
	try:
		cursor.prepare(sql)
		cursor.execute(None, rec)
		fund_db.commit()
		print('delete suc')
	except:
		fund_db.rollback()
		print('delete fail')
print('delete fund_tag_info表')
sql_delete = 'DELETE from fund_tag_info where  batchno = :batchno and algid = :algid'
#DBDelete(sql_delete, rec_d)
print('delete fund_index_info表')
sql_delete = 'DELETE from fund_index_info where  batchno = :batchno and algid = :algid'
#DBDelete(sql_delete, rec_d)

#定义数据操作函数
def DBInsert(sql, rec):
	try:
		cursor.prepare(sql)
		cursor.executemany(None, rec)
		fund_db.commit()
		print('insert suc')
	except cx_Oracle.DatabaseError as e:
		fund_db.rollback()
		#其他错误处理
		raise(e)

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


def opendate(code):
	sql = "select t0.F22_1099 from wind.TB_OBJECT_1099 t0 LEFT OUTER JOIN wind.TB_OBJECT_1090 t1 ON F2_1090 = F1_1099 where  t1.F16_1090 = '%s' " %(code)
	print(code)
	data1 = cu.execute(sql).fetchall()[0]

	return data1[0]




# 找到距离某日期最近的周五的日期
def get_friday(start_date):
    """
    功能
    --------
    找到距离某日期最近的周五的日期,由于wind中计算周频的数据都用的是每周五的数据计算，于是需要根据指定日期往前寻找最近一个周五的日期

    参数
    --------
    start_date:日期,字符格式,如'20190110'

    返回值
    --------
    返回一个具体的日期，返回格式为字符格式，如'20190105'。

    参看
    --------
    无关联函数

    示例
    --------
    >>>a = get_friday('20190110')
    >>>a
    '20190104'
     """
    while True:  # 这是一个死循环，只有当break的时候跳出循环，如果不对就会一直循环下去。在本例中，是周五时跳出循环，不是周五的时候会前寻
        # 然后又进行判断是否是周五，直到是交易日跳出循环，返回start_date
        if pd.to_datetime(start_date).weekday() == 4:
            break
        else:
            # print(start_date, '日期非星期五，前推到最近一交易日')
            temp_date = pd.to_datetime(start_date).date()  # 转换成datetime里面的date格式，如果没有后面的.date那么就是包含具体小时分钟的
            start_date = (temp_date + relativedelta(days=-1)).strftime('%Y%m%d')  # 前一天的日期后又变为'年月日'的格式
    return start_date



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
        '''%{'date':date_gen(years=4,end=end_date)}
net_value = pd.DataFrame(cu.execute(sql).fetchall(), columns=['基金代码', '日期', '基金净值收盘价'])
net_value_group = net_value.groupby('基金代码')

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
        temp_value = net_value_group.get_group(code).sort_values(by='日期', ascending=True)
    except KeyError:
        temp_value = pd.DataFrame(columns=['基金代码', '日期', '基金净值收盘价'])
    return temp_value





#################################################################################################
# 风格稳定性标签(约19分钟)
#################################################################################################


trade_dates2 = trade_dates[trade_dates.交易日期<end_date]
trade_dates2.columns=['日期']

def get_netfull(x):
    x = x.sort_values(by = '日期',ascending = False)
    trade = trade_dates2[trade_dates2.日期>x['日期'].iloc[-1]]
    net_full = pd.merge(trade, x, on='日期', how='left').reset_index(drop=True)
    net_full = net_full.fillna(method='bfill')
    return net_full
net_full = net_value.groupby('基金代码').apply(lambda x:get_netfull(x))
net_full_group = net_full.groupby('基金代码')

print('生成每只基金的每周五的净值数据')
# 生成每只基金的每周五的净值数据
end = get_friday(end_date)
dates2 = pd.Series(name='日期')
lenth = int(170)
for i in range(lenth)[:: -1]:
    temp = pd.Series(date_gen(days=7 * i, end=end), name='日期')
    dates2 = dates2.append(temp)

def get_yeild(x):
    x = x.sort_values(by = '日期',ascending = True)
    x['收益率'] = x['基金净值收盘价'].pct_change()
    del x['基金净值收盘价']
    return x
net_friday =  net_full[net_full.日期.isin(dates2)]
net_friday = net_friday.groupby('基金代码').apply(lambda x : get_yeild(x))
net_friday_group = net_friday.groupby('基金代码')



market_RBSA=grow_100.join([value_100,grow_200,value_200,grow_small,value_small,bond_1,bond_1_3,bond_3_5,bond_5_7,
                      bond_7_10,bond_10,HSI,SPX])

market_RBSA = market_RBSA.loc[dates2]
for i in ['818100PGTR.CI', '818100PVTR.CI', '818200PGTR.CI', '818200PVTR.CI',
       '818300PGTR.CI', '818300PVTR.CI', 'CBA00111',
       'CBA00121', 'CBA00131', 'CBA00141', 'CBA00151', 'CBA00161' ,'HSI.HI', 'SPX.GI']:
    market_RBSA[str(i)+'收益率'] = market_RBSA[str(i)].pct_change()
    del market_RBSA[str(i)]
market_RBSA = market_RBSA.join(r[['一年定存利率']])
for i in range(len(market_RBSA)):
    if np.isnan(market_RBSA.iloc[i,-1]):
        market_RBSA.iloc[i, -1] = market_RBSA.iloc[i-1,-1]


#取某基金的周收益率序列
def get_weekly_yeild(x):
    try:
        df = net_friday_group.get_group(x)
        df.index = df['日期']
        del df['日期']
        del df['基金代码']
        df.columns = [x]
    except KeyError:
        df = pd.DataFrame(index=dates2)
        df[x] = np.nan
    return df




class RBSA:


    def rbsa(self, start_date, end_date, year_lag):
        """
        功能
        --------
        计算近几年的RBSA回归结果，要先定义好net1和codes，net1是需要基金的基金列表，其中有成绩年限，codes为net1中的基金代码

        参数
        --------
        year_lag:整数，如1,2,3

        返回值
        --------
        返回一个dateframe，其中包含每一只基金回归的R2和回归系数

        参看
        --------
        无关联函数

        示例
        --------
        >>>net1 = net0[net0.二级分类.isin(['标准配置型','可转债型','环球股票','普通债券型','股票型','纯债型','灵活配置型', '激进配置型','激进债券型',
    '保守配置型', '沪港深股票型','沪港深配置型',' 纯债型'])]
        >>>codes = net1['基金代码']
        >>>RBSA(1).head()
       近1年回归系数（标普A股100纯成长）  近1年回归系数（标普A股100纯价值）   ...      近1年R^2    基金代码
    0         1.152305e-01             0.283733                         ...    0.896055  000001
    1         0.000000e+00             0.169836                         ...    0.734744  000003
    2         6.484767e-19             0.019776                         ...    0.463100  000005
    3         3.230120e-17             0.000000                         ...    0.959403  000008
    4         3.147889e-01             0.233594                         ...    0.889642  000011
         """

        end = get_friday(end_date)
        start = date_gen(years=year_lag, end=end_date)
        col = ["近" + str(year_lag) + "年回归系数（标普A股100纯成长）"
        ,"近" + str(year_lag) + "年回归系数（标普A股100纯价值）"
        , "近" + str(year_lag) + "年回归系数（标普A股200纯成长）"
        , "近" + str(year_lag) + "年回归系数（标普A股200纯价值）"
        , "近" + str(year_lag) + "年回归系数（标普A股小盘纯成长）"
        ,"近" + str(year_lag) + "年回归系数（标普A股小盘纯价值）"
               ,"近" + str(year_lag) + "年回归系数（中债-新综合财富(1年以下)）"
         ,"近" + str(year_lag) + "年回归系数（中债-新综合财富(1-3年)）"
         ,"近" + str(year_lag) + "年回归系数（中债-新综合财富(3-5年)）"
         ,"近" + str(year_lag) + "年回归系数（中债-新综合财富(5-7年)）"
        , "近" + str(year_lag) + "年回归系数（中债-新综合财富(7-10年)）"
       , "近" + str(year_lag) + "年回归系数（中债-新综合财富(10年以上)）"
            , "近" + str(year_lag) + "年回归系数（恒生指数）"
            , "近" + str(year_lag) + "年回归系数（标普500）"
            , "近" + str(year_lag) + "年回归系数（一年定存利率）"
               ]
        col2 = ['基金代码',"近" + str(year_lag) + "年R^2"]
        R2_result =pd.DataFrame(columns=col2.extend(col))
        for i in codes:
            global net1
            index1 = net1[net1.基金代码 == i].index.tolist()[0]
            if net1.loc[index1,'成立年限'] < year_lag:
                pass

            else:
                dates = get_weekly_yeild(i).loc[start:end]
                dates = dates.join(market_RBSA)
                dates = dates.drop(dates.index[0])
                df_big = dates
                df_big=df_big.dropna(axis=0,how='any')
                X_big = df_big.drop(str(i),1)
                y_big = df_big[str(i)]
                #进行带约束条件的回归
                x0 = np.random.rand(15)
                x0 /= sum(x0)
                X = np.mat(X_big)
                Y = np.mat(y_big)
                func = lambda x: ((Y.T - X * (np.mat(x).T)).T * (Y.T - X * (np.mat(x).T))).sum()
                cons4 = ({'type': 'ineq', 'fun': lambda x: x[0]},
                {'type': 'ineq', 'fun': lambda x: x[1]},
                {'type': 'ineq', 'fun': lambda x: x[2]},
                {'type': 'ineq', 'fun': lambda x: x[3]},
                {'type': 'ineq', 'fun': lambda x: x[4]},
                {'type': 'ineq', 'fun': lambda x: x[5]},
                {'type': 'ineq', 'fun': lambda x: x[6]},
                {'type': 'ineq', 'fun': lambda x: x[7]},
                {'type': 'ineq', 'fun': lambda x: x[8]},
                {'type': 'ineq', 'fun': lambda x: x[9]},
                {'type': 'ineq', 'fun': lambda x: x[10]},
                {'type': 'ineq', 'fun': lambda x: x[11]},
                {'type': 'ineq', 'fun': lambda x: x[12]},
                {'type': 'ineq', 'fun': lambda x: x[13]},
                {'type': 'ineq', 'fun': lambda x: x[14]},

                {'type': 'ineq', 'fun': lambda x: 1-x[0]},
                {'type': 'ineq', 'fun': lambda x: 1-x[1]},
                {'type': 'ineq', 'fun': lambda x: 1-x[2]},
                {'type': 'ineq', 'fun': lambda x: 1-x[3]},
                {'type': 'ineq', 'fun': lambda x: 1-x[4]},
                {'type': 'ineq', 'fun': lambda x: 1-x[5]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[6]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[7]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[8]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[9]},
                {'type': 'ineq', 'fun': lambda x: 1- x[10]},
                {'type': 'ineq', 'fun': lambda x: 1- x[11]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[12]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[13]},
                {'type': 'ineq', 'fun': lambda x: 1 - x[14]},
                {'type': 'eq', 'fun': lambda x: x[0]+x[1]+x[2]+x[3]+x[4]+x[5]+x[6]+x[7]+x[8]+x[9]+x[10]+x[11]+x[12]+x[13]+x[14]-1})
                res = minimize(func, x0, method='SLSQP', constraints=cons4)
                R2 = 1 - res.fun / ((np.ravel(y_big).var()) * len(y_big))
                if R2 <0:
                    R2 = 0
                res.x[res.x < 0] = 0
                df3 = pd.DataFrame(res.x)
                df3 = df3.T
                df3.columns = col
                df3["近" + str(year_lag) + "年R^2"] = R2
                df3['基金代码'] = i
                R2_result = R2_result.append(df3,ignore_index=True)
        return R2_result


    def count(self):

        end = get_friday(end_date)
        year_lag = 1
        start = date_gen(years=year_lag, end=end_date)
        col = ["近" + str(year_lag) + "年回归系数（标普A股100纯成长）"
            , "近" + str(year_lag) + "年回归系数（标普A股100纯价值）"
            , "近" + str(year_lag) + "年回归系数（标普A股200纯成长）"
            , "近" + str(year_lag) + "年回归系数（标普A股200纯价值）"
            , "近" + str(year_lag) + "年回归系数（标普A股小盘纯成长）"
            , "近" + str(year_lag) + "年回归系数（标普A股小盘纯价值）"
            , "近" + str(year_lag) + "年回归系数（中债-新综合财富(1年以下)）"
            , "近" + str(year_lag) + "年回归系数（中债-新综合财富(1-3年)）"
            , "近" + str(year_lag) + "年回归系数（中债-新综合财富(3-5年)）"
            , "近" + str(year_lag) + "年回归系数（中债-新综合财富(5-7年)）"
            , "近" + str(year_lag) + "年回归系数（中债-新综合财富(7-10年)）"
            , "近" + str(year_lag) + "年回归系数（中债-新综合财富(10年以上)）"
            , "近" + str(year_lag) + "年回归系数（恒生指数）"
            , "近" + str(year_lag) + "年回归系数（标普500）"
            , "近" + str(year_lag) + "年回归系数（一年定存利率）"
               ]
        col2 = ['基金代码', "近" + str(year_lag) + "年R^2"]
        R2_result = pd.DataFrame(columns=col2.extend(col))

        dates = get_weekly_yeild(i).loc[start:end]
        dates = dates.join(market_RBSA)
        dates = dates.drop(dates.index[0])
        df_big = dates
        df_big = df_big.dropna(axis=0, how='any')
        X_big = df_big.drop(str(i), 1)
        y_big = df_big[str(i)]
        # 进行带约束条件的回归
        x0 = np.random.rand(15)
        x0 /= sum(x0)
        X = np.mat(X_big)
        Y = np.mat(y_big)
        func = lambda x: ((Y.T - X * (np.mat(x).T)).T * (Y.T - X * (np.mat(x).T))).sum()
        cons4 = ({'type': 'ineq', 'fun': lambda x: x[0]},
                 {'type': 'ineq', 'fun': lambda x: x[1]},
                 {'type': 'ineq', 'fun': lambda x: x[2]},
                 {'type': 'ineq', 'fun': lambda x: x[3]},
                 {'type': 'ineq', 'fun': lambda x: x[4]},
                 {'type': 'ineq', 'fun': lambda x: x[5]},
                 {'type': 'ineq', 'fun': lambda x: x[6]},
                 {'type': 'ineq', 'fun': lambda x: x[7]},
                 {'type': 'ineq', 'fun': lambda x: x[8]},
                 {'type': 'ineq', 'fun': lambda x: x[9]},
                 {'type': 'ineq', 'fun': lambda x: x[10]},
                 {'type': 'ineq', 'fun': lambda x: x[11]},
                 {'type': 'ineq', 'fun': lambda x: x[12]},
                 {'type': 'ineq', 'fun': lambda x: x[13]},
                 {'type': 'ineq', 'fun': lambda x: x[14]},

                 {'type': 'ineq', 'fun': lambda x: 1 - x[0]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[1]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[2]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[3]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[4]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[5]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[6]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[7]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[8]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[9]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[10]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[11]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[12]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[13]},
                 {'type': 'ineq', 'fun': lambda x: 1 - x[14]},
                 {'type': 'eq',
                  'fun': lambda x: x[0] + x[1] + x[2] + x[3] + x[4] + x[5] + x[6] + x[7] + x[8] + x[9] + x[10] + x[11] +
                                   x[12] + x[13] + x[14] - 1})
        res = minimize(func, x0, method='SLSQP', constraints=cons4)
        R2 = 1 - res.fun / ((np.ravel(y_big).var()) * len(y_big))
        if R2 < 0:
            R2 = 0
        res.x[res.x < 0] = 0
        df3 = pd.DataFrame(res.x)
        df3 = df3.T
        df3.columns = col
        df3["近" + str(year_lag) + "年R^2"] = R2
        df3['基金代码'] = i
        R2_result = R2_result.append(df3, ignore_index=True)



#输出计算结果
#1年
net1 = net0[net0.二级分类.isin(['标准配置型','可转债型','环球股票','普通债券型','股票型','纯债型','灵活配置型', '激进配置型','激进债券型',
'保守配置型', '沪港深股票型','沪港深配置型',' 纯债型'])]
codes = net1['基金代码']
R2_1 = RBSA(1)
R2_1 = pd.merge(net0[['基金代码','基金简称','成立年限','二级分类']],R2_1,on='基金代码',how='left')

#2年
R2_2 = RBSA(2)
R2_2 = pd.merge(net0[['基金代码','基金简称','成立年限','二级分类']],R2_2,on='基金代码',how='left')

#3年
R2_3 = RBSA(3)
R2_3 = pd.merge(net0[['基金代码','基金简称','成立年限','二级分类']],R2_3,on='基金代码',how='left')

writer = pd.ExcelWriter(path+'/RBSA'+str(end_date)+'.xlsx')
R2_1.to_excel(writer,'近1年回归结果',index = False)
R2_2.to_excel(writer,'近2年回归结果',index = False)
R2_3.to_excel(writer,'近3年回归结果',index = False)
writer.save()


stock =net1[['基金代码','二级分类']]
stock = pd.merge(stock,R2_1[['基金代码','近1年R^2']],on='基金代码',how = 'left')
stock = pd.merge(stock,R2_2[['基金代码','近2年R^2']],on='基金代码',how = 'left')
stock = pd.merge(stock,R2_3[['基金代码','近3年R^2']],on='基金代码',how = 'left')

kind = stock['二级分类'].drop_duplicates()
for type in kind:
    if type =='纯债型':
        all = stock[(stock.二级分类 == type)|(stock.二级分类 == ' 纯债型')]
    else:
        all = stock[stock.二级分类 == type]
    for i in [1,2,3]:
        try:
            df = all['近' + str(i) + '年R^2'].dropna()
            high = np.percentile(df, 80)
            low = np.percentile(df, 20)
            stock.loc[(stock.二级分类 == type) & (stock['近' + str(i) + '年R^2'] > high), str(i) + '年风格稳定性'] = '高-' + str(i) + '年风格稳定性'
            stock.loc[(stock.二级分类 == type) & (stock['近' + str(i) + '年R^2'] <= low) & (stock['近' + str(i) + '年R^2'] >= 0), str(i) + '年风格稳定性'] = '低-' + str(i) + '年风格稳定性'
            stock.loc[(stock.二级分类 == type) & (stock['近' + str(i) + '年R^2'] <= high) & (stock['近' + str(i) + '年R^2'] > low), str(i) + '年风格稳定性'] = '中-' + str(i) + '年风格稳定性'
        except IndexError:
            pass
Style_stability = stock
del Style_stability['二级分类']
Performance = pd.merge(net0,Style_stability,on = '基金代码',how = 'left')

for i in range(3):
	#风格稳定性 标签入库
	pdData = Style_stability[['基金代码','%d年风格稳定性'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000100200%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	#DBInsert(sql_tag_insert, rec)

	#风格稳定性 标签衍生指标入库
	pdData = Performance[['基金代码', '近%d年R^2'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'Last%dYearR2'%(i+1), str(value[1]), None, algid, batchno,  thisfilename, todaydate))
	#DBInsert(sql_index_insert, rec)

print('跑完风格稳定性标签')