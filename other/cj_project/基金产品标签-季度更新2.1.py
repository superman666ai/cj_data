# -*- coding: utf-8 -*-

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
from WindPy import w
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


def MaxDrawdown(df):
    """
    功能
    --------
    根据基金的净值数据：df中净值数据列 来计算最大回撤率（正）

    参数
    --------
    df:一个dataframe,其中应该有一列是基金净值序列，按日期升序排列，净值那一列的列名必须为'基金净值收盘价'

    返回值
    --------
    返回一个数值，float格式

    参看
    --------
    无关联函数

    示例
    --------
    >>>df = pd.DataFrame([1,2,3,5,1],columns = ['基金净值收盘价'])
    >>>MaxDrawdown(df)
    0.8
     """
    return_list = list(df['基金净值收盘价'])
    i = np.argmax((np.maximum.accumulate(return_list) - return_list) / np.maximum.accumulate(return_list))  # 结束位置
    if i == 0:
        return 0
    j = np.argmax(return_list[:i])  # 开始位置
    return (return_list[j] - return_list[i]) / (return_list[j])

def days(str1, str2):
    """     功能
            --------
            生成两个日期间隔的天数，用来算成立年限
            参数
            --------
            str1:日期1(时间晚的那个),格式为字符格式，如'20181011'
            str2:日期2(时间早的那个),格式为字符格式，如'20180911'
            返回值
            --------
            返回一个数字,int格式，如 10
            参看
            --------
            无关联函数
            示例
            --------
            >>>a = days('20181011','20180911')
            >>>a
            30
            """
    date1 = datetime.datetime.strptime(str1[0:10], "%Y%m%d")
    date2 = datetime.datetime.strptime(str2[0:10], "%Y%m%d")
    num = (date1 - date2).days
    return num


for i in net0.index:
    code = net0.loc[i, '基金代码']
    net0.loc[i, '成立日期'] = opendate(code)
    net0.loc[i, '成立年限'] = days(end_date, opendate(code)) / 365

del net0['成立日期']

net_bond = net0[net0.二级分类.isin(['保守配置型','纯债型','激进债券型','普通债券型','可转债型',' 纯债型'])]

#生成前4个季度报告期
dates_report = [date_gen(months=i,end = reportdate) for i in range(0,12,3)]
def get_report(dates):
    """ 功能
        --------
        将3月6月9月12月的日期转换为当月最后一天的日期，为了生成报告期日期

        参数
        --------
        dates:格式为字符格式，如'20181211',输入应为3月6月9月12月的某一日期
        返回值
        --------
        返回一个具体的日期，返回格式为字符格式，如'20181231'。

        参看
        --------
        无关联函数。

        示例
        --------
        >>>a = get_report('20181211')
        >>>a
        '20181231'
        """
    for i in range(len(dates)):
        if dates[i][4:6] in ['03','12']:
            dates[i] = dates[i][0:6] + '31'
        else:
            dates[i] = dates[i][0:6] + '30'
    return dates
dates_report = get_report(dates_report)
dates_str = str(tuple(dates_report))

#################################################################################################
# 将基金净值和全部交易日合并，取前一个净值公布日的净值填充缺失值
# 取每只基金每周五的净值数据，并求周收益率
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
    df = w.wsd(code, "close", "2015-01-01", end_date, "","Currency=CNY", usedf=True)[1]
    df.index = pd.Series(df.index).apply(lambda x: str(x)[:4] + str(x)[5:7] + str(x)[8:10])
    df.columns = [code]
    return df

# 中证800
market2 = market_fetch('000906')
market1 = market_fetch('000001')
market1 = market1[market1.index < '20050105']
a = market2[market2.index == '20050104'].iloc[0, 0] / market1[market1.index== '20050104'].iloc[-1, 0]
market1['000906'] = market1['000001'] * a
index = market1[market1.index == '20050104'].index.tolist()[0]
market_800 = pd.concat([market1[['000906']] , market2], axis=0).drop([index])

# 中证国债
m_country = market_fetch('h11006')

# 恒生指数
HSI  = get_market_wind('HSI.HI')

#中证综合债
m_bond = market_fetch('h11009')
# 中证短债
market_short = market_fetch('h11015')
# 中证可转债
market_tran = market_fetch('000832')
# MSCI
MSCI  = get_market_wind("892400.MI")


market=market_800.join([m_country,HSI,m_bond,market_short,market_tran,MSCI])
market.columns = ['中证800','中证国债','恒生指数','中证综合债','中证短债','中证可转债','MSCI']


#跑RBSA的市场组合
#标普500
SPX  = get_market_wind('SPX.GI')

#标普中国A股100纯成长总收益指数
grow_100 = get_market_wind('818100PGTR.CI')

#标普中国A股100纯价值总收益指数
value_100 = get_market_wind('818100PVTR.CI')

#标普中国A股200纯成长总收益指数
grow_200 = get_market_wind('818200PGTR.CI')

#标普中国A股200纯价值总收益指数
value_200 = get_market_wind('818200PVTR.CI')

#标普中国A股小盘纯成长总收益指数
grow_small = get_market_wind('818300PGTR.CI')

#标普中国A股小盘纯价值总收益指数
value_small = get_market_wind('818300PVTR.CI')


#se4_4中债-新综合财富指数
bond_1 = market_fetch_CBA('CBA00111')
bond_1_3 = market_fetch_CBA('CBA00121')
bond_3_5 = market_fetch_CBA('CBA00131')
bond_5_7 = market_fetch_CBA('CBA00141')
bond_7_10  = market_fetch_CBA('CBA00151')
bond_10 = market_fetch_CBA('CBA00161')

#一年定存利率
r = w.edb("M0043808", "2015-01-08", end_date,usedf=True)[1]
r['时间'] = r.index
r = r.reset_index(drop=True)
r.index = pd.Series(r['时间']).apply(lambda x: str(x)[:4] + str(x)[5:7] + str(x)[8:10])
r.columns = ['一年定存利率','时间']

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


#生成每个类别的基准收益率
market_all = market.loc[dates2]
for i in ['中证800','中证国债','恒生指数','中证综合债','中证短债','中证可转债','MSCI']:
    market_all[str(i)+'收益率'] = market_all[str(i)].pct_change()
def get_market(type):
    """
    功能
    --------
    提取某个二级分类的市场组合近三年的周收益率数据，

    参数
    --------
    type:二级分类，字符格式，如'股票型'

    返回值
    --------
    返回一个pd.Series，name = '市场组合收益率',index为日期，为市场组合周收益率数据，按日期升序排列

    参看
    --------
    无关联函数

    示例
    --------
    >>>get_market('股票型')
日期
20151030         NaN
20151106    0.066762
20151113   -0.004076
20151120    0.014694
20151127   -0.056840
20151204    0.029192
     """
    if type =='股票型':
        market_type  = market_all['中证800收益率']
    elif type == '激进配置型':
        market_type = market_all['中证800收益率']*0.8+market_all['中证国债收益率']*0.2
    elif type =='标准配置型':
        market_type = market_all['中证800收益率']*0.6+market_all['中证国债收益率']*0.4
    elif type == '保守配置型':
        market_type = market_all['中证800收益率'] * 0.2 + market_all['中证国债收益率'] * 0.8
    elif type == '灵活配置型':
        market_type = market_all['中证800收益率'] * 0.5 + market_all['中证国债收益率'] * 0.5
    elif type == '沪港深股票型':
        market_type = market_all['中证800收益率'] * 0.45 + market_all['中证国债收益率'] * 0.1+market_all['恒生指数收益率']*0.45
    elif type == '沪港深配置型':
        market_type = market_all['中证800收益率'] * 0.35 + market_all['中证国债收益率'] * 0.3 + market_all['恒生指数收益率'] * 0.35
    elif type =='纯债型':
        market_type = market_all['中证综合债收益率']
    elif type == '普通债券型':
        market_type = market_all['中证综合债收益率']*0.9+market_all['中证800收益率'] * 0.1
    elif type == '激进债券型':
        market_type = market_all['中证综合债收益率']*0.8+market_all['中证800收益率'] * 0.2
    elif type =='短债型':
        market_type = market_all['中证短债收益率']
    elif type =='可转债型':
        market_type = market_all['中证可转债收益率']
    elif type =='环球股票':
        market_type = market_all['MSCI收益率']
    market_type.name = '市场组合收益率'
    return  market_type

#算每一个类别同类平均的收益率，生成每个类别的同类基准收益率
net_kind = pd.merge(net_friday,data2[['基金代码','二级分类']],on = '基金代码',how='left')
market_mean = net_kind.groupby(['二级分类','日期']).mean()
#取时market_mean.ix['股票型']

print('市场组合就绪')


#################################################################################################
# 风格稳定性标签(约19分钟)
#################################################################################################
def RBSA(year_lag):
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


#######################################################################################################################
# 持仓标签
# 信用评级估算标签
#######################################################################################################################
#取前五大重仓债券的信息

#全市场持有证券明细，不包括QD基金
sql_stock='''
select    基金代码,F6_1102,持有证券代码,G16_1001,OB_OBJECT_NAME_1090,G4_1001,OB_OBJECT_NAME_1024,G6_1001,持有比例,持有证券ID
from 
(
select 基金代码,F6_1102,持有证券代码,OB_OBJECT_NAME_1090,OB_OBJECT_NAME_1024,持有比例,持有证券ID
from
(
select 基金代码,F6_1102,F16_1090 as 持有证券代码,F4_1090 as 证券类型代码,持有比例,持有证券ID,OB_OBJECT_NAME_1090
from
(select F16_1090 as 基金代码,F7_1102 as 基金ID, F6_1102,F3_1102 as 持有证券ID,F5_1102 as 持有比例
from wind.TB_OBJECT_1102 left join wind.TB_OBJECT_1090
on F7_1102 = F2_1090
where
F6_1102 = '%(date)s' )
left join wind.TB_OBJECT_1090
on 持有证券ID = F2_1090)
left join wind.TB_OBJECT_1024
on 证券类型代码 = F1_1024)
left join wind.GB_OBJECT_1001
on 持有证券ID=G1_1001
'''%{'date':reportdate}
holded = pd.DataFrame(cu.execute(sql_stock).fetchall(), columns=['基金代码','报告期','A股代码','港股代码','A股简称','港股简称','A股类型','港股类型','持有比例','持有证券ID']).sort_values(
        by='基金代码', ascending=True).reset_index(drop=True)

holded['证券代码'] = holded.apply(lambda x: x[2] or x[3],axis = 1)
holded['证券简称'] = holded.apply(lambda x: x[4] or x[5],axis = 1)
holded['证券类型'] = holded.apply(lambda x: x[6] or x[7],axis = 1)

#筛选其中的偏债型基金
holded1 = holded[holded.基金代码.isin(net_bond['基金代码'])]

#直接全部取出所有债券的评级信息，并保留最新的评级信息
sql2 = '''
        SELECT
          T0.F16_1090 AS 证券代码,
          T1.F3_1735 AS 信用等级,F4_1735
        FROM
        wind.TB_OBJECT_1735 T1  
        LEFT JOIN wind.TB_OBJECT_1090 T0 ON T0.F2_1090 = T1.F1_1735
        '''
bond_rating = pd.DataFrame(cu.execute(sql2).fetchall(), columns=['证券代码', '信用评级','公告日期']).sort_values(by='公告日期',ascending=False)
bond_rating.drop_duplicates('证券代码','first',inplace=True)
del bond_rating['公告日期']

bond_rating2 = bond_rating.append(rating)#将同业存单的评级信息合并进去
bond_rating2.drop_duplicates('证券代码','first',inplace=True)
portfolio_bond = pd.merge(holded1, bond_rating2, on='证券代码', how='left')

net_all = deepcopy(net_bond)
net_maxbond = pd.DataFrame(columns=['基金代码','报告期','证券代码','证券简称','持有比例','证券类型','信用评级'])
for i in net_all.index:
    code = net_all.loc[i,'基金代码']
    df1 = portfolio_bond[portfolio_bond.基金代码 == code]
    j = reportdate
    try:
        df2 = df1[df1.报告期==j].sort_values(by='基金代码', ascending=True).reset_index(drop=True)
        df3 = df2[(df2.证券类型 != 'Ａ股') & (df2.证券类型 != '资产支持证券') & (df2.证券类型 != '期货')].sort_values(by='持有比例',
                                                                                ascending=False).reset_index(drop=True)
        net_maxbond = net_maxbond.append(df3[['基金代码', '报告期', '证券代码', '证券简称', '持有比例', '证券类型', '信用评级']],
                                         ignore_index=True)
    except KeyError:
        pass
#将缺失的金融债的信用评级填充为AAA
net_maxbond.loc[(net_maxbond.证券类型=='金融债')&(net_maxbond.信用评级==0),'信用评级'] = 'AAA'

#打信用评级估计标签
bond = deepcopy(net_bond)
for i in bond.index:
    code =  bond.loc[i,'基金代码']
    index1 = bond[bond.基金代码 == code].index[0]
    df = net_maxbond[net_maxbond.基金代码==code][['信用评级','持有比例']]
    df = df.sort_values(by='持有比例', ascending=False).reset_index(drop=True)
    df = df[df.信用评级!=0]
    try:
        kind = pd.DataFrame(df['信用评级'].drop_duplicates(), columns=['信用评级'])
        for g in kind.index:
            type = kind.loc[g,'信用评级']
            df4 = df[df.信用评级==type]
            kind.loc[g,'持有比例']=df4.持有比例.sum()
        try:
            AAA = kind[kind.信用评级=='AAA'].iloc[-1,-1]
        except IndexError:
            AAA=0
        try:
            AA = kind[kind.信用评级 == 'AA+'].iloc[-1, -1]
        except IndexError:
            AA =0
        try:
            A1 = kind[kind.信用评级 == 'A-1'].iloc[-1, -1]
        except IndexError:
            A1 =0
        a = df.持有比例.sum()
        try:
            ratio = (AAA+AA+A1)/a
        except ZeroDivisionError:
            ratio =np.nan
        net_maxbond.loc[i,'估算信用评级比例'] = ratio
        if ratio >= 0.8:
            bond.loc[index1,'信用评级'] = '高-信用等级'
        elif 0.5<=ratio< 0.8:
            bond.loc[index1, '信用评级'] = '中-信用等级'
        elif ratio < 0.5:
            bond.loc[index1, '信用评级'] = '低-信用等级'
    except IndexError:
        pass
Credit_rating = bond
Performance = pd.merge(Performance,Credit_rating[['基金代码','信用评级']],on='基金代码',how='left')

#信用评级 标签入库
pdData = Performance[['基金代码','信用评级']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0001011000', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
#DBInsert(sql_tag_insert, rec)

#信用评级 标签衍生指标入库
pdData = net_maxbond[['基金代码', '估算信用评级比例']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], 'HBSARatio', str(value[1]), None, algid, batchno, thisfilename, todaydate))
#DBInsert(sql_index_insert, rec)

print('跑完信用评级估算')

#######################################################################################################################
# 杠杆率标签（3分钟左右）
#######################################################################################################################
net_all=deepcopy(net_bond)

sql_kind='''
select F16_1090 as 基金代码,F14_1104 as 截止时间,F5_1104 as 股票占比,F11_1104 as 现金占比,F13_1104 as 其他资产占比,F32_1104 as 债券市值占比,F45_1104 as 权证占比,F52_1104 as 基金占比,F55_1104 as 货币市场工具占比
from wind.TB_OBJECT_1090 left join wind.TB_OBJECT_1104
on F15_1104 = F2_1090
where
F14_1104 in %(date)s

'''%{'date':dates_str}
holded_kind= pd.DataFrame(cu.execute(sql_kind).fetchall(), columns=['基金代码','报告期','股票%','现金%','其他资产%','债券%','权证%','基金%','货币市场工具%']
                      ).sort_values(by='基金代码', ascending=True).reset_index(drop=True)
holded_kind = holded_kind[holded_kind.基金代码.isin(net_all['基金代码'])]
holded_kind = holded_kind.fillna(0)
holded_kind['杠杆率'] = holded_kind['股票%']+holded_kind['现金%']+holded_kind['其他资产%']+holded_kind['债券%']+holded_kind['权证%']+holded_kind['基金%']+holded_kind['货币市场工具%']
holded_kind = holded_kind[['基金代码','报告期','杠杆率']]
net_lev_mean = holded_kind.groupby('基金代码').mean()
net_lev = net_all

bond = deepcopy(net_bond)
bond = pd.merge(net_bond,data2[['基金代码','运作方式']],on = '基金代码',how = 'left')
for i in bond.index:
    code =  bond.loc[i,'基金代码']
    if bond.loc[i,'成立年限'] >1:
        lev = net_lev_mean.loc[code, '杠杆率']
        way = bond.loc[i, '运作方式']
        if way == '开放式':
            if lev >= 130:
                bond.loc[i, '杠杆率'] = '高-杠杆率'
            elif lev <= 110:
                bond.loc[i, '杠杆率'] = '低-杠杆率'
            else:
                bond.loc[i, '杠杆率'] = '中-杠杆率'
        if way in ['封闭式', '定期开放']:
            if lev >= 160:
                bond.loc[i, '杠杆率'] = '高-杠杆率'
            elif lev <= 120:
                bond.loc[i, '杠杆率'] = '低-杠杆率'
            else:
                bond.loc[i, '杠杆率'] = '中-杠杆率'

net_lev_mean['基金代码'] = net_lev_mean.index
net_lev_mean.columns = ['平均杠杆率','基金代码']
Leverrage_ratio = pd.merge(bond,net_lev_mean,on='基金代码',how='left')
Performance = pd.merge(Performance,Leverrage_ratio[['基金代码','平均杠杆率','杠杆率']],on='基金代码',how='left')

#杠杆率 标签入库
pdData = Performance[['基金代码','杠杆率']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0001012000', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
#DBInsert(sql_tag_insert, rec)
#杠杆率 标签衍生指标入库
pdData = Performance[['基金代码','平均杠杆率']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], 'AvgLeverageRatio', str(value[1]), None,algid, batchno, thisfilename, todaydate))
#DBInsert(sql_index_insert, rec)

print('跑完杠杆率')


#######################################################################################################################
# 单一持有人比例(1分钟以内）
#######################################################################################################################
bond = deepcopy(net_bond)
j = reportdate
year = j[0:4]
if j[4:6]=='03':
    semi = 1
elif j[4:6] =='06':
    semi = 2
elif j[4:6] =='09':
    semi = 3
elif j[4:6] =='12':
    semi = 4

net3 = bond[['基金代码']]
wind_str4 = tuple(bond['基金代码'].apply(lambda x:x+'.OF'))
day ='reportDateType='+str(semi)+';year='+str(year)
df = w.wss(wind_str4, "holder_single_totalholdingpct", day)
data = pd.DataFrame(df.Data, index=[str(year) + '年第'+str(semi)+'季度单一机构超过20%的持有人%']).T
data['基金代码'] = df.Codes
data['基金代码'] = data['基金代码'].apply(lambda x: str(x)[0:6])
net_20holding = data


num = days(end_date,j)/365
for i in bond.index:
	code =  bond.loc[i,'基金代码']
	tmp_info = net_20holding[net_20holding.基金代码 == code]
	if len(tmp_info) == 0:
		continue
	else:	
		index1 = tmp_info.index[0]
	instr_1 = net_20holding.loc[index1,str(year)+'年第'+str(semi)+'季度单一机构超过20%的持有人%']
	if instr_1 > 80:
		bond.loc[i, '单一机构持有人比例'] = '高-单一机构持有人比例'
	elif instr_1 <30 or (bond.loc[i,'成立年限'] > num and np.isnan(instr_1)):
		bond.loc[i, '单一机构持有人比例'] = '低-单一机构持有人比例'
	elif bond.loc[i,'成立年限'] <= num and np.isnan(instr_1):
		bond.loc[i, '单一机构持有人比例'] = np.nan
	else:
		bond.loc[i, '单一机构持有人比例'] = '中-单一机构持有人比例'

Single_hold_percent = bond
Performance = pd.merge(Performance,Single_hold_percent[['基金代码','单一机构持有人比例']],on='基金代码',how='left')

#单一机构持有人比例 标签入库
pdData = Performance[['基金代码','单一机构持有人比例']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0001009000', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

#单一持有人比例  标签衍生指标入库
#最近季报	reportdate
pdData = net_20holding [['基金代码', str(year) + '年第'+str(semi)+'季度单一机构超过20%的持有人%']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], 'InstQuaterYearShare', str(value[1]), str(reportdate), algid, batchno, thisfilename, todaydate))
DBInsert(sql_index_insert, rec)

print('跑完单一持有人比例')


#################################################################################################
# 3.1超额收益（5分钟以内）
#################################################################################################

#获取基金业绩基准行情数据
date_str = str(tuple(dates2))
sql2= '''
   SELECT
   S_INFO_WINDCODE,
     TRADE_DT 日期,
     S_DQ_CLOSE AS 复权收盘价
     FROM
       wind.chinaMutualfundbenchmarkeod 
       where 
       TRADE_DT in %(date)s
   '''%{'date':date_str}
market_BI = pd.DataFrame(cu.execute(sql2).fetchall(), columns=['指数代码','日期', '指数收盘价'])
market_BI_group = market_BI.groupby('指数代码')

def get_BI(code):
    """
    功能
    --------
    找到某只基金的业绩基准收盘价数据

    参数
    --------
    code:基金代码，字符格式，如'000003'

    返回值
    --------
    返回一个dataframe,index为日期，列名为[基金业绩基准代码+'收益率']

    参看
    --------
    无关联函数

    示例
    --------
    >>>get_BI('000003').head()
            000003BI.WI收益率
日期
20151030             NaN
20151106        0.018830
20151113       -0.009930
20151120        0.006293
20151127       -0.014389
     """
    code = str(code)+'BI.WI'
    try:
        temp_market = market_BI_group.get_group(code).sort_values(by='日期', ascending=True)
        temp_market[code + '收益率'] = temp_market['指数收盘价'].pct_change()
        temp_market.index= temp_market['日期']
        del temp_market['指数收盘价']
        del temp_market['日期']
        del temp_market['指数代码']
    except KeyError:
        temp_market = pd.DataFrame()
    return temp_market

def alpha(net,year_lag,market_type):
    """
    功能
    --------
    计算基金近几年的超额收益，使用同类基准和基金基准

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年
    market_type:市场组合类型
    market_type=0表示使用同类基准
    market_type=1表示使用同类平均，当计算货币型的时候，使用同类平均
    market_type=2时，使用基金基准

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果列和超额收益标签列

    参看
    --------
    get_BI():取某只基金对应的基金基准收益率数据
    get_market():取某个二级分类的基准组合收益率数据

    示例
    --------
    >>>net = pd.merge(net_act_plus,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
    >>>alpha(net,1,0).head()
    基金代码      基金简称       ...       1年超额收益（同类基准）  1年超额收益（同类基准）标签
0  000001      华夏成长       ...           0.018592  中-1年超额收益（同类基准）
1  000003  中海可转换债券A       ...          -0.102884             NaN
2  000005    嘉实增强信用       ...           0.068762  高-1年超额收益（同类基准）
3  000009    易方达天天A       ...           0.003132  中-1年超额收益（同类基准）
4  000010    易方达天天B       ...           0.004411  中-1年超额收益（同类基准）
     """
    end = get_friday(end_date)
    start = date_gen(years=year_lag, end=end_date)
    kind = net['二级分类'].drop_duplicates()
    for i in kind:
        if (market_type == 1)or(i=='货币型'):
            market = market_mean.ix[i]
            col = str(year_lag) + '年超额收益（同类基准）'
        elif market_type == 0:
            market = get_market(i)
            col = str(year_lag) + '年超额收益（同类基准）'
        net_kind = net[net.二级分类==i]['基金代码']
        alpha = []
        for  j in net_kind:
            if market_type == 2:
                market = get_BI(j)
                col = str(year_lag) + '年超额收益（基金基准）'
            dates = get_weekly_yeild(j).loc[start:end]
            dates = dates.drop(dates.index[0])
            if (net[net.基金代码==j].iloc[0,3]<year_lag) or market.empty:
                a=np.nan
            else:
                dates = dates.join(market)
                dates.columns = ['基金收益率', '市场组合收益率']
                df = dates
                df = df.dropna(axis=0, how='any')
                try:
                    X = np.array(df['市场组合收益率']).reshape(df['市场组合收益率'].shape[0], 1)
                    regr = LinearRegression()
                    regr.fit(X, df['基金收益率'])
                    a = regr.intercept_ * 52
                except ValueError:
                    a=np.nan
            alpha.append(a)
        net.loc[net.二级分类==i,col] = alpha
        alpha1 = pd.Series(alpha).dropna()
        alpha1 = alpha1[alpha1>0]
        try:
            high = np.percentile(alpha1, 80)
            low = np.percentile(alpha1, 20)
            net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签'] = '高-' + col
            net.loc[(net.二级分类 == i) & (0 < net[col]) & (net[col] < low), col +'标签' ] = '低-' + col
            net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col +'标签' ] = '中-' + col
        except IndexError:
            pass
    return net
net = pd.merge(net_act_plus,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [1,2,3]:
    net = alpha(net,i,0)
    net = alpha(net, i, 2)
Alpha  = net
del Alpha['基金简称']
del Alpha['二级分类']
del Alpha['成立年限']
Performance = pd.merge(Performance,Alpha,on = '基金代码',how = 'left')
Performance.to_excel(path+'/Performance.xlsx')

for i in range(3):
	#超额收益率 标签入库
	#基金基准
	pdData = Alpha[['基金代码','%d年超额收益（基金基准）标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200100%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#同类基准
	pdData = Alpha[['基金代码','%d年超额收益（同类基准）标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200100%d'%(i+4), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#超额收益率 标签衍生指标入库
	#基金基准
	pdData = Alpha[['基金代码','%d年超额收益（基金基准）'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'AlphaCategroyFund%dYear'%(i+1), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)
	
	#同类基准
	pdData = Alpha[['基金代码','%d年超额收益（同类基准）'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'AlphaCategroyBenchmark%dYear'%(i+1), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)


print('跑完超额收益')

#################################################################################################
# 3.2信息比率（运行大概2分钟）
#################################################################################################
def IR(net,year_lag):
    """
    功能
    --------
    计算基金近几年的信息比率

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果信息比率列和信息比率标签列

    参看
    --------
    get_market():取某个二级分类的基准组合收益率数据

    示例
    --------
    >>>net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
    >>>IR(net,1).head()
    基金代码      基金简称   二级分类       成立年限    1年信息比率  1年信息比率标签
0  000001      华夏成长  标准配置型  17.131507 -0.558956       NaN
1  000003  中海可转换债券A   可转债型   5.871233 -1.545706       NaN
2  000005    嘉实增强信用  普通债券型   5.904110  1.795728  高-1年信息比率
3  000011    华夏大盘精选    股票型  14.482192  0.783975  中-1年信息比率
4  000014      华夏聚利  普通债券型   5.873973 -1.432578       NaN
         """
    end = get_friday(end_date)
    start = date_gen(years=year_lag, end=end_date)
    kind = net['二级分类'].drop_duplicates()
    col = str(year_lag)+'年信息比率'
    for i in kind:
        market = get_market(i)
        net_kind = net[net.二级分类==i]['基金代码']
        alpha = []
        for  j in net_kind:
            dates = get_weekly_yeild(j).loc[start:end]
            dates = dates.drop(dates.index[0])
            if net[net.基金代码==j].iloc[0,3]<year_lag:
                IR=np.nan
            else:
                dates = dates.join(market)
                dates.columns = ['基金收益率', '市场组合收益率']
                dates['差额'] = dates['基金收益率'] - dates['市场组合收益率']
                tracking_error = dates['差额'].std()
                dates['基金净值'] = dates['基金收益率']+1
                dates['市场组合净值'] = dates['市场组合收益率']+1
                R_p = dates['基金净值'].product()/ dates.iloc[0,-1]
                R_m = dates['市场组合净值'].product() / dates.iloc[0,-1]
                IR = (R_p - R_m) / (tracking_error * np.sqrt(52))
            alpha.append(IR)
        net.loc[net.二级分类==i,col] = alpha
        alpha1 = pd.Series(alpha).dropna()
        alpha1 = alpha1[alpha1>0]
        try:
            high = np.percentile(alpha1, 80)
            low = np.percentile(alpha1, 20)
            net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签'] = '高-' + str(year_lag) + '年信息比率'
            net.loc[(net.二级分类 == i) & (0 < net[col]) & (net[col] < low), col +'标签' ] = '低-' + str(year_lag) + '年信息比率'
            net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col +'标签'] = '中-' + str(
                year_lag) + '年信息比率'
        except IndexError:
            pass
    return net
net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [1,2,3]:
    net = IR(net,i)

Information_ratio  = net
del Information_ratio['成立年限']
del Information_ratio['基金简称']
del Information_ratio['二级分类']
Performance = pd.merge(Performance,Information_ratio,on = '基金代码',how = 'left')


for i in range(3):
	#信息比率 标签入库
	pdData = Information_ratio[['基金代码','%d年信息比率标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200200%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)


	#信息比率 标签衍生指标入库
	pdData = Information_ratio[['基金代码','%d年信息比率标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'InformationRatio%dYear'%(i+1), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完信息比率')

#################################################################################################
# 3.3夏普比率（1分钟）
#################################################################################################
def Sharp(net,year_lag):
    """
    功能
    --------
    计算基金近几年的夏普比率

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果夏普比率列和夏普比率标签列

    参看
    --------
    get_market():取某个二级分类的基准组合收益率数据

    示例
    --------
    >>>net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
    >>>Sharp(net,1).head()
    基金代码      基金简称   二级分类    ...     1年信息比率标签    1年夏普比率  1年夏普比率标签
0  000001      华夏成长  标准配置型    ...          NaN -0.963856       NaN
1  000003  中海可转换债券A   可转债型    ...          NaN -0.911552       NaN
2  000005    嘉实增强信用  普通债券型    ...     高-1年信息比率  4.864770  中-1年夏普比率
3  000011    华夏大盘精选    股票型    ...     中-1年信息比率 -0.831787       NaN
4  000014      华夏聚利  普通债券型    ...          NaN -0.430936       NaN
         """
    end = get_friday(end_date)
    start = date_gen(years=year_lag, end=end_date)
    kind = net['二级分类'].drop_duplicates()
    col = str(year_lag)+'年夏普比率'
    r1 = r[(r.时间 > pd.to_datetime(start).date()) & (r.时间 < pd.to_datetime(end).date())]
    interest = r1['一年定存利率'].mean() / (100 * 52)
    for i in kind:
        net_kind = net[net.二级分类==i]['基金代码']
        alpha = []
        for  j in net_kind:
            dates = get_weekly_yeild(j).loc[start:end]
            dates = dates.drop(dates.index[0])
            if net[net.基金代码==j].iloc[0,3]<year_lag:
                a=np.nan
            else:
                dates.columns = ['基金收益率']
                stv = dates['基金收益率'].std() * np.sqrt(52)
                a = ((dates['基金收益率'].mean() - interest) / stv) * 52
            alpha.append(a)
        net.loc[net.二级分类==i,col] = alpha
        alpha1 = pd.Series(alpha).dropna()
        alpha1 = alpha1[alpha1>0]
        try:
            high = np.percentile(alpha1, 80)
            low = np.percentile(alpha1, 20)
            net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签'] = '高-' + str(year_lag) + '年夏普比率'
            net.loc[(net.二级分类 == i) & (0 < net[col]) & (net[col] < low), col +'标签'] = '低-' + str(year_lag) + '年夏普比率'
            net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col +'标签'] = '中-' + str(
                year_lag) + '年夏普比率'
        except IndexError:
            pass
    return net
net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [1,2,3]:
    net = Sharp(net,i)

Sharp_ratio  = net
del Sharp_ratio['成立年限']
del Sharp_ratio['基金简称']
del Sharp_ratio['二级分类']
Performance = pd.merge(Performance,Sharp_ratio,on = '基金代码',how = 'left')


for i in range(3):
	#夏普率 标签入库
	pdData = Sharp_ratio[['基金代码','%d年夏普比率标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200300%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#夏普率 标签衍生指标入库
	pdData = Sharp_ratio[['基金代码','%d年夏普比率标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'SharpeRatio%dYear'%(i+1), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完夏普比率')

#################################################################################################
# 3.4波动率（2分钟以内）
#################################################################################################
def Std(net,year_lag):
    """
    功能
    --------
    计算基金近几年的年化波动率

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果波动率列和波动率标签列

    参看
    --------
    无关联函数

    示例
    --------
    >>>net = pd.merge(net_act_plus,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
    >>>Std(net,1).head()
     基金代码      基金简称   二级分类   ...          1年波动率  1年波动率标签
0  000001      华夏成长  标准配置型   ...            0.197650  中-1年波动率
1  000003  中海可转换债券A   可转债型   ...           0.157967  高-1年波动率
2  000005    嘉实增强信用  普通债券型   ...           0.014891  中-1年波动率
3  000011    华夏大盘精选    股票型   ...             0.244267  中-1年波动率
4  000014      华夏聚利  普通债券型   ...            0.060850  高-1年波动率
         """
    end = get_friday(end_date)
    start = date_gen(years=year_lag, end=end_date)
    kind = net['二级分类'].drop_duplicates()
    col = str(year_lag)+'年波动率'
    for i in kind:
        net_kind = net[net.二级分类==i]['基金代码']
        alpha = []
        for  j in net_kind:
            dates = get_weekly_yeild(j).loc[start:end]
            dates = dates.drop(dates.index[0])
            if net[net.基金代码==j].iloc[0,3]<year_lag:
                stv=np.nan
            else:
                dates.columns = ['基金收益率']
                stv = dates['基金收益率'].std()*np.sqrt(52)
            alpha.append(stv)
        net.loc[net.二级分类==i,col] = alpha
        alpha1 = pd.Series(alpha).dropna()
        alpha1 = alpha1[alpha1>0]
        try:
            high = np.percentile(alpha1, 80)
            low = np.percentile(alpha1, 20)
            net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签'] = '高-' + str(year_lag) + '年波动率'
            net.loc[(net.二级分类 == i) & (0 <= net[col]) & (net[col] < low), col+'标签'] = '低-' + str(year_lag) + '年波动率'
            net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col+'标签' ] = '中-' + str(
                year_lag) + '年波动率'
        except IndexError:
            pass
    return net

net = pd.merge(net_act_plus,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [1,2,3]:
    net = Std(net,i)
Standard_deviation  = net
del Standard_deviation['成立年限']
del Standard_deviation['基金简称']
del Standard_deviation['二级分类']
Performance = pd.merge(Performance,Standard_deviation,on = '基金代码',how = 'left')

for i in range(3):
	#波动率 标签入库
	pdData = Standard_deviation[['基金代码','%d年波动率标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200400%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#波动率 标签衍生指标入库
	pdData = Standard_deviation[['基金代码','%d年波动率'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'StandardDeviation%dYear'%(i+1), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完标准差')

#################################################################################################
# 3.5下行波动率（1分钟）
#################################################################################################
def DownStd(net,year_lag):
    """
    功能
    --------
    计算基金近几年的下行波动率

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果下行波动率列和下行波动率标签列

    参看
    --------
    无关联函数

    示例
    --------
    >>>net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
    >>>DownStd(net,1).head()
    基金代码      基金简称   二级分类       成立年限   1年下行波动率  1年下行波动率标签
0  000001      华夏成长  标准配置型  17.131507  0.162969  中-1年下行波动率
1  000003  中海可转换债券A   可转债型   5.871233  0.130286  高-1年下行波动率
2  000005    嘉实增强信用  普通债券型   5.904110  0.004639  中-1年下行波动率
3  000011    华夏大盘精选    股票型  14.482192  0.194942  中-1年下行波动率
4  000014      华夏聚利  普通债券型   5.873973  0.045783  高-1年下行波动率
         """
    end = get_friday(end_date)
    start = date_gen(years=year_lag, end=end_date)
    kind = net['二级分类'].drop_duplicates()
    col = str(year_lag)+'年下行波动率'
    for i in kind:
        net_kind = net[net.二级分类==i]['基金代码']
        alpha = []
        for  j in net_kind:
            dates = get_weekly_yeild(j).loc[start:end]
            dates = dates.drop(dates.index[0])
            if net[net.基金代码==j].iloc[0,3]<year_lag:
                down_stv=np.nan
            else:
                dates.columns = ['基金收益率']
                net_np = np.array(dates['基金收益率'].dropna(axis=0, how='any'))
                down_net = np.array(np.delete(net_np, np.where(net_np >= 0)[0]))
                down_stv = pow(np.power(down_net, 2).sum() / (len(dates)-1), 1 / 2) * np.sqrt(52)
            alpha.append(down_stv)
        net.loc[net.二级分类==i,col] = alpha
        alpha1 = pd.Series(alpha).dropna()
        alpha1 = alpha1[alpha1>0]
        try:
            high = np.percentile(alpha1, 80)
            low = np.percentile(alpha1, 20)
            net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签' ] = '高-' + str(year_lag) + '年下行波动率'
            net.loc[(net.二级分类 == i) & (0 <= net[col]) & (net[col] < low),col +'标签' ] = '低-' + str(year_lag) + '年下行波动率'
            net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col +'标签' ] = '中-' + str(
                year_lag) + '年下行波动率'
        except IndexError:
            pass
    return net

net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [1,2,3]:
    net = DownStd(net,i)

Des_standard_deviation  = net
del Des_standard_deviation['成立年限']
del Des_standard_deviation['基金简称']
del Des_standard_deviation['二级分类']
Performance = pd.merge(Performance,Des_standard_deviation,on = '基金代码',how = 'left')

for i in range(3):
	#下行波动率 标签入库
	pdData = Performance[['基金代码','%d年下行波动率标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200500%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#下行波动率 标签衍生指标入库
	pdData = Performance[['基金代码','%d年下行波动率'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'DesStandardDeviation%dYear'%(i+1), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完下行标准差')

#################################################################################################
# 3.6最大回撤(大概5分钟）
#################################################################################################

def max_back(net1):
    """
    功能
    --------
    计算基金近几年的最大回撤

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果最大回撤列和最大回撤标签列

    参看
    --------
    MaxDrawdown(),计算某基金净值序列的最大回撤数值

    示例
    --------
    >>>net1 = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left')
    >>>max_back(net1).head()
  基金代码      基金简称   二级分类    ...      近1年最大回撤   近2年最大回撤   近3年最大回撤
0  000001      华夏成长  标准配置型    ...     0.227228  0.246771  0.246771
1  000003  中海可转换债券A   可转债型    ...     0.200984  0.249993  0.343131
2  000005    嘉实增强信用  普通债券型    ...     0.007681  0.007681  0.025195
3  000011    华夏大盘精选    股票型    ...     0.269229  0.278336  0.278336
4  000014      华夏聚利  普通债券型    ...     0.059778  0.061381  0.075567
         """
    for i in net1.基金代码:
        all = netvalue(i).reset_index(drop=True)
        month_lag=6
        year_lag=0
        col1 = "近半年最大回撤"
        start = date_gen(years=year_lag, months=month_lag, end=end_date)
        index1 = net1[net1.基金代码 == i].index.tolist()[0]
        if opendate(i) > start:
            back = np.nan
        else:
            all2 = all[(all.日期 >= start) & (all.日期 <= end_date)]
            try:
                back = MaxDrawdown(all2)
            except ValueError as e:
                back = np.nan
        net1.loc[index1, col1] = back
        for year_lag in [1,2,3]:
            month_lag = 0
            col2 = "近" + str(year_lag) + "年最大回撤"
            start = date_gen(years=year_lag, months=month_lag, end=end_date)
            index1 = net1[net1.基金代码 == i].index.tolist()[0]
            if opendate(i) > start:
                back = np.nan
            else:
                all2 = all[(all.日期 >= start) & (all.日期 <= end_date)]
                try:
                    back = MaxDrawdown(all2)
                except ValueError as e:
                    back = np.nan
            net1.loc[index1, col2] = back
    return net1
net1 = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left')
net1 = max_back(net1)

maxback = pd.DataFrame(columns=['基金代码', '基金简称', '二级分类', '成立年限', '近半年最大回撤','近1年最大回撤',
                                '近2年最大回撤','近3年最大回撤','半年最大回撤标签','1年最大回撤标签',
                                '2年最大回撤标签','3年最大回撤标签'])
kind = net1['二级分类'].drop_duplicates()
for type in kind:
    all = net1[net1.二级分类 == type]
    for i in [0.5,1,2,3]:
        if i ==0.5:
            col = '近半年最大回撤'
            high = np.percentile(all[[col]].dropna(),80)
            low = np.percentile(all[[col]].dropna(),20)
            all.loc[all[col] >= high, '半年最大回撤标签'] = '高-半年最大回撤'
            all.loc[all[col] <= low, '半年最大回撤标签'] = '低-半年最大回撤'
            all.loc[(all[col] >= low) & (all[col] <= high), '半年最大回撤标签'] = '中-半年最大回撤'
        if i in [1,2,3]:
            col2 = "近" + str(i) + "年最大回撤"
            high = np.percentile(all[[col2]].dropna(), 80)
            low = np.percentile(all[[col2]].dropna(), 20)
            all.loc[all[col2] >= high, str(i) + "年最大回撤标签"] = '高-'+str(i)+'年最大回撤'
            all.loc[all[col2] <= low, str(i) + "年最大回撤标签"] = '低-' + str(i) + '年最大回撤'
            all.loc[(all[col2] >= low)&(all[col2] <= high), str(i) + "年最大回撤标签"] = '中-' + str(i) + '年最大回撤'
    maxback = maxback.append(all,ignore_index=True)

maxback = maxback[['基金代码', '近半年最大回撤','半年最大回撤标签','近1年最大回撤','1年最大回撤标签',
                                '近2年最大回撤','2年最大回撤标签','近3年最大回撤',
                                '3年最大回撤标签']]
Max_drawdown =maxback.sort_values(by = '基金代码',ascending=True).reset_index(drop=True)


Performance = pd.merge(Performance,Max_drawdown,on = '基金代码',how = 'left')

for i in range(4):
	#最大回撤 标签入库
	if i == 0:
		pdData = Performance[['基金代码','半年最大回撤标签']]
	else:
		pdData = Performance[['基金代码','%d年最大回撤标签'%(i)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200600%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#最大回撤 标签衍生指标入库
	if i == 0:
		pdData = Performance[['基金代码','近半年最大回撤']]
	else:
		pdData = Performance[['基金代码','近%d年最大回撤'%(i)]]
	rec.clear()
	for value in pdData.values:
		if i == 0:
			rec.append((value[0], 'MaxDrawdown6Month', str(value[1]), None,algid, batchno, thisfilename, todaydate))
		else:
			rec.append((value[0], 'MaxDrawdown%dYear'%(i), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完最大回撤')

#################################################################################################
# 被动型-根据对应跟踪指数取指数的行情数据
# 这部分为后面取不同基金跟踪指数的数据定义一些函数，便于后面计算
#################################################################################################
data7 = w.wsd(tuple(net_pass['基金代码'].apply(lambda x:x+'.OF')), "fund_trackindexcode", end_date, end_date)
follow = pd.DataFrame(data7.Data, index=['跟踪指数代码']).T
follow['基金代码'] = data7.Codes
net_pass['跟踪指数代码'] = follow['跟踪指数代码']

net_pass.loc[net_pass.二级分类.isin(['股票型','激进配置型','标准配置型','灵活配置型','沪港深股票型','沪港深配置型','环球股票']),'类型'] ='偏股型'
net_pass.loc[net_pass.二级分类.isin(['保守配置型','纯债型','激进债券型','普通债券型','可转债型',' 纯债型']),'类型'] ='偏债型'

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
    if i ==None:
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
    return  market

#################################################################################################
# 3.7 跟踪稳定性(12分钟）
#################################################################################################
"""
功能
--------
计算被动型基金近几年的跟踪误差

参数
--------
net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
year_lag:近几年

返回值
--------
返回一个dataframe,是在net上添加计算结果跟踪误差列和跟踪误差标签列

参看
--------
get_track_index():取某只基金跟踪指数的收盘价数据

示例
--------
>>>net = pd.merge(net_pass,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
>>>Tracking_error_fun(net,1).head()
基金代码            基金简称  二级分类    ...          成立年限   1年跟踪稳定性  1年跟踪稳定性标签
0  000049        中银标普全球精选  环球股票    ...      5.873973  0.065392  低-1年跟踪稳定性
1  000059     国联安中证医药100A   股票型    ...      5.449315  0.025992  中-1年跟踪稳定性
2  000368  汇添富沪深300安中动态策略   股票型    ...      5.238356  0.013520  中-1年跟踪稳定性
3  000369     广发全球医疗保健人民币  环球股票    ...      5.145205  0.045259  低-1年跟踪稳定性
4  000596       前海开源中证军工A   股票型    ...      4.684932  0.029257  中-1年跟踪稳定性
"""
def Tracking_error_fun(net,year_lag):
	end = get_friday(end_date)
	start = date_gen(years=year_lag, end=end_date)
	kind = net['类型'].drop_duplicates()
	col = str(year_lag) + '年跟踪稳定性'
	for i in kind:
		net_kind = net[net.类型 == i]['基金代码']
		alpha = []
		for  j in net_kind:
			market = get_track_index(net[net.基金代码==j].iloc[0,3])
			dates = get_weekly_yeild(j).loc[start:end]
			if net[net.基金代码==j].iloc[0,5]<year_lag:
				tracking_error=np.nan
			else:
				dates = dates.join(market)
				dates.columns = ['基金收益率', '市场组合净值']
				tmp = dates[['市场组合净值']]
				tmp.to_excel(path+'/市场组合净值.xlsx')
				dates['市场组合收益率'] = dates['市场组合净值'].pct_change()
				dates['差额'] = dates['基金收益率'] - dates['市场组合收益率']
				dates = dates.dropna(axis=0, how='any')
				tracking_error = dates['差额'].std() * np.sqrt(52)
			alpha.append(tracking_error)
		net.loc[net.类型==i,col] = alpha
		alpha1 = pd.Series(alpha).dropna()
		try:
			high = np.percentile(alpha1, 80)
			low = np.percentile(alpha1, 20)
			net.loc[(net.类型 == i) & (net[col] > high), col +'标签'] = '低-' + col
			net.loc[(net.类型 == i) & (0 < net[col]) & (net[col] < low), col +'标签'] = '高-' + col
			net.loc[(net.类型 == i) & (low <= net[col]) & (net[col] <= high), col +'标签'] = '中-' + col
		except IndexError:
			pass
	return net

net = pd.merge(net_pass,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [1,2,3]:
    net = Tracking_error_fun(net,i)

Tracking_error  = net
del Tracking_error['基金简称']
del Tracking_error['成立年限']
del Tracking_error['二级分类']

Performance = pd.merge(Performance,Tracking_error,on = '基金代码',how = 'left')

for i in range(3):
	#跟踪稳定性 标签入库
	pdData = Performance[['基金代码','%d年跟踪稳定性标签'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200700%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#跟踪稳定性 标签衍生指标入库
	pdData = Performance[['基金代码','%d年跟踪稳定性'%(i+1)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], 'TrackingError%dYear'%(i+1), str(value[1]), None, algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完跟踪误差')

#################################################################################################
# 3.8相对最大回撤（7分钟以内）
#################################################################################################

def Relative_Maxback(net,year_lag):
    """
    功能
    --------
    计算基金近几年的相对最大回撤

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果相对最大回撤列和相对最大回撤标签列

    参看
    --------
    get_track_index():取某只基金跟踪指数的收盘价数据

    示例
    --------
    >>>net = pd.merge(net_pass,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
    >>>Relative_Maxback(net,1).head()
     基金代码            基金简称  二级分类     ...           成立年限 半年指数型最大回撤  半年指数型最大回撤标签
0  000049        中银标普全球精选  环球股票     ...       5.873973 -0.042586  高-半年指数型最大回撤
1  000059     国联安中证医药100A   股票型     ...       5.449315 -0.015722  中-半年指数型最大回撤
2  000368  汇添富沪深300安中动态策略   股票型     ...       5.238356 -0.005359  中-半年指数型最大回撤
3  000369     广发全球医疗保健人民币  环球股票     ...       5.145205 -0.054238  高-半年指数型最大回撤
4  000596       前海开源中证军工A   股票型     ...       4.684932 -0.017783  中-半年指数型最大回撤

         """
    if year_lag == 0.5:
        start = date_gen(months=6,end = end_date)
        col = '半年指数型最大回撤'
    else:
        start = date_gen(years=year_lag, end=end_date)
        col = str(year_lag) + '年指数型最大回撤'
    kind = net['类型'].drop_duplicates()
    for i in kind:
        net_kind = net[net.类型== i]['基金代码']
        alpha = []
        for  j in net_kind:
            if opendate(j) > start:
                maxback = np.nan
            else:
                market = get_track_index(net[net.基金代码 == j].iloc[0, 3])
                all = netvalue(j).reset_index(drop=True)
                all.index = all['日期']
                all = all.loc[start:end_date]
                del all['基金代码']
                del all['日期']
                all = all.join(market)
                all = all.dropna(axis=0, how='any')
                df = all.values
                df3 = []
                for k in range(1, len(df)):
                    g = np.argmin(df[k][0] / df[:k, 0] - df[k][1] / df[:k, 1])
                    df3.append(df[k][0] / df[g, 0] - df[k][1] / df[g, 1])
                maxback = min(df3)
            alpha.append(maxback)
        net.loc[net.类型 == i, col] = alpha
        alpha1 = pd.Series(alpha).dropna()
        try:
            high = np.percentile(alpha1, 80)
            low = np.percentile(alpha1, 20)
            net.loc[(net.类型 == i) & (net[col] > high), col +'标签'] = '低-' + col
            net.loc[(net.类型 == i) & (net[col] < low), col +'标签'] = '高-' + col
            net.loc[(net.类型 == i) & (low <= net[col]) & (net[col] <= high), col +'标签'] = '中-' + col
        except IndexError:
            pass
    return net

net = pd.merge(net_pass,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
for i in [0.5,1,2,3]:
    net = Relative_Maxback(net,i)

Max_drawdown_indexfun  = net
del Max_drawdown_indexfun['基金简称']
del Max_drawdown_indexfun['成立年限']
del Max_drawdown_indexfun['二级分类']
del Max_drawdown_indexfun['跟踪指数代码']
del Max_drawdown_indexfun['类型']
Performance = pd.merge(Performance,Max_drawdown_indexfun,on = '基金代码',how = 'left')

for i in range(4):
	#相对最大回撤 标签入库
	if i == 0:
		pdData = Performance[['基金代码','半年指数型最大回撤标签']]
	else:
		pdData = Performance[['基金代码','%d年指数型最大回撤标签'%(i)]]
	rec.clear()
	for value in pdData.values:
		rec.append((value[0], '000200800%d'%(i+1), str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
	DBInsert(sql_tag_insert, rec)

	#相对最大回撤 标签衍生指标入库
	if i == 0:
		pdData = Performance[['基金代码','半年指数型最大回撤']]
	else:
		pdData = Performance[['基金代码','%d年指数型最大回撤'%(i)]]
	rec.clear()
	for value in pdData.values:
		if i == 0:
			rec.append((value[0], 'MaxDrawdownIndexFund6Month', str(value[1]), None,algid, batchno, thisfilename, todaydate))
		else:
			rec.append((value[0], 'MaxDrawdownIndexFund%dYear'%(i), str(value[1]), None,algid, batchno, thisfilename, todaydate))
	DBInsert(sql_index_insert, rec)

print('跑完相对最大回撤')

##########################################################################################################
# 运营标签

#################################################################################################
# 4.1规模（2分钟）
#################################################################################################
sql_get_scale = '''
select 基金代码,全称,简称,成立日,F3_1101/100000000,F13_1101
from(
SELECT
          F16_1090 as 基金代码,
          F1_1099 as 识别ID,
          OB_OBJECT_NAME_1099 as 全称,
          OB_OBJECT_NAME_1090 as 简称,
          F22_1099 as 成立日
        FROM
        wind.TB_OBJECT_1099
        LEFT OUTER JOIN wind.TB_OBJECT_1090 ON F2_1090 = F1_1099
        )
left join wind.TB_OBJECT_1101
on F14_1101=识别ID
where F13_1101 >= '%(date)s'
'''%{'date':reportdate}
data1 = pd.DataFrame(cu.execute(sql_get_scale).fetchall(), columns=['基金代码','基金全称','基金简称','成立日','资产净值(亿元)','净值截止日期']).\
    sort_values(by='净值截止日期',ascending=True).reset_index(drop=True)
data1 = data1.dropna(axis=0,how = 'any')
data1.drop_duplicates('基金代码',keep='last',inplace=True)
data1 = data1.sort_values(by='基金代码',ascending=True).reset_index(drop=True)
hold_sum = data1.groupby('基金全称').sum()
hold_sum['基金全称'] = hold_sum.index
del data1['资产净值(亿元)']
data1 = pd.merge(data1,hold_sum,on='基金全称',how = 'left')

data0 = pd.merge(data2[['基金代码','一级分类','二级分类']],data1,on='基金代码',how = 'left')
asset_mean = data0.groupby('二级分类').mean()
for i in data0['二级分类'].drop_duplicates():
	avg = asset_mean.loc[i,'资产净值(亿元)']
	data0.loc[data0.二级分类==i]
	net_kind = data0[data0.二级分类==i]
	for j in net_kind.index:
		data0.loc[(data0.index ==j)&(data0['资产净值(亿元)']>=100),'基金规模'] ='巨大规模'
		data0.loc[(data0.index == j) & (data0['资产净值(亿元)'] > avg*1.2)&(data0['资产净值(亿元)']<100), '基金规模'] = '同类平均以上规模'
		data0.loc[(data0.index == j) & (data0['资产净值(亿元)'] <= avg * 1.2) & (data0['资产净值(亿元)'] >= avg*0.8), '基金规模'] = '同类平均规模'
		data0.loc[(data0.index == j) & (data0['资产净值(亿元)'] < avg * 0.8) & (data0['资产净值(亿元)'] > 1), '基金规模'] = '同类平均以下规模'
		data0.loc[(data0.index == j) & (data0['资产净值(亿元)'] < avg * 0.8) & (data0['资产净值(亿元)'] <= 1), '基金规模'] = '同类微小规模'
		data0.loc[data0.index == j, '同类平均规模'] = avg
Fund_size = data0[['基金代码','基金简称','一级分类','二级分类','净值截止日期','资产净值(亿元)','基金规模', '同类平均规模']]
Performance = pd.merge(Performance,Fund_size[['基金代码','净值截止日期','资产净值(亿元)','基金规模']],on='基金代码',how='left')

#基金规模 标签入库
pdData = Performance[['基金代码','基金规模']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0003001000', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

#基金规模 标签衍生指标入库
pdData = Performance[['基金代码','资产净值(亿元)', '净值截止日期']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], 'FundScale', str(value[1]), str(value[2]),algid, batchno, thisfilename, todaydate))
DBInsert(sql_index_insert, rec)

print('跑完规模')

#################################################################################################
# 4.2份额（1分钟）
#################################################################################################
sql_get_share = '''
select F16_1090,F9_1495,F10_1495,F2_1495,F3_1495
from  wind.TB_OBJECT_1495
LEFT OUTER JOIN wind.TB_OBJECT_1090 ON F2_1090 = F1_1495
where 
F3_1495 = '%(date)s'
'''%{'date':reportdate}
data3 = pd.DataFrame(cu.execute(sql_get_share).fetchall(), columns=['基金代码','期初份额','期末份额','开始日期','截止日期']).\
    sort_values(by='基金代码',ascending=True).reset_index(drop=True)
data3['份额变化率'] = (data3['期末份额']-data3['期初份额'])/data3['期初份额']
data0 = pd.merge(data2[['基金代码','二级分类']],data3,on='基金代码',how = 'left')
for i in data0['二级分类'].drop_duplicates():
    net3 = data0[data0.二级分类==i]
    net0_pass = net3[net3.份额变化率>=0]
    try :
        high1 = np.percentile(net0_pass.份额变化率, 80)
        low1 = np.percentile(net0_pass.份额变化率, 20)
        data0.loc[(data0.二级分类 == i) & (data0.份额变化率 > high1), '份额季度变化'] = '同类份额大量增加'
        data0.loc[(data0.二级分类 == i) & (data0.份额变化率 > low1) & (data0.份额变化率 < high1), '份额季度变化'] = '同类份额小量增加'
        data0.loc[(data0.二级分类 == i) & (data0.份额变化率 <= low1) & (data0.份额变化率 >= 0), '份额季度变化'] = '同类份额稳定'
    except IndexError:
        pass
    try:
        net0_nev = net3[net3.份额变化率 < 0]
        high2 = np.percentile(net0_nev.份额变化率, 80)
        low2 = np.percentile(net0_nev.份额变化率, 20)
        data0.loc[(data0.二级分类 == i) & (data0.份额变化率 > high2), '份额季度变化'] = '同类份额稳定'
        data0.loc[(data0.二级分类 == i) & (data0.份额变化率 > low2) & (data0.份额变化率 < high2), '份额季度变化'] = '同类份额小量减少'
        data0.loc[(data0.二级分类 == i) & (data0.份额变化率 <= low2), '份额季度变化'] = '同类份额大量减少'
    except IndexError:
        pass
Quarterly_shares_change = data0
Performance  = pd.merge(Performance ,Quarterly_shares_change[['基金代码','份额变化率','份额季度变化']],on = '基金代码',how = 'left')

#基金规模 标签入库
pdData = Performance[['基金代码','份额季度变化']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0003002001', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

#基金规模 标签衍生指标入库
pdData = Performance[['基金代码','份额变化率']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], 'QuarterlySharesChange', str(value[1]), None, algid, batchno, thisfilename, todaydate))
DBInsert(sql_index_insert, rec)

print('跑完份额')

#######################################################################################################################
# 综合标签(1分钟以内）
#################################################################################################################
#5.1机构综合评级-晨星


star_5 = star[star.iloc[:,-1]==11111]
star_5 = star_5.fillna(0)
for i in range(len(star_5)):
    index = star_5.index[i]
    l2 = star_5.iloc[i, -24:-1].mean()
    l3 = star_5.iloc[i, -36:-1].mean()
    l4 = star_5.iloc[i, -48:-1].mean()
    l5 = star_5.iloc[i, -60:-1].mean()
    if l2 ==11111:
        star_5.loc[index ,'连续两年晨星5星（3年）'] = '连续两年晨星5星（3年）'
    if l3 == 11111:
        star_5.loc[index , '连续三年晨星5星（3年）'] = '连续三年晨星5星（3年）'
    if l4 == 11111:
        star_5.loc[index , '连续四年晨星5星（3年）'] = '连续四年晨星5星（3年）'
    if l5 == 11111:
        star_5.loc[index, '连续五年晨星5星（3年）'] = '连续五年晨星5星（3年）'
if '连续五年晨星5星（3年）'in star_5.columns:
    pass
else:
    star_5['连续五年晨星5星（3年）'] = np.nan
col = star.columns[-1]
star.loc[star[col]==1,'晨星3年评级'] = '晨星3年评级1星'
star.loc[star[col]==11,'晨星3年评级'] = '晨星3年评级2星'
star.loc[star[col]==111,'晨星3年评级'] = '晨星3年评级3星'
star.loc[star[col]==1111,'晨星3年评级'] = '晨星3年评级4星'
star.loc[star[col]==11111,'晨星3年评级'] = '晨星3年评级5星'
data_star = net0[net0.成立年限>=3]
data_star = pd.merge(data_star[['基金代码']],star_5[['基金代码','连续两年晨星5星（3年）',
                                                 '连续三年晨星5星（3年）','连续四年晨星5星（3年）','连续五年晨星5星（3年）']],on= '基金代码',how='left')
Agency_rating_ms = pd.merge(data_star,star[['基金代码','晨星3年评级']],on = '基金代码',how = 'left')

Performance = pd.merge(Performance,Agency_rating_ms,on='基金代码',how = 'left')

#评级 标签入库 
pdData = Agency_rating_ms[['基金代码','连续两年晨星5星（3年）']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0004001004', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))

DBInsert(sql_tag_insert, rec)

pdData = Agency_rating_ms[['基金代码','连续三年晨星5星（3年）']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0004001005', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

pdData = Agency_rating_ms[['基金代码','连续四年晨星5星（3年）']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0004001006', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

pdData = Agency_rating_ms[['基金代码','连续五年晨星5星（3年）']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0004001007', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

pdData = Agency_rating_ms[['基金代码','晨星3年评级']]
rec.clear()
for value in pdData.values:
	rec.append((value[0], '0004001001', str(value[1]), algid, batchno,'Y', thisfilename, todaydate))
DBInsert(sql_tag_insert, rec)

####
fund_db.close()

##################################################################################################
# 结果输出
##################################################################################################
Performance.to_excel(path+'/基金产品标签-季度更新'+str(end_date)+'.xlsx')
print('跑完晨星评级')
print((time.time()-start_time)/60)