#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/3 14:28
# @Author  : GaoJian
# @File    : relative_maxback.py

import pandas as pd
import os
import numpy as np
import cx_Oracle
import time
from datetime import datetime, timedelta
import datetime
import logging

from sql import sql_oracle

SQL_INDEX_INSERT = "INSERT INTO fund_index_info( fundId, indexCode, indexValue, reportdate, algid, batchno,  createUser, createDate ) VALUES(:fundId, :indexCode, :indexValue,:reportdate, :algid, :batchno, :createuser, :createDate)"

#
log_dir = 'log'
if os.path.exists(log_dir) and os.path.isdir(log_dir):
    pass
else:
    os.makedirs(log_dir)
log_file_name = 'season_label_0630_alpha_0.log'
LOG_FILE_PATH = os.path.join('log', log_file_name)
logging.basicConfig(
                    level = logging.INFO,
                    format='%(asctime)s-%(levelname)s-%(message)s',
                    datefmt='%y-%m-%d %H:%M',
                    filename=LOG_FILE_PATH,
                    filemode='a'
                    )
fh = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

my_logger = logging.getLogger()
my_logger.addHandler(fh)
my_logger.addHandler(ch)

# 连接数据库
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
start_time = time.time()
cu = fund_db.cursor()

#投研平台库
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


def relative_maxback(code, start_date, end_date, tag=None, type=None):
    """

    :param code: 基金代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param tag: 标的
    :param type: 类型 基金为fund fof 为 fof

    :return: 基金一段日期内的相对最大回撤
    """

    # 按照查找某指数基金净值
    market = find_tag_value()

    # 按照日期查找基金净值
    all = find_net_value(code, start_date, end_date, type)

    # 计算相对最大回撤
    all.index = all['日期']
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

    return maxback



class DBSession(object):
    def __init__(self, sql_string='', batch_size=500):
        self.cu_wind = sql_oracle.cu
        self.cu_pra = sql_oracle.cu_pra_sel
        self.fund_db_pra = sql_oracle.fund_db_pra
        self.rec_list = []
        self.sql_string = sql_string
        self.batch_size = 500

    def __del__(self):
        self.finish()

    def finish(self):
        if self.rec_list:
            self.db_insert(self.sql_string, self.rec_list)
            self.rec_list = []
        return

    def add_info(self, rec):
        self.rec_list.append(rec)
        if len(self.rec_list) > self.batch_size:
            self.db_insert(self.sql_string, self.rec_list)
            self.rec_list = []
        return

    def db_insert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            # print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            # 其他错误处理
            raise (e)

    def DBInsert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            # print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            # 其他错误处理
            raise (e)


def main(start_date, end_date, type, j):
    sql1 = '''select distinct(fundid) from fund_index_info'''
    all_codes = cu_pra.execute(sql1).fetchall()

    db_insert_session = DBSession(SQL_INDEX_INSERT)
    todaydate = datetime.datetime.now().strftime('%Y%m%d')

    for i in all_codes:
        a = 0
        try:
            rst_value = relative_maxback(i[0], start_date, end_date, type=type)
        except Exception as e:

            rst_value = None

        # print(rst_value)


        rec = (i[0], 'MaxDrawdownIndexFund%dYear'%(j), str(rst_value), None, '0000000003',
               "20190331", "relative_maxback.py", todaydate)

        print(rec)

        db_insert_session.add_info(rec)
    db_insert_session.finish()



if __name__ == '__main__':
    # 基金例子

    # start_date, end_date = '19980101', '20071231'
    # start_date, end_date = '20080101', '20151231'
    # start_date, end_date = '20181001', '20190331'
    # start_date, end_date = '20180401', '20190331'
    type = "fund"

    # # ofo例子
    # code = "2002151FOFIF3"
    # start_date = 20190101
    # end_date = 20190120
    # type = "fof"
    for i in range(1, 4):
        if i == 1:
            start_date, end_date = '20180101', '20190331'
        elif i == 2:
            start_date, end_date = '20170101', '20190331'
        elif i == 3:
            start_date, end_date = '20160101', '20190331'

        # print(start_date, end_date)
        else:
            break

        main(start_date, end_date, type, i)
