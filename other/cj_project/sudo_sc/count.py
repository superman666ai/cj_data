# -*- encoding:utf-8 -*-
# coding:utf-8

# 统计排名
import os

# 为避免连接Oracle乱码问题
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'

import cx_Oracle
import pandas as pd
import numpy as np
import time
from datetime import timedelta
from dateutil.parser import parse
import xlrd
import datetime
import math
from datetime import timedelta
from dateutil.relativedelta import relativedelta

[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)

# 投研平台库
[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
# [userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pif', 'pif', '172.16.125.151', 'pif']
try:
    fund_dbpra = cx_Oracle.connect(user=userNamepif, password=passwordpif, dsn=hostIPpif + '/' + dbNamepif)
    cu_pra = fund_dbpra.cursor()
except cx_Oracle.DatabaseError as e:
    print('数据库链接失败')

path = os.path.dirname(os.path.realpath(__file__)) + '/'

rptdate = '20190331'


def sum_manager_percent(date=None):
    """

    :param date: 季报日期
    :return: 基金经理团队总人数的百分位排名
    """

    # 取出基金公司基金经理总人数标签
    ##无需设置参数，输出结果分别为基金公司、基金公司成立日、基金公司基金经理总人数
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272)e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018
    
            '''

    cu = fund_db.cursor()
    sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])

    # 计算百分位排名
    sum_manager.sort_values("sum_manager", axis=0, ascending=False, inplace=True)
    print(sum_manager)
    return sum_manager


def mean_manager_date_end_percent(date=None):
    """

    :param date: 季报日期
    :return: 基金经理平均管理年限的百分位排名
    """

    # 不输入date 默认为当前日期
    if not date:
        today = datetime.datetime.today().strftime('%Y%m%d')
    else:
        today = str(date)

    # 取数时无需设置参数
    # 筛选出基金公司名、公司成立日、基金经理名字、该基金经理在此基金公司管理第一支基金的开始时间
    sql = '''
            SELECT
            b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272,MIN(c.F3_1272)
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018
            JOIN
            (SELECT
            F1_1272,F2_1272,F3_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL)c
            ON a.F1_1099 = c.F1_1272
            GROUP BY b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272

            '''

    cu = fund_db.cursor()
    manage_date_start = pd.DataFrame(cu.execute(sql).fetchall(),
                                     columns=['company', 'start_up_date', 'manager', 'start_date'])

    # 取数时无需设置参数
    # 筛选出基金公司名、公司成立日、基金经理名字、该基金经理目前在此基金公司管理最后一支基金的截止时间（none表示截止目前基金仍在存续）
    sql = '''
            SELECT
            b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272,MAX(c.F4_1272)
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018
            JOIN
            (SELECT
            F1_1272,F2_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL)c
            ON a.F1_1099 = c.F1_1272
            GROUP BY b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272

            '''

    cu = fund_db.cursor()
    manage_date_end = pd.DataFrame(cu.execute(sql).fetchall(),
                                   columns=['company', 'start_up_date', 'manager', 'end_date'])

    now = []
    ###today为更新标签日当日日期
    # today = '20190514'
    for i in manage_date_end['end_date']:
        if i == None:
            i = today
        now.append(i)
    manage_date_end['end_date'] = pd.DataFrame(now)

    manage_date = pd.merge(manage_date_start, manage_date_end, on=['company', 'start_up_date', 'manager'])

    # 转化为时间格式并相减计算中间天数（即基金经理平均管理时长）
    start = pd.to_datetime(manage_date['start_date'])
    end = pd.to_datetime(manage_date['end_date'])
    distance = pd.DataFrame(end - start)
    distance.columns = ['distance']
    day = []
    for i in distance['distance']:
        i = i.days
        i = round(i / 365, 2)
        day.append(i)
    manage_date['distance'] = day

    #################################################1、基金公司基金经理平均管理年限标签
    # 计算每个公司的基金经理平均管理年限（不管该基金经理是否现就职于该公司）
    mean_manage_years = manage_date.groupby(by=['company', 'start_up_date'])['distance'].mean()
    mean_manage_years = pd.DataFrame(mean_manage_years)
    mean_manage_years.columns = ['mean_manage_years']
    # 计算百分位排名
    mean_manage_years.sort_values("mean_manage_years", axis=0, ascending=False, inplace=True)
    print(mean_manage_years)
    return mean_manage_years


def off_managers_per(lag=3, date=None):
    """

    :param lag: 近几年 例：3/近三年
    :param date: 季报日期
    :return:
    """
    # 不输入date 默认为当前日期
    if not date:
        date = datetime.datetime.today().strftime('%Y%m%d')
    else:
        date = str(date)

    print(date)

    #
    now = pd.to_datetime(date).date()

    # 转变为三年前日期
    date_years_before = (now - relativedelta(years=lag)).strftime('%Y%m%d')

    # 取出X年以内离职的基金经理人数数据
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING MAX(d.F4_1272) >= '%(date_years_before)s' AND MIN(d.F4_1272) IS NOT NULL)e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

            ''' % {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    off_managers = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'off_managers'])

    # 取出基金公司基金经理总人数标签
    ##无需设置参数，输出结果分别为基金公司、基金公司成立日、基金公司基金经理总人数
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING (MAX(d.F4_1272) >= '%(date_years_before)s' OR MIN(d.F4_1272) IS NULL) AND MIN(d.F3_1272) < '%(date_years_before)s' )e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

            ''' % {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])

    off_managers = pd.merge(off_managers, sum_manager, on=['company', 'start_up_date'])
    off_managers['percent'] = off_managers['off_managers'] / sum_manager['sum_manager']

    # 计算基金经理近三年离职的百分位排名
    off_managers.sort_values("percent", axis=0, ascending=False, inplace=True)

    return off_managers


def main():
    sum_manager_percent()
    mean_manager_date_end_percent()
    off_managers_per()


if __name__ == "__main__":
    main()
