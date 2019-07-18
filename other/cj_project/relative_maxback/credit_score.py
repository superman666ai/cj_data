#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/3 16:22
# @Author  : GaoJian
# @File    : credit_score.py

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

import time
import math

# 连接数据库
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
start_time = time.time()
cu = fund_db.cursor()

# 投研平台库
[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
fund_db_pra = cx_Oracle.connect(user=userNamepif, password=passwordpif, dsn=hostIPpif + '/' + dbNamepif)
cu_pra = fund_db_pra.cursor()


def find_reportdate(date=None):
    """
    根据输入日期查询 最新季报日期
    :param date:
    :return:
    """

    # 根据当前日期判断最新季报日期
    # 每季度截止15个工作日内披露季报，经过测算，16个工作日内全部基金的季报披露完，保险起见，设置大于等于17个工作日为阈值，即17个工作日之后认为季报全部披露
    if not date:
        end_date = datetime.datetime.now() + timedelta(days=-1)
        end_date = end_date.strftime('%Y%m%d')

    else:
        end_date = date

    # 日期问题
    # 取出所有的交易日
    sql = '''
        SELECT F1_1010
        FROM wind.TB_OBJECT_1010
        WHERE
        F1_1010 < '20251231'
        ORDER BY
        F1_1010 DESC
        '''
    trade_dates = pd.DataFrame(cu.execute(sql).fetchall(), columns=['交易日期'])

    if end_date[4:6] in ['01', '02', '03']:
        report = str(int(end_date[0:4]) - 1) + '1231'
        day = len(trade_dates[(trade_dates.交易日期 <= end_date) & (trade_dates.交易日期 > report)])
        if day >= 17:
            reportdate = report
        else:
            reportdate = str(int(end_date[0:4]) - 1) + '0930'
    elif end_date[4:6] in ['04', '05', '06']:
        report = end_date[0:4] + '0331'
        day = len(trade_dates[(trade_dates.交易日期 <= end_date) & (trade_dates.交易日期 > report)])
        if day >= 17:
            reportdate = report
        else:
            reportdate = str(int(end_date[0:4]) - 1) + '1231'
    elif end_date[4:6] in ['07', '08', '09']:
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

    print('使用季报日期:' + str(reportdate))
    return reportdate


def find_hold_allmarket(reportdate):
    """
    全市场持有证券明细，不包括QD基金
    :return:
    """

    sql_stock = '''
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
       ''' % {'date': reportdate}

    holded = pd.DataFrame(cu.execute(sql_stock).fetchall(),
                          columns=['基金代码', '报告期', 'A股代码', '港股代码', 'A股简称', '港股简称', 'A股类型', '港股类型', '持有比例',
                                   '持有证券ID']).sort_values(
        by='基金代码', ascending=True).reset_index(drop=True)

    holded['证券代码'] = holded.apply(lambda x: x[2] or x[3], axis=1)
    holded['证券简称'] = holded.apply(lambda x: x[4] or x[5], axis=1)
    holded['证券类型'] = holded.apply(lambda x: x[6] or x[7], axis=1)
    return holded


def find_all_credit_info():
    """
    直接全部取出所有债券的评级信息，并保留最新的评级信息
    :return:
    """

    sql2 = '''
            SELECT
              T0.F16_1090 AS 证券代码,
              T1.F3_1735 AS 信用等级,F4_1735
            FROM
            wind.TB_OBJECT_1735 T1  
            LEFT JOIN wind.TB_OBJECT_1090 T0 ON T0.F2_1090 = T1.F1_1735
            '''
    bond_rating = pd.DataFrame(cu.execute(sql2).fetchall(), columns=['证券代码', '信用评级', '公告日期']).sort_values(by='公告日期',
                                                                                                          ascending=False)
    bond_rating.drop_duplicates('证券代码', 'first', inplace=True)
    del bond_rating['公告日期']

    return bond_rating


def find_rating():
    """
    查询同业存单
    """
    return pd.DataFrame([])


def find_debt_fund():
    sql = """
    SELECT CPDM
    FROM T_FUND_CLASSIFY_HIS
    WHERE DL = '偏债类'"""

    df_fund = pd.DataFrame(cu_pra.execute(sql).fetchall(), columns=['基金代码'])
    return df_fund


def credit_score(date=None):
    # 查找季报日期
    reportdate = find_reportdate(date)

    # 查找全市场持有证券明细
    holded = find_hold_allmarket(reportdate)

    # 筛选其中的偏债型基金
    net_bond = find_debt_fund()

    holded1 = holded[holded.基金代码.isin(net_bond['基金代码'])]

    # 直接全部取出所有债券的评级信息，并保留最新的评级信息
    bond_rating = find_all_credit_info()

    # 查询同业存单
    rating = find_rating()

    # 将同业存单的评级信息合并进去
    bond_rating2 = bond_rating.append(rating)
    bond_rating2.drop_duplicates('证券代码', 'first', inplace=True)
    portfolio_bond = pd.merge(holded1, bond_rating2, on='证券代码', how='left')

    # 评级
    net_bond = pd.DataFrame(portfolio_bond["基金代码"].unique(), columns=["基金代码"])

    net_all = deepcopy(net_bond)
    net_maxbond = pd.DataFrame(columns=['基金代码', '报告期', '证券代码', '证券简称', '持有比例', '证券类型', '信用评级'])
    for i in net_all.index:
        code = net_all.loc[i, '基金代码']
        df1 = portfolio_bond[portfolio_bond.基金代码 == code]
        j = reportdate
        try:
            df2 = df1[df1.报告期 == j].sort_values(by='基金代码', ascending=True).reset_index(drop=True)
            df3 = df2[(df2.证券类型 != 'Ａ股') & (df2.证券类型 != '资产支持证券') & (df2.证券类型 != '期货')].sort_values(by='持有比例',
                                                                                                    ascending=False).reset_index(
                drop=True)

            net_maxbond = net_maxbond.append(df3[['基金代码', '报告期', '证券代码', '证券简称', '持有比例', '证券类型', '信用评级']],
                                             ignore_index=True)
        except KeyError:
            pass


    # 将缺失的金融债的信用评级填充为AAA
    net_maxbond.loc[(net_maxbond.证券类型 == '金融债') & (net_maxbond.信用评级 == 0), '信用评级'] = 'AAA'


    # 打信用评级估计标签
    bond = deepcopy(pd.DataFrame(net_maxbond["基金代码"].unique(), columns=["基金代码"]))
    for i in bond.index:
        code = bond.loc[i, '基金代码']

        index1 = bond[bond.基金代码 == code].index[0]
        df = net_maxbond[net_maxbond.基金代码 == code][['信用评级', '持有比例']]
        df = df.sort_values(by='持有比例', ascending=False).reset_index(drop=True)
        df = df[df.信用评级 != 0]

        try:
            kind = pd.DataFrame(df['信用评级'].drop_duplicates(), columns=['信用评级'])
            for g in kind.index:
                type = kind.loc[g, '信用评级']
                df4 = df[df.信用评级 == type]
                kind.loc[g, '持有比例'] = df4.持有比例.sum()
            try:
                AAA = kind[kind.信用评级 == 'AAA'].iloc[-1, -1]
            except IndexError:
                AAA = 0
            try:
                AA = kind[kind.信用评级 == 'AA+'].iloc[-1, -1]
            except IndexError:
                AA = 0
            try:
                A1 = kind[kind.信用评级 == 'A-1'].iloc[-1, -1]
            except IndexError:
                A1 = 0
            a = df.持有比例.sum()
            try:
                ratio = (AAA + AA + A1) / a
            except ZeroDivisionError:
                ratio = np.nan
            net_maxbond.loc[i, '估算信用评级比例'] = ratio
            if ratio >= 0.8:
                bond.loc[index1, '信用评级'] = '高-信用等级'
            elif 0.5 <= ratio < 0.8:
                bond.loc[index1, '信用评级'] = '中-信用等级'
            elif ratio < 0.5:
                bond.loc[index1, '信用评级'] = '低-信用等级'
        except IndexError:
            pass

    print(bond)


if __name__ == '__main__':
    date = "20180101"
    credit_score(date)
