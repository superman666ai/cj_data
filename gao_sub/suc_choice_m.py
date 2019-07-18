#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/10 14:41
# @Author  : GaoJian
# @File    : suc_choice.py


import pandas as pd

pd.set_option('display.max_columns', None)

from pandas import to_datetime
from sql_con import DBSession
from func_tools import *
from datetime import datetime, timedelta
import cx_Oracle

[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
cu_wind = fund_db.cursor()
[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
cu_pra = fund_db_pra.cursor()


# connect to ziguan





# 行业配置
SQL_INDEX = """
                    INSERT INTO t_fund_opt_ability_bak
                    ( FUNDID, TRADEDATE, rptcycle, timeoptabilityhs300, INDUSTRYABILITYHS300) 
                    VALUES(:fundId, :tradedate, :rptcycle,:timeoptabilityhs300,:INDUSTRYABILITYHS300)"""


db_insert_session = DBSession(SQL_INDEX)


def data_list_func(datestart, dateend):
    # 转为日期格式
    datestart = datetime.strptime(datestart, '%Y-%m-%d')
    dateend = datetime.strptime(dateend, '%Y-%m-%d')
    date_list = []
    date_list.append(datestart.strftime(str('%Y%m%d')))
    from datetime import timedelta

    while datestart < dateend:
        # 日期叠加一天
        datestart += timedelta(days=+1)
        # 日期转字符串存入列表
        date_list.append(str(datestart.strftime('%Y%m%d')))
    return date_list


def main(year, code):

    for i in range(1, 8):
        try:
            trade_date = year + '0'+ str(i)
            print(trade_date)

            sql = '''select fundid, tradedate, timeoptabilityhs300, INDUSTRYABILITYHS300 
            from  t_fund_opt_ability 
            where tradedate like '{}%' 
            and fundid='{}' and rptcycle != 'M' '''.format(trade_date, code)
            df = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                              columns=['fundid', 'tradedate', 'sum', 'value'])

            df['sum_new'] = df['sum'].apply(lambda x: x + 1)
            df['value_new'] = df['value'].apply(lambda x: x + 1)

            rec = (code, trade_date, 'M', df['sum_new'].cumprod().values[-1] - 1,
                   df['value_new'].cumprod().values[-1] - 1)

            print(rec)
            db_insert_session.add_info(rec)

        except Exception as e:
            print(e)
            continue
    db_insert_session.finish()


####传入起止时间,基金代码，指数代码，返回一个dataframe，里面包含起止时间内每天的择时能力的数值
if __name__ == '__main__':

    sql = '''select distinct(fundcode) from fund_predict_sw1detail'''
    code = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(), columns=['code'])
    i = 0
    number = len(code.values)
    print(len(code.values))
    for code in code.values:
        i += 1
        print(code)
        print('第 {} ------，总数{}'.format(i, number))
        code = code[0]
        year = '2019'

        main(year, code)







