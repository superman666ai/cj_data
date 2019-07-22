import csv
import pandas as pd
import numpy as np
import codecs
import os
import sys
import xlrd
from dateutil.parser import parse
pd.set_option('display.max_columns', None)
import time
from pandas import to_datetime
#import seaborn as sns
#import matplotlib.pyplot as plt

# from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LinearRegression
import math
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import threading
import multiprocessing
#connect to ziguan
import cx_Oracle
# pd.set_option('display.max_columns', None)
# In[2]:
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user = userName, password = password, dsn = hostIP + '/' + dbName)
cu_wind = fund_db.cursor()
[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.125.222:1521', 'pra']
fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
cu_pra = fund_db_pra.cursor()
#connect to ziguan




def predict(start_time,end_time,code,stock_index):
    df=pd.read_excel('gao_sub/上下限.xlsx',dtype ={'fundid':str})
    # print(df)
    df.columns=['1','2','jjname','3','4','6','qyl_min','qyl_max','min','max','fundid','7']

    df=df[['jjname','qyl_min','qyl_max','min','max','fundid']]
    # print(df)
    jj=df[df['fundid']==code]

    median=(jj.iloc[0,1]+jj.iloc[0,2])/200

    sql='''
    select fundcode,tradedate,predict_weight from fund_predict_sw1detail where fundcode='%(code)s'
    '''% { 'code': code}

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
    ###指数
    market = pd.DataFrame(cu_wind.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])

    qy=pd.read_sql(sql,fund_db_pra)
    qy.TRADEDATE = to_datetime(qy.TRADEDATE, format="%Y-%m-%d")
    qy.TRADEDATE = qy.TRADEDATE.apply(lambda x: datetime.strftime(x, "%Y%m%d"))
    date=qy[(qy['TRADEDATE']>=start_time) & (qy['TRADEDATE']<=end_time)]
    date=date[date['FUNDCODE']==code]

    times=date['TRADEDATE'].drop_duplicates()
    sum=0
    df2=pd.DataFrame()
    for i in times:
        tday=market[market['日期']==i]
        ###剔除非交易日
        if tday.empty:
            pass
        else:

            index1=tday.index
            t1day=market.loc[index1-1,'指数收盘价']
            t1day=t1day.tolist()
            t1day=t1day[0]
            per=(t1day-tday.iloc[0,1])/(tday.iloc[0,1]+0.00000000000000001)
            df2.loc[i,'value']=((date[date['TRADEDATE']==i])['PREDICT_WEIGHT'].sum()-median)*per
            sum+=((date[date['TRADEDATE']==i])['PREDICT_WEIGHT'].sum()-median)*per

    df2['date']=df2.index
    print(df2)
    return df2




####传入起止时间,基金代码，指数代码，返回一个dataframe，里面包含起止时间内每天的择时能力的数值
if __name__=='__main__':
    start_time='20190701'
    end_time='20190718'
    code='166006'
    stock_index='000300'
    predict(start_time,end_time,code,stock_index)
