# -*- coding: utf-8 -*-
"""
Created on Fri Jul  5 14:01:46 2019

@author: lenovo
"""

# HP滤波
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sql_con import sql_oracle
from sql_con import DBSession
from func_tools import *


SQL_INDEX_INSERT = """
                    INSERT INTO t_fund_trend
                    ( FUNDID, TRADEDATE, STOCKTONAV, FUNDTREND, HS300CLOSE, HS300TREND) 
        
                    VALUES(:fundId, :tradedate, :stocktonav,:fundtrend, :hs300close, :hs300trend)"""

db_insert_session = DBSession(SQL_INDEX_INSERT)


class HPfilter(object):

    def load_fund(self):
        """

        :return:
        """

    # 取市场指数数据
    def market_fetch(self, stock_index):
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
        market = pd.DataFrame(sql_oracle.cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
        # market.index = market['日期']
        # del market['日期']
        # market.columns = ["指数"]
        return market


    def find_all_fundcode(self):
        sql = """select distinct(fundcode)
                from fund_predict_sw1detail"""
        dbdata = sql_oracle.cu_pra_sel.execute(sql).fetchall()

        return dbdata


    def hpfilter_func(self):
        """

        :return:
        """

        all_fundcode = self.find_all_fundcode()

        # print(all_fundcode)
        #
        # all_fundcode = ['163801']
        num = len(all_fundcode)
        n = 1

        for code in all_fundcode:
            print('-----{}----num:{}'.format(n, num))
            n+=1
            code = code[0]

            sql = """select tradedate, fundcode, sum(predict_weight) 
            from fund_predict_sw1detail where fundcode = '{}'
            group by  fundcode, tradedate order  by tradedate""".format(code)

            dbdata = sql_oracle.cu_pra_sel.execute(sql).fetchall()

            df = pd.DataFrame(dbdata, columns=['日期', '基金代码', '权益占比'])
            df['日期'] = pd.to_datetime(df['日期'])

            # 指数
            mark_df = self.market_fetch('000300')

            mark_df['日期'] = pd.to_datetime(mark_df['日期'])

            df = pd.merge(df, mark_df, how="left", on="日期")

            # 指数补充空值
            df['指数收盘价'].fillna(method='bfill', inplace=True)
            df['指数收盘价'].fillna(method='ffill', inplace=True)


            index = df['日期']

            df.set_index(index, inplace=True)
            # print(df.head())
            lamta = 1600

            # 计算趋势
            df['权益占比'] = df['权益占比'].astype("float32")

            cycle, trend = sm.tsa.filters.hpfilter(df['权益占比'], lamta)


            _, df['权益占比趋势'] = cycle, trend

            df['指数收盘价'] = df['指数收盘价'].astype('float32')

            cycle, trend = sm.tsa.filters.hpfilter(df['指数收盘价'], lamta)

            _, df['指数收盘价趋势'] = cycle, trend

            df.reset_index(drop=True, inplace=True)

            df["日期"] = df["日期"].apply(lambda x: x.strftime("%Y%m%d"))

            for i in df.values:
                rec = (i[1], i[0], i[2], i[4], i[3], i[-1])

                print(rec)
                db_insert_session.add_info(rec)

        db_insert_session.finish()







def main():
    obj = HPfilter()
    obj.hpfilter_func()



if __name__ == '__main__':
    main()
