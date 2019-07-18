#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/10 8:23
# @Author  : GaoJian
# @File    : choice.py



# 行业配置
import numpy as np
import pandas as pd

from sql_con import sql_oracle
from sql_con import DBSession
from func_tools import *

SQL_INDEX = """
                    INSERT INTO t_fund_opt_ability
                    ( FUNDID, TRADEDATE, rptcycle, timeoptabilityhs300) 
                    VALUES(:fundId, :tradedate, :rptcycle,:timeoptabilityhs300)"""




db_insert_session = DBSession(SQL_INDEX)



class ChoiceP(object):

    def __init__(self, fundid, date):
        self.fundid = fundid
        self.date = date
        self.befor_date = date_gen(days=1,end=self.date)


    def load_industry_pro(self, incode):
        sql = """
                select f16_1090 as tradingcode, f2_1474 as dates, f7_1474 as lastdayclose
                from wind.tb_object_1474 left join wind.tb_object_1090
                on f1_1474=f2_1090
                where f4_1090 ='S'
                and f2_1474 in '{}'
                and f16_1090 in '{}'
                """.format(self.date, incode)

        db_data = pd.DataFrame(sql_oracle.cu.execute(sql).fetchall(),
                               columns=['行业代码', '日期', '行业收益'])
        to_day = db_data["行业收益"].values[0]

        sql = """
                       select f16_1090 as tradingcode, f2_1474 as dates, f7_1474 as lastdayclose
                       from wind.tb_object_1474 left join wind.tb_object_1090
                       on f1_1474=f2_1090
                       where f4_1090 ='S'
                       and f2_1474 in '{}'
                       and f16_1090 in '{}'
                       """.format(self.befor_date, incode)

        db_data = pd.DataFrame(sql_oracle.cu.execute(sql).fetchall(),
                               columns=['行业代码', '日期', '行业收益'])


        before_day = db_data["行业收益"].values[0]

        return (to_day-before_day) / before_day



    def load_industry_weight(self):
        """
        行业权重
        :return:
        """
        # date 处理
        date_new = self.date[:4] + "-" + str(int(self.date[4:6]))+"-"+ self.date[6:]

        sql = """
                select * from t_swyjhyyc
                where fundcode='{}' and tradedate='{}'and predict_weight !=0 """.format(self.fundid, date_new)
        db_data = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                               columns=['基金代码', '日期', '行业代码', '权益占比', '行业名称'])

        return db_data

        # 取市场指数数据


    # 取市场指数数据
    def market_fetch(self, stock_index):

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

        market['index_pro'] = market['指数收盘价'].pct_change()
        market.dropna(inplace=True)

        market.index = market['日期']
        market = market.loc[[self.date]]


        return market["index_pro"].values[0]


    def count(self):
        # 获取权重
        weight_df = self.load_industry_weight()

        tem_df = pd.DataFrame()
        print("----", tem_df.head())

        pro_list = []
        # 获取行业收益
        for i in weight_df.行业代码.values:

            pro = self.load_industry_pro(i[:-3])
            pro_list.append(pro)

        weight_df["industry_pro"] = pro_list

        #获取指数收益率
        index_pro = self.market_fetch('000300')

        weight_df["index_pro"] = index_pro

        weight_df['权益占比'] = weight_df['权益占比'].astype("float64")


        weight_df = weight_df.eval("choice = 权益占比*(industry_pro-index_pro ) ")

        return weight_df['choice'].sum()







def main_shell():

    sql = """select distinct(tradedate)
            from t_fund_opt_ability"""

    date_list = sql_oracle.cu_pra_sel.execute(sql).fetchall()

    new_df = pd.DataFrame()

    sum_list = []
    date_list_new = []
    fundid = '001040'
    rptcycle = "M"
    print(fundid, "------")
    for i in date_list:

        date = str(i[0])
        obj = ChoiceP(fundid, date)
        sum = obj.count()

        sum_list.append(sum)
        date_list_new.append(date)
        break

    new_df["fundid"] = fundid
    new_df["日期"] = date_list_new
    new_df["sum"] = sum_list
    new_df["rptcycle"] = rptcycle

    print(new_df)

        # sql = """ INSERT INTO t_fund_opt_ability
        #             ( FUNDID, TRADEDATE, rptcycle, timeoptabilityhs300)
        #             VALUES('{}','{}','{}','{}')
        # """.format(code, date, rptcycle, sum*20)

        # rec = (code, date, rptcycle, sum*20)
        # print(rec)
    #     db_insert_session.add_info(rec)
    # db_insert_session.finish()




if __name__ == '__main__':
    start_date, end_date = '20180101', '20191232'
    main_shell()
