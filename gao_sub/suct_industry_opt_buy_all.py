#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/10 9:58
# @Author  : GaoJian
# @File    : suct_industry_opt.py

from sql_con import *
from func_tools import *
import numpy as np

# 行业配置

SQL_IN = """
                    INSERT INTO t_fund_industry_opt
                    ( INDUSTRYID, FUNDID, TRADEDATE, INDUSTRYCFGHS300,INDUSTRYLEVEL, INDUSTRYNAME) 

                    VALUES(:INDUSTRYID, :FUNDID, :TRADEDATE,:INDUSTRYCFGHS300, :INDUSTRYLEVEL, :SWNAME)"""

db_insert_session = DBSession(SQL_IN)


class ChoiceP(object):

    def __init__(self, fundid, year):
        self.fundid = fundid
        self.year = year

    def load_industry_pro(self, incode):
        sql = """
                select f16_1090 as tradingcode, f2_1474 as dates, f7_1474 as lastdayclose
                from wind.tb_object_1474 left join wind.tb_object_1090
                on f1_1474=f2_1090
                where f4_1090 ='S'
                and f16_1090 in '{}'
                """.format(incode)

        df = pd.DataFrame(sql_oracle.cu.execute(sql).fetchall(),
                               columns=['行业代码', '日期', '行业收益'])

        df['industry_pro'] = df['行业收益'].pct_change()

        del df['行业代码']

        del df['行业收益']

        return df

    def load_industry_weight(self):
        """
        行业权重
        :return:
        """
        # # date 处理
        # date_new = self.date[:4] + "-" + str(int(self.date[4:6])) + "-" + self.date[6:]

        sql = """
                select * from fund_predict_sw1detail
                where fundcode='{}' and predict_weight !=0""".format(self.fundid)
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

        del market['指数收盘价']

        return market

    def count(self):

        # 行业占比
        weight_df = self.load_industry_weight()

        grop = weight_df.groupby("行业代码")

        index_pro = self.market_fetch('000300')

        print(weight_df)


        for i, j in grop:
            try:
                j["日期"] = j["日期"].apply(lambda x: str(datetime.strptime(x, '%Y-%m-%d').strftime("%Y%m%d")))
                # print(j.head())
                j = pd.merge(j, index_pro, how='left', on='日期')

                # 查询行业收益
                indust_df = self.load_industry_pro(i[:-3])

                j = pd.merge(j, indust_df, how='left', on='日期')

                j = j.eval("pro =权益占比*(industry_pro-index_pro) +1 ")


                value = j['pro'].cumprod().values[-1] - 1
                if str(value) == 'nan':
                    continue

                rec = (i.split('.')[0], self.fundid, '2019', value, '1', j["行业名称"].values[0])


                print(rec)
                db_insert_session.add_info(rec)

            except Exception as e:
                print(e)
                continue
        db_insert_session.finish()





def main(code, year):
    obj = ChoiceP(code, year)
    obj.count()


if __name__ == '__main__':
    sql = '''select distinct(fundcode) from fund_predict_sw1detail'''
    code = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(), columns=['code'])

    num = len(code.values)
    for i in code.values:
        print(i, '------------', num)
        code = i[0]
        year = 2019
        main(code, year)

    #
    # code = '001040'
    # main(code, 2019)

