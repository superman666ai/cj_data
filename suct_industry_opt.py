#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/10 9:58
# @Author  : GaoJian
# @File    : suct_industry_opt.py

from sql_con import *
from func_tools import *
# 行业配置能力


SQL_IN = """
                    INSERT INTO t_fund_industry_opt
                    ( INDUSTRYID, FUNDID, TRADEDATE, INDUSTRYCFGHS300,INDUSTRYLEVEL, INDUSTRYNAME) 

                    VALUES(:INDUSTRYID, :FUNDID, :TRADEDATE,:INDUSTRYCFGHS300, :INDUSTRYLEVEL, :SWNAME)"""



db_insert_session = DBSession(SQL_IN)



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
                select * from fund_predict_sw1detail
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

        try:
            # 行业占比
            weight_df = self.load_industry_weight()



        #     tem_df = pd.DataFrame()
        #
        #
            pro_list = []
            # 获取行业收益
            for i in weight_df.行业代码.values:

                pro = self.load_industry_pro(i[:-3])
                pro_list.append(pro)

            weight_df["industry_pro"] = pro_list


        #     #获取指数收益率
            index_pro = self.market_fetch('000300')

            weight_df["index_pro"] = index_pro

            weight_df['权益占比'] = weight_df['权益占比'].astype("float64")

            weight_df = weight_df.eval("sum_pro = industry_pro-index_pro")



            return weight_df
        #
        #

        except Exception as e:
            return pd.DataFrame()
        #
        # return weight_df['choice'].sum()]


def all_date(datestart, dateend):
    from datetime import datetime
    if datestart is None:
        datestart = '2016-01-01'


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


def main(fundid, year):
    import calendar

    Format = "%d-%d-%d"

    for i in range(1, 8):
        d = calendar.monthrange(year, i)
        begin = Format % (year, i, 1)
        end = Format % (year, i, d[1])
        df = pd.DataFrame()

        date_list = all_date(begin, end)

        for date in date_list:
            try:
                print(date)
                obj = ChoiceP(fundid, date)
                sum_df = obj.count()
                df = df.append(sum_df)
            except Exception as e:
                continue

        grop = df.groupby("行业代码")

        indust_list = []
        indust_value_list = []
        indust_name_list = []

        for k, l in grop:
            indust_list.append(k.split('.')[0])
            indust_value_list.append(sum(l['sum_pro']))
            indust_name_list.append(l["行业名称"].values[0])



        if len(str(i)) == 1:
            trade_date = str(year)+ "0" + str(i)
        else:
            trade_date = str(year) + str(i)

        res_df = pd.DataFrame()


        res_df["行业代码"] = indust_list
        res_df['fundid'] = fundid
        res_df['trade_date'] = trade_date
        res_df['收益'] = indust_value_list
        res_df['等级'] = '1'
        res_df['名称'] = indust_name_list


        for i in res_df.values:

            rec = (i[0], i[1], i[2], i[3], i[4],i[5])

            print(rec)
            # db_insert_session.add_info(rec)

        # db_insert_session.finish()


if __name__ == '__main__':
    sql = '''select distinct(fundcode) from fund_predict_sw1detail'''
    code = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(), columns=['code'])

    for i in code.values:
        print(i)
        code = i[0]
        year = 2019
        main(code, year)







