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

[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.125.222:1521', 'pra']
fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
cu_pra = fund_db_pra.cursor()


# connect to ziguan


def predict(start_time,end_time,code,stock_index):
    df=pd.read_excel('上下限.xlsx',dtype ={'fundid':str})
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

    return df2




# 行业配置
SQL_INDEX = """
                    INSERT INTO t_fund_opt_ability
                    ( FUNDID, TRADEDATE, rptcycle, timeoptabilityhs300, INDUSTRYABILITYHS300) 
                    VALUES(:fundId, :tradedate, :rptcycle,:timeoptabilityhs300,:INDUSTRYABILITYHS300)"""

db_insert_session = DBSession(SQL_INDEX)


class ChoiceP(object):

    def __init__(self, fundid, date):
        self.fundid = fundid
        self.date = date
        self.befor_date = date_gen(days=1, end=self.date)



    def load_industry_pro(self, incode):
        sql = """
                   select f16_1090 as tradingcode, f2_1474 as dates, f7_1474 as lastdayclose
                   from wind.tb_object_1474 left join wind.tb_object_1090
                   on f1_1474=f2_1090
                   where f4_1090 ='S'
                   and f2_1474 = '{}'
                   and f16_1090 in '{}'
                   """.format(self.date, incode)

        db_data = pd.DataFrame(sql_oracle.cu.execute(sql).fetchall(),
                               columns=['琛屼笟浠ｇ爜', '鏃ユ湡', '琛屼笟鏀剁泭'])
        to_day = db_data["琛屼笟鏀剁泭"].values[0]

        sql = """
                          select f16_1090 as tradingcode, f2_1474 as dates, f7_1474 as lastdayclose
                          from wind.tb_object_1474 left join wind.tb_object_1090
                          on f1_1474=f2_1090
                          where f4_1090 ='S'
                          and f2_1474 = '{}'
                          and f16_1090 in '{}'
                          """.format(self.befor_date, incode)

        db_data = pd.DataFrame(sql_oracle.cu.execute(sql).fetchall(),
                               columns=['琛屼笟浠ｇ爜', '鏃ユ湡', '琛屼笟鏀剁泭'])

        before_day = db_data["琛屼笟鏀剁泭"].values[0]


        return (to_day - before_day) / before_day

    def load_industry_weight(self):
        """
        行业权重
        :return:
        """
        # date 处理


        date_new = self.date[:4] + "-" + self.date[4:6] + "-" + self.date[6:]


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

        if self.date == '20190719':
            market = market.loc[['20190718']]
        else:
            market = market.loc[[self.date]]
        return market["index_pro"].values[0]

    def count(self):

        try:
            # 获取权重
            weight_df = self.load_industry_weight()

            if weight_df.empty:
                return 0



            tem_df = pd.DataFrame()

            pro_list = []
            # 获取行业收益
            for i in weight_df.行业代码.values:

                pro = self.load_industry_pro(i[:-3])
                pro_list.append(pro)


            weight_df["industry_pro"] = pro_list

            # 获取指数收益率
            index_pro = self.market_fetch('000300')


            weight_df["index_pro"] = index_pro



            weight_df['权益占比'] = weight_df['权益占比'].astype("float64")

            weight_df = weight_df.eval("choice = 权益占比*(industry_pro-index_pro ) ")

        except Exception as e:
            e
            return 0

        return weight_df['choice'].sum()

    def indust_opt(self, code, start, end):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        weight_date = start[:4] + "-" + start[4:6] + "-" + start[6:]

        sql = """
                select * from fund_predict_sw1detail
                where fundcode='{}' and tradedate='{}'and predict_weight !=0 
                """.format(code, weight_date)
        df = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                               columns=['基金代码', '日期', '行业代码', '权益占比', '行业名称'])

        print(df.head())

        return




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
    import calendar
    Format = "%d-%d-%d"
    for i in range(7, 8):

        try:
            d = calendar.monthrange(year, i)
            begin = Format % (year, i, 1)
            end = Format % (year, i, d[1])
            stock_index = '000300'

            other_df = predict(datetime.strptime(begin, "%Y-%m-%d").strftime("%Y%m%d"),'20190718', code, stock_index)

            datelist = data_list_func(begin, end)



            df = pd.DataFrame()
            date_list = []
            sum_list = []

            for date in datelist:
                end_time = date
                obj = ChoiceP(code, end_time)
                sum_value = obj.count()
                if sum_value == 0:
                    continue
                date_list.append(end_time)
                sum_list.append(sum_value)


            df['date'] = date_list
            df['date'] = df['date'].astype('str')
            df['sum'] = sum_list


            df = pd.merge(df, other_df, how='left', on='date')

            # 指数补充空值
            df['value'].fillna(method='bfill', inplace=True)
            df['value'].fillna(method='ffill', inplace=True)

            print(df.tail())

            df["fundid"] = code
            df["rptcycle"] = 'D'

            if sum(df['sum']) == 0:
                print('sum  为 0 ')
                continue
            if sum(df['value']) == 0:
                print('value  为 0 ')
                continue

            df['sum_new'] = df['sum'].apply(lambda x: x + 1)
            df['value_new'] = df['value'].apply(lambda x: x + 1)

            dic = {}
            dic['date'] = date[0:6]
            dic['sum'] = df['sum_new'].cumprod().values[-1] - 1
            dic['value'] = df['value_new'].cumprod().values[-1] - 1
            dic['fundid'] = code
            dic['rptcycle'] = 'M'
            df = df.append([dic])

            del df['sum_new']
            del df['value_new']

            # df = df.tail(1)
            for k in df.values:
                rec = (k[3], k[0], k[4], k[1], k[2])
                # FUNDID, TRADEDATE, rptcycle, timeoptabilityhs300, INDUSTRYABILITYHS300
                print(rec)
                db_insert_session.add_info(rec)
        except Exception as e:
            print(e)
            continue

    db_insert_session.finish()

####传入起止时间,基金代码，指数代码，返回一个dataframe，里面包含起止时间内每天的择时能力的数值
if __name__ == '__main__':
    #
    # sql = '''select distinct(fundcode) from fund_predict_sw1detail'''
    # code = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(), columns=['code'])
    # i = 0
    # number = len(code.values)
    # print(len(code.values))
    # for code in code.values:
    #     i += 1
    #     print('第 {} ------，总数{}'.format(i, number))
    #     code = code[0]
    #     year = 2019
    #     print(code)
    #     main(year, code)
    #
    start = '20190401'
    end = '20190430'
    code = '166006'

    obj = ChoiceP('166006', '20190719')
    obj.indust_opt(code, start, end)



