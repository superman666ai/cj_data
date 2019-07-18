#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/10 17:03
# @Author  : GaoJian
# @File    : manager_his.py

"""

--基金经理 生涯、年度：回报(区间收益)、回撤、波动、基准超额、指数超额(300,500,800, 债券指数)

具体来说 就是  基金 在基金经理维度 每年和 任期的 区间收益、年化波动、最大回撤


"""
from sql_con import *
from func_tools import *
import pandas as pd
from zz_script_hub.season_label import *


# 行业配置
SQL_INDEX = """
            INSERT INTO t_manager_return_risk
            (fundid, managerid, rptcycle, startdate, enddate, fundreturn, maxdrawdown, volatility, maxdrawdownhs300,
            maxdrawdownzz500, maxdrawdownzz800, volatilityhs300, volatilityzz500, volatilityzz800,
            hs300return, hs300alpha, zz500return, zz500alpha, zz800return, zz800alpha)
            
            VALUES
            (:fundid, :managerid, :rptcyle, :startdate, :enddate, :fundreturn, :maxdrawdown, :volatility, :maxdrawdownhs300,
            :maxdrawdownzz500, :maxdrawdownzz800, :volatilityhs300, :volatilityzz500, :volatilityzz800,
            :hs300return, :hs300alpha, :zz500return, :zz500alpha, :zz800return, :zz800alpha)"""


db_insert_session = DBSession(SQL_INDEX)


def find_every_year():
    sql = """
            select f11_1272 基金经理代码, f2_1272 姓名 ,f16_1090 基金代码,ob_object_name_1090 基金简称,
            f3_1272 as 任职开始时间, 
            f4_1272 as 任职截止时间  
            from wind.tb_object_1272 
            left join wind.tb_object_1090 on f1_1272 = f2_1090
            where f3_1272 is not null and  f4_1090 ='J'  order by f11_1272,f1_1272
            """
    df = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                      columns=['manageid', 'name', 'fundid', 'fundname', 'start', 'end'])

    df.fillna(datetime.now().strftime("%Y%m%d"), inplace=True)

    res_df = pd.DataFrame()

    group = df.groupby("manageid")
    for manageid, manageinfo in group:

        # 按照基金分组
        group_fund = manageinfo.groupby("fundid")

        res_list = []
        for fundid, fundinfo in group_fund:

            for info in fundinfo.values:

                try:
                    start = (info[4])
                    end = (info[5])

                    # 计算任期以来的指标
                    fundid = fundid
                    manageid = manageid
                    rptcyle = '0'
                    start = start
                    end = end

                    # 区间收益
                    fundreturn = is_nan(interval_profit(fundid, start, end)[1])
                    # # 最大回撤
                    maxdrawdown = is_nan(max_draw_down(fundid, start, end))
                    # # 波动率 年化
                    volatility = is_nan(standard_deviation(fundid, start, end))
                    # # 沪深300最大回撤
                    maxdrawdownhs300 = is_nan(max_draw_down_index('000300', start, end))
                    # # 中证500最大回撤
                    maxdrawdownzz500 = is_nan(max_draw_down_index('000905', start, end))
                    # # 中证800最大回撤
                    maxdrawdownzz800 = is_nan(max_draw_down_index('000906', start, end))
                    # # 沪深300波动率 - 年化
                    volatilityhs300 = is_nan(standard_deviation_index('000300', start, end))
                    # # 中证500波动率 - 年化
                    volatilityzz500 = is_nan(standard_deviation_index('000905', start, end))
                    # # 中证800波动率 - 年化
                    volatilityzz800 = is_nan(standard_deviation_index('000906', start, end))
                    # # 沪深300收益率 -
                    hs300return = is_nan(interval_profit_index('000300', start, end)[1])
                    # # 沪深300超额
                    hs300alpha = fundreturn - hs300return
                    # #  中证500收益率
                    zz500return = interval_profit_index('000905', start, end)[1]
                    # #  中证500超额
                    zz500alpha = fundreturn - zz500return
                    # #  中证800收益率
                    zz800return = interval_profit_index('000906', start, end)[1]
                    # #  中证800超额
                    zz800alpha = fundreturn - zz800return
                    """----------暂时没有接口----------------"""

                    # # 基金基准波动率 - 年化
                    # dic["volatilitybench"] = count_number(date[0], date[1])
                    # # 基金基准最大回撤
                    # dic["maxdrawdownbench"] = count_number(fundid, date[0], date[1])

                    # # 基金基准收益率 -
                    # dic["benchmarkreturn"] = count_number(date[0], date[1])
                    # # 基金基准超额 -
                    # dic["benchmarkalpha"] = count_number(date[0], date[1])

                    # #  债券基准收益率
                    # dic["bondindexreturn"] = count_number(date[0], date[1])
                    # #  债券基准超额
                    # dic["bondindexalpha"] = count_number(date[0], date[1])
                    """----------暂时没有接口----------------"""

                    rec = (fundid, manageid, rptcyle, start, end, fundreturn, maxdrawdown, volatility, maxdrawdownhs300,
                           maxdrawdownzz500, maxdrawdownzz800, volatilityhs300, volatilityzz500, volatilityzz800,
                           hs300return, hs300alpha, zz500return, zz500alpha, zz800return, zz800alpha)

                    print(rec)
                    db_insert_session.add_info(rec)

                    # 计算每年的指标
                    date_iter = deal_time(start, end)
                    for year, date in date_iter:

                        fundid= fundid
                        manageid = manageid
                        rptcyle= year
                        start = date[0]
                        end= date[1]

                        # 区间收益
                        fundreturn = is_nan(interval_profit(fundid, date[0], end)[1])
                        # # 最大回撤
                        maxdrawdown = is_nan(max_draw_down(fundid, start, end))
                        # # 波动率 年化
                        volatility = is_nan(standard_deviation(fundid, start, end))
                        # # 沪深300最大回撤
                        maxdrawdownhs300 = is_nan(max_draw_down_index('000300', start, end))
                        # # 中证500最大回撤
                        maxdrawdownzz500 = is_nan(max_draw_down_index('000905', start, end))
                        # # 中证800最大回撤
                        maxdrawdownzz800 = is_nan(max_draw_down_index('000906', start, end))
                        # # 沪深300波动率 - 年化
                        volatilityhs300 = is_nan(standard_deviation_index('000300', start, end))
                        # # 中证500波动率 - 年化
                        volatilityzz500 = is_nan(standard_deviation_index('000905', start, end))
                        # # 中证800波动率 - 年化
                        volatilityzz800 = is_nan(standard_deviation_index('000906', start, end))
                        # # 沪深300收益率 -
                        hs300return = is_nan(interval_profit_index('000300', start, end)[1])
                        # # 沪深300超额
                        hs300alpha = fundreturn - hs300return
                        # #  中证500收益率
                        zz500return = interval_profit_index('000905', start, end)[1]
                        # #  中证500超额
                        zz500alpha = fundreturn - zz500return
                        # #  中证800收益率
                        zz800return = interval_profit_index('000906', start, end)[1]
                        # #  中证800超额
                        zz800alpha = fundreturn - zz800return

                        rec = (fundid, manageid, rptcyle, start, end, fundreturn, maxdrawdown, volatility, maxdrawdownhs300,
                               maxdrawdownzz500, maxdrawdownzz800, volatilityhs300, volatilityzz500, volatilityzz800,
                               hs300return, hs300alpha, zz500return, zz500alpha, zz800return, zz800alpha)

                        print(rec)
                        db_insert_session.add_info(rec)
                except Exception as e:
                    print(e)
                    continue


    db_insert_session.finish()


def deal_time(st_date=None, ed_date=None):
    rpts = {}

    for j in range(int(st_date[0:4]), int(ed_date[0:4]) + 1, 1):
        if j == int(st_date[0:4]):
            rpts[str(j)] = [st_date, str(j) + '1231']
        elif j == int(ed_date[0:4]):
            rpts[str(j)] = [str(j) + '0101', ed_date]
        else:
            rpts[str(j)] = [str(j) + '0101', str(j) + '1231']

    return rpts.items()


def is_nan(value):
    if str(value) == 'nan':
        return 0
    else:
        return value



def count_number(start=None, end=None):
    import numpy as np
    return np.random.random_sample([1])[0]


if __name__ == '__main__':


    find_every_year()
