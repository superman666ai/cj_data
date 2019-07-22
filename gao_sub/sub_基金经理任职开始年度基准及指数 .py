#####我的算法
import pandas as pd
import os
import numpy as np
# import pymysql
import cx_Oracle
import time
from functools import reduce
from datetime import timedelta
from dateutil import rrule
import datetime
# from WindPy import w
from scipy import stats
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression
from dateutil.parser import parse
import math
import calendar

# from sqlalchemy import create_engine

# 最大回撤
# 公式可以这样表达：
# D为某一天的净值，i为某一天，j为i后的某一天，Di为第i天的产品净值，Dj则是Di后面某一天的净值
# drawdown就是最大回撤率
# drawdown=max（Di-Dj）/Di，其实就是对每一个净值进行回撤率求值，然后找出最大的。可以使用程序实现

# 算法逻辑
# 1.取该基金区间内的单位复权净值


# 连接数据库
[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)

# 连接数据库
[userName, password, hostIP, dbName, tablePrefix] = ['wind', 'wind', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db_wind = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)

# 当前文件名称
thisfilename = os.path.basename(__file__)
algid = '0000000031'
dblink_dict = {}
dblink_dict['wind'] = fund_db_wind
dblink_dict['投研'] = fund_db_pra
cu = fund_db_wind.cursor()
cu1 = fund_db_pra.cursor()


def zhoubodong_qz(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
        select
        f3_1288 as 截止日期, f2_1288 as 复权单位净值 
        from
        wind.TB_OBJECT_1288
        where 
        f1_1288 ='S3600328'
        and
        f3_1288 >= '%(start_date)s'
        and
        f3_1288 <= '%(end_date)s' order by f3_1288
        ''' % {'end_date': end_date, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)

    # zhou_fund_price = pd.DataFrame(fund_price2.iloc[0,:]).T
    # for i in fund_price2.index:
    #     if i>0 and i%1==0:
    #         zhou_fund_price = zhou_fund_price.append(fund_price2.iloc[i,:])
    #     else:
    #         pass
    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    return zhou_bodong


def fund_performance_qz(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select
        f3_1288 as 截止日期, f2_1288 as 复权单位净值 
        from
        wind.TB_OBJECT_1288
        where 
        f1_1288 ='S3600328'
        and
        f3_1288 >= '%(start_date)s'
        and
        f3_1288 <= '%(end_date)s' order by f3_1288 desc
        ''' % {'end_date': end_date, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance


def max_down_fund_qz(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
        f3_1288 as 截止日期, f2_1288 as 复权单位净值 
        from
        wind.TB_OBJECT_1288
        where 
        f1_1288 ='S3600328'
        and
        f3_1288 >= '%(start_date)s'
        and
        f3_1288 <= '%(end_date)s'  order by f3_1288
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0

    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)
    price_list = fund_price2['复权单位净值'].tolist()
    i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
    if i == 0:
        max_down_value = 0
    else:
        j = np.argmax(price_list[:i])  # 开始位置
        max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
    return -max_down_value


def zhoubodong_zh(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
     select t1.f2_1120 as 截止日期, t1.f8_1120 as 复权单位净值
        from tb_object_1120 t1, TB_OBJECT_1090 t2
         where t1.f1_1120 = t2.F2_1090
           and t2.f16_1090 = '%(code)s'
           AND f4_1090 = 'S'
           and t1.f2_1120 >= '%(start_date)s'
           and t1.f2_1120 <= '%(end_date)s'
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)

    # zhou_fund_price = pd.DataFrame(fund_price2.iloc[0,:]).T
    # for i in fund_price2.index:
    #     if i>0 and i%1==0:
    #         zhou_fund_price = zhou_fund_price.append(fund_price2.iloc[i,:])
    #     else:
    #         pass
    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    return zhou_bodong


def fund_performance_zh(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select t1.f2_1120 as 截止日期, t1.f8_1120 as 复权单位净值
        from tb_object_1120 t1, TB_OBJECT_1090 t2
         where t1.f1_1120 = t2.F2_1090
           and t2.f16_1090 = '%(code)s'
           AND f4_1090 = 'S'
           and t1.f2_1120 >= '%(start_date)s'
           and t1.f2_1120 <= '%(end_date)s'
         ORDER BY t1.f2_1120 DESC
''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance


def max_down_fund_zh(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select t1.f2_1120 as 截止日期, t1.f8_1120 as 复权单位净值
        from tb_object_1120 t1, TB_OBJECT_1090 t2
         where t1.f1_1120 = t2.F2_1090
           and t2.f16_1090 = '%(code)s'
           AND f4_1090 = 'S'
           and t1.f2_1120 >= '%(start_date)s'
           and t1.f2_1120 <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0

    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)
    price_list = fund_price2['复权单位净值'].tolist()
    i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
    if i == 0:
        max_down_value = 0
    else:
        j = np.argmax(price_list[:i])  # 开始位置
        max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
    return -max_down_value


def max_down_fund(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
    trade_dt as 截止日期, s_dq_close as 复权单位净值 
    from
    wind.chinamutualfundbenchmarkeod
    where 
    s_info_windcode= '%(code)s'
    and
    trade_dt >= '%(start_date)s'
    and
    trade_dt <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0

    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)
    price_list = fund_price2['复权单位净值'].tolist()
    i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
    if i == 0:
        max_down_value = 0
    else:
        j = np.argmax(price_list[:i])  # 开始位置
        max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
    return -max_down_value


def date_gen(days=None, months=None, years=None, end=None):
    """生成指定日期距离最近的交易日期
        功能
        --------
        生成指定日期之前几年/月/日距离最近的交易日期

        参数
        --------
        days:需要往前多少个日历日，格式为int，可以为具体数字，也可为变量
        months:需要往前多少个月份，格式为int，可以为具体数字，也可为变量
        years:需要往前多少个年份，格式为int，可以为具体数字，也可为变量
        end:截止日期，格式为'20171220'，以该截止日期往前距离XX日、XX月、XX年后最近的交易日期
        参数需要写全，如days=3。days、months、years必须输入一个，end也是必要参数

        返回值
        --------
        返回一个具体的日期，返回格式为字符格式，如'20181011'。

        参看
        --------
        if_trade(start_date)：关联函数。

        示例
        --------
        >>>a = date_gen(days = 3,end = '20181220')
        >>>a
        '20181217'

        """

    def none(par):
        if not par:  # 意思是如果par为None时
            par = 0
        return par

    [days, months, years] = [none(days), none(months), none(years)]
    end = pd.to_datetime(end)
    start_date = (end - relativedelta(days=days, months=months, years=years)).strftime('%Y%m%d')
    return start_date


def zhoubodong(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
        select
        trade_dt as 截止日期, s_dq_close as 复权单位净值 
        from
        wind.chinamutualfundbenchmarkeod
        where 
        s_info_windcode= '%(code)s'
        and
        trade_dt >= '%(start_date)s'
        and
        trade_dt <= '%(end_date)s'
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)

    # zhou_fund_price = pd.DataFrame(fund_price2.iloc[0,:]).T
    # for i in fund_price2.index:
    #     if i>0 and i%1==0:
    #         zhou_fund_price = zhou_fund_price.append(fund_price2.iloc[i,:])
    #     else:
    #         pass
    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    return zhou_bodong


def fund_performance(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select
    trade_dt as 截止日期, s_dq_close as 复权单位净值 
    from
    wind.chinamutualfundbenchmarkeod
    where 
    s_info_windcode= '%(code)s'
    and
    trade_dt >= '%(start_date)s'
    and
    trade_dt <= '%(end_date)s'  order by trade_dt desc
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance


def get_tradedate(zrr):
    cu1.execute('select jyr from pra_info.txtjyr t where t.zrr=:rq', rq=zrr)
    rs = cu1.fetchall()[0][0]
    return rs

def deal_time(st_date=None, ed_date=None):
    """
    返回一个可迭代对象
    :param st_date:
    :param ed_date:
    :return:
    """
    rpts = {}

    for j in range(int(st_date[0:4]), int(ed_date[0:4]) + 1, 1):
        if j == int(st_date[0:4]):
            rpts[str(j)] = [st_date, str(j) + '1231']
        elif j == int(ed_date[0:4]):
            rpts[str(j)] = [str(j) + '0101', ed_date]
        else:
            rpts[str(j)] = [str(j) + '0101', str(j) + '1231']

    return rpts.items()



starttime = datetime.datetime.now()
print(starttime)

fund_result = pd.DataFrame(
    columns=['基金代码', '基金简称', '二级分类', '基金经理', '任职以来最大回撤', '任职以来波动率', '任职以来回报'])

from sql_con import *

sql = """select cpdm, jjjc, yjfl, ejfl, clr from fund_classify"""

data = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                    columns=['基金代码', '基金简称', '一级分类', '二级分类', '成立日'])

# data = pd.read_excel('D:/分类-0213.xlsx', dtype=str)


data_kind = data[['基金代码', '基金简称', '一级分类', '二级分类']]
# data_kind = data_kind[(data_kind['一级分类']=='股票型')|(data_kind['一级分类']=='沪港深')|(data_kind['一级分类']=='配置型')|(data_kind['一级分类']=='配置型')|(data_kind['一级分类']=='QDII')]
# data_kind = data_kind[(data_kind['一级分类'] == '债券型')]
# data_kind = data_kind[data_kind['基金代码']=='000256']

num = len(data_kind.index)
for i in data_kind.index:

    print('第 {} 个----------总数{}'.format(i, num))

    fund_code = data_kind.基金代码[i]

    query_sql = '''SELECT t1.f16_1090, t.f2_1272, nvl(t.f3_1272,0), nvl(t.f4_1272,0),t.F11_1272
            from wind.TB_OBJECT_1272 t, wind.tb_object_1090 t1
            where t.f1_1272 = t1.f2_1090  and t1.f4_1090 = 'J'
               and t1.f16_1090 = '%(fund_code)s'    order by t.f3_1272
            ''' % {'fund_code': fund_code}
    fund_manager = pd.DataFrame(cu.execute(query_sql).fetchall(),
                                columns=['fundid', 'name', 'start', 'end', 'managerid'])

    # 日期 为 0 补充为20181231
    fund_manager.loc[fund_manager['end'] == '0', 'end'] = '20181231'

    print(fund_manager)

    for j in range(len(fund_manager.values)):
        fund_benchmark = fund_manager.values[j][0]
        benchmark_code = fund_benchmark + 'BI.WI'

        begin_date = fund_manager.values[j][2]
        manager_enddate = fund_manager.values[j][3]
        end_date = manager_enddate


        # 计算每年
        date_iter = deal_time(str(begin_date), str(manager_enddate))
        for year, date in date_iter:
            print("year: start, end", year, date[0], date[1])
            begin_date = date[0]
            manager_enddate = date[1]
            end_date = manager_enddate
            trade_date = begin_date[0:4]

            record_sql = '''select manager_enddate from FUND_BENCHMARK_PERFORMANCE_bak t where t.fundcode= '%(fund_code)s'
                         and t.fundmanager= '%(fundmanager)s' and trade_date ='%(trade_date)s' ''' % {
                'fund_code': fund_benchmark, 'fundmanager': fund_manager.values[j][1], 'trade_date': trade_date}
            manager_performance = pd.DataFrame(cu1.execute(record_sql).fetchall())

            # 判断该年度是否已经计算
            flag = manager_performance.empty
            if (not flag):
                print('{}年度已计算'.format(year))


            # 开始 结束 日期 转化为交易日
            if (get_tradedate(begin_date) != 0):
                begin_date = get_tradedate(begin_date)
            if (get_tradedate(end_date) != 0):
                end_date = get_tradedate(end_date)


            else:
                result_hc = max_down_fund(benchmark_code, begin_date, end_date)
                result_nhbd = zhoubodong(benchmark_code, begin_date, end_date)
                if (math.isnan(result_nhbd)):
                    result_nhbd = 0
                result_hb = fund_performance(benchmark_code, begin_date, end_date)
                rec = []
                rec.append(data_kind.基金代码[i])
                rec.append(data_kind.基金简称[i])
                rec.append(data_kind.二级分类[i])
                rec.append(fund_manager.values[j][1])
                rec.append(result_hc)
                rec.append(result_nhbd)
                rec.append(result_hb)
                rec.append(fund_manager.values[j][2])
                rec.append(fund_manager.values[j][3])
                rec.append(benchmark_code)
                rec.append(fund_manager.values[j][4])
                rec.append(trade_date)
                insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                             "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                cu1.execute(insert_sql, rec)
                if (data_kind.二级分类[i] == '可转债'):
                    result_hc = max_down_fund_zh('000832', begin_date, end_date)
                    result_nhbd = zhoubodong_zh('000832', begin_date, end_date)
                    if (math.isnan(result_nhbd)):
                        result_nhbd = 0
                    result_hb = fund_performance_zh('000832', begin_date, end_date)
                    rec = []
                    rec.append(data_kind.基金代码[i])
                    rec.append(data_kind.基金简称[i])
                    rec.append(data_kind.二级分类[i])
                    rec.append(fund_manager.values[j][1])
                    rec.append(result_hc)
                    rec.append(result_nhbd)
                    rec.append(result_hb)
                    rec.append(fund_manager.values[j][2])
                    rec.append(fund_manager.values[j][3])
                    rec.append('000832')
                    rec.append(fund_manager.values[j][4])
                    rec.append(trade_date)
                    insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                 "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                    cu1.execute(insert_sql, rec)
                elif (data_kind.二级分类[i] == '激进债券型' or data_kind.二级分类[i] == '普通债券型' or data_kind.二级分类[i] == '纯债'):
                    result_hc = max_down_fund_qz('CBA00203', begin_date, end_date)
                    result_nhbd = zhoubodong_qz('CBA00203', begin_date, end_date)
                    if (math.isnan(result_nhbd)):
                        result_nhbd = 0
                    result_hb = fund_performance_qz('CBA00203', begin_date, end_date)
                    rec = []
                    rec.append(data_kind.基金代码[i])
                    rec.append(data_kind.基金简称[i])
                    rec.append(data_kind.二级分类[i])
                    rec.append(fund_manager.values[j][1])
                    rec.append(result_hc)
                    rec.append(result_nhbd)
                    rec.append(result_hb)
                    rec.append(fund_manager.values[j][2])
                    rec.append(fund_manager.values[j][3])
                    rec.append('CBA00203')
                    rec.append(fund_manager.values[j][4])
                    rec.append(trade_date)
                    insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                 "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                    cu1.execute(insert_sql, rec)
                elif (data_kind.二级分类[i] == '股票型' or data_kind.二级分类[i] == '激进配置型' or
                      data_kind.二级分类[i] == '标准配置型' or data_kind.二级分类[i] == '保守配置型' or data_kind.二级分类[i] == '灵活配置型'):
                    code_list = ['000300', '000905', '000906', 'CBA00203']
                    for index in range(len(code_list)):
                        if (code_list[index] == 'CBA00203'):
                            result_hc = max_down_fund_qz(code_list[index], begin_date, end_date)
                            result_nhbd = zhoubodong_qz(code_list[index], begin_date, end_date)
                            if (math.isnan(result_nhbd)):
                                result_nhbd = 0
                            result_hb = fund_performance_qz(code_list[index], begin_date, end_date)
                        else:
                            result_hc = max_down_fund_zh(code_list[index], begin_date, end_date)
                            result_nhbd = zhoubodong_zh(code_list[index], begin_date, end_date)
                            if (math.isnan(result_nhbd)):
                                result_nhbd = 0
                            result_hb = fund_performance_zh(code_list[index], begin_date, end_date)
                        rec = []
                        rec.append(data_kind.基金代码[i])
                        rec.append(data_kind.基金简称[i])
                        rec.append(data_kind.二级分类[i])
                        rec.append(fund_manager.values[j][1])
                        rec.append(result_hc)
                        rec.append(result_nhbd)
                        rec.append(result_hb)
                        rec.append(fund_manager.values[j][2])
                        rec.append(fund_manager.values[j][3])
                        rec.append(code_list[index])
                        rec.append(fund_manager.values[j][4])
                        rec.append(trade_date)
                        insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                     "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                        cu1.execute(insert_sql, rec)
                else:
                    result_hc = max_down_fund_zh('000300', begin_date, end_date)
                    result_nhbd = zhoubodong_zh('000300', begin_date, end_date)
                    if (math.isnan(result_nhbd)):
                        result_nhbd = 0
                    result_hb = fund_performance_zh('000300', begin_date, end_date)
                    rec = []
                    rec.append(data_kind.基金代码[i])
                    rec.append(data_kind.基金简称[i])
                    rec.append(data_kind.二级分类[i])
                    rec.append(fund_manager.values[j][1])
                    rec.append(result_hc)
                    rec.append(result_nhbd)
                    rec.append(result_hb)
                    rec.append(fund_manager.values[j][2])
                    rec.append(fund_manager.values[j][3])
                    rec.append('000300')
                    rec.append(fund_manager.values[j][4])
                    rec.append(trade_date)
                    insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                 "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                    cu1.execute(insert_sql, rec)
                fund_db_pra.commit()


        # 计算任期
        trade_date = '0'
        begin_date = fund_manager.values[j][2]
        manager_enddate = fund_manager.values[j][3]
        end_date = manager_enddate


        record_sql = '''select manager_enddate from FUND_BENCHMARK_PERFORMANCE_bak t where t.fundcode= '%(fund_code)s'
                            and t.fundmanager= '%(fundmanager)s' and trade_date ='%(trade_date)s' ''' % {
            'fund_code': fund_benchmark, 'fundmanager': fund_manager.values[j][1], 'trade_date': trade_date}
        manager_performance = pd.DataFrame(cu1.execute(record_sql).fetchall())

        flag = manager_performance.empty
        if (get_tradedate(begin_date) != 0):
            begin_date = get_tradedate(begin_date)
        if (get_tradedate(end_date) != 0):
            end_date = get_tradedate(end_date)

        if (not flag):
            print('任期已计算')

        else:
            result_hc = max_down_fund(benchmark_code, begin_date, end_date)
            result_nhbd = zhoubodong(benchmark_code, begin_date, end_date)
            if (math.isnan(result_nhbd)):
                result_nhbd = 0
            result_hb = fund_performance(benchmark_code, begin_date, end_date)
            rec = []
            rec.append(data_kind.基金代码[i])
            rec.append(data_kind.基金简称[i])
            rec.append(data_kind.二级分类[i])
            rec.append(fund_manager.values[j][1])
            rec.append(result_hc)
            rec.append(result_nhbd)
            rec.append(result_hb)
            rec.append(fund_manager.values[j][2])
            rec.append(fund_manager.values[j][3])
            rec.append(benchmark_code)
            rec.append(fund_manager.values[j][4])
            rec.append(trade_date)
            insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                         "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
            cu1.execute(insert_sql, rec)

            if (data_kind.二级分类[i] == '可转债'):
                result_hc = max_down_fund_zh('000832', begin_date, end_date)
                result_nhbd = zhoubodong_zh('000832', begin_date, end_date)
                if (math.isnan(result_nhbd)):
                    result_nhbd = 0
                result_hb = fund_performance_zh('000832', begin_date, end_date)
                rec = []
                rec.append(data_kind.基金代码[i])
                rec.append(data_kind.基金简称[i])
                rec.append(data_kind.二级分类[i])
                rec.append(fund_manager.values[j][1])
                rec.append(result_hc)
                rec.append(result_nhbd)
                rec.append(result_hb)
                rec.append(fund_manager.values[j][2])
                rec.append(fund_manager.values[j][3])
                rec.append('000832')
                rec.append(fund_manager.values[j][4])
                rec.append(trade_date)
                insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                             "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                cu1.execute(insert_sql, rec)
            elif (data_kind.二级分类[i] == '激进债券型' or data_kind.二级分类[i] == '普通债券型' or data_kind.二级分类[i] == '纯债'):
                result_hc = max_down_fund_qz('CBA00203', begin_date, end_date)
                result_nhbd = zhoubodong_qz('CBA00203', begin_date, end_date)
                if (math.isnan(result_nhbd)):
                    result_nhbd = 0
                result_hb = fund_performance_qz('CBA00203', begin_date, end_date)
                rec = []
                rec.append(data_kind.基金代码[i])
                rec.append(data_kind.基金简称[i])
                rec.append(data_kind.二级分类[i])
                rec.append(fund_manager.values[j][1])
                rec.append(result_hc)
                rec.append(result_nhbd)
                rec.append(result_hb)
                rec.append(fund_manager.values[j][2])
                rec.append(fund_manager.values[j][3])
                rec.append('CBA00203')
                rec.append(fund_manager.values[j][4])
                rec.append(trade_date)
                insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                             "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                cu1.execute(insert_sql, rec)
            elif (data_kind.二级分类[i] == '股票型' or data_kind.二级分类[i] == '激进配置型' or
                  data_kind.二级分类[i] == '标准配置型' or data_kind.二级分类[i] == '保守配置型' or data_kind.二级分类[i] == '灵活配置型'):
                code_list = ['000300', '000905', '000906', 'CBA00203']
                for index in range(len(code_list)):
                    if (code_list[index] == 'CBA00203'):
                        result_hc = max_down_fund_qz(code_list[index], begin_date, end_date)
                        result_nhbd = zhoubodong_qz(code_list[index], begin_date, end_date)
                        if (math.isnan(result_nhbd)):
                            result_nhbd = 0
                        result_hb = fund_performance_qz(code_list[index], begin_date, end_date)
                    else:
                        result_hc = max_down_fund_zh(code_list[index], begin_date, end_date)
                        result_nhbd = zhoubodong_zh(code_list[index], begin_date, end_date)
                        if (math.isnan(result_nhbd)):
                            result_nhbd = 0
                        result_hb = fund_performance_zh(code_list[index], begin_date, end_date)
                    rec = []
                    rec.append(data_kind.基金代码[i])
                    rec.append(data_kind.基金简称[i])
                    rec.append(data_kind.二级分类[i])
                    rec.append(fund_manager.values[j][1])
                    rec.append(result_hc)
                    rec.append(result_nhbd)
                    rec.append(result_hb)
                    rec.append(fund_manager.values[j][2])
                    rec.append(fund_manager.values[j][3])
                    rec.append(code_list[index])
                    rec.append(fund_manager.values[j][4])
                    rec.append(trade_date)
                    insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                 "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                    cu1.execute(insert_sql, rec)
            else:
                result_hc = max_down_fund_zh('000300', begin_date, end_date)
                result_nhbd = zhoubodong_zh('000300', begin_date, end_date)
                if (math.isnan(result_nhbd)):
                    result_nhbd = 0
                result_hb = fund_performance_zh('000300', begin_date, end_date)
                rec = []
                rec.append(data_kind.基金代码[i])
                rec.append(data_kind.基金简称[i])
                rec.append(data_kind.二级分类[i])
                rec.append(fund_manager.values[j][1])
                rec.append(result_hc)
                rec.append(result_nhbd)
                rec.append(result_hb)
                rec.append(fund_manager.values[j][2])
                rec.append(fund_manager.values[j][3])
                rec.append('000300')
                rec.append(fund_manager.values[j][4])
                rec.append(trade_date)
                insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                             "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                cu1.execute(insert_sql, rec)
            fund_db_pra.commit()


cu.close()
cu1.close()
fund_db_pra.close()
fund_db_wind.close()
endtime = datetime.datetime.now()
print((endtime - starttime).seconds)
