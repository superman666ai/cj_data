#####我的算法
import pandas as pd
import os
import numpy as np
import cx_Oracle
import time
from functools import reduce
from datetime import timedelta
from dateutil import rrule
import datetime
from scipy import stats
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression
from dateutil.parser import parse
import math
import calendar


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
[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db_wind = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)

# 当前文件名称
thisfilename = os.path.basename(__file__)
algid = '0000000031'
dblink_dict = {}
dblink_dict['wind'] = fund_db_wind
dblink_dict['投研'] = fund_db_pra
cu = fund_db_wind.cursor()
cu1 = fund_db_pra.cursor()

def max_down_fund(code='163807', start_date='20150528', end_date='20190225'):
    judgesql = '''
        select
        f13_1101 as 截止日期, f21_1101 as 复权单位净值 
        from
        wind.tb_object_1101
        left join wind.tb_object_1090
        on f2_1090 = f14_1101
        where 
        F16_1090= '%(code)s'
        and
        (F13_1101 = '%(start_date)s'
        or
        f13_1101 = '%(end_date)s')
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    judgeresult = pd.DataFrame(cu.execute(judgesql).fetchall(), columns=['截止日期','复权单位净值'])
    if (len(judgeresult)<2):
        return 100
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
    f13_1101 as 截止日期, f21_1101 as 复权单位净值 
    from
    wind.tb_object_1101
    left join wind.tb_object_1090
    on f2_1090 = f14_1101
    where 
    F16_1090= '%(code)s'
    and
    F13_1101 >= '%(start_date)s'
    and
    f13_1101 <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 100
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)
    price_list = fund_price2['复权单位净值'].tolist()
    i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
    if i == 0:
        max_down_value = 0
    else:
        j = np.argmax(price_list[:i])  # 开始位置
        max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
    return -max_down_value

def zhoubodong(code='163807', start_date='20190101', end_date='20190225'):
    judgesql = '''
        select
        f13_1101 as 截止日期, f21_1101 as 复权单位净值 
        from
        wind.tb_object_1101
        left join wind.tb_object_1090
        on f2_1090 = f14_1101
        where 
        F16_1090= '%(code)s'
        and
        (F13_1101 = '%(start_date)s'
        or
        f13_1101 = '%(end_date)s')
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    judgeresult = pd.DataFrame(cu.execute(judgesql).fetchall(), columns=['截止日期','复权单位净值'])
    if (len(judgeresult)<2):
        return 100
    sql = '''
    select
    f13_1101 as 截止日期, f21_1101 as 复权单位净值 
    from
    wind.tb_object_1101
    left join wind.tb_object_1090
    on f2_1090 = f14_1101
    where 
    F16_1090= '%(code)s'
    and
    F13_1101 >= '%(start_date)s'
    and
    f13_1101 <= '%(end_date)s'
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

def fund_performance(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
    f13_1101 as 截止日期, f21_1101 as 复权单位净值 
    from
    wind.tb_object_1101
    left join wind.tb_object_1090
    on f2_1090 = f14_1101
    where 
    F16_1090= '%(code)s'
    and
   (F13_1101 = '%(start_date)s'
    or
    f13_1101 = '%(end_date)s') order by F13_1101 desc
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (len(fund_price)<2):
        return 100
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance

def get_max_down_rank(code = '163807', start_date = '20150528', end_date = '20190225'):
    # 函数作用是提取区间内基金的最大回撤排名的数值
    # 提取同类型基金代码
    fund_kind  = data_kind[data_kind.基金代码==code]['二级分类'].tolist()[0]
    data_kind2 = data_kind[data_kind.二级分类==fund_kind].reset_index(drop=True)
    for i in data_kind2.index:
        try:
            max_down_value = max_down_fund(data_kind2.基金代码[i],start_date,end_date)
            if(max_down_value<100):
                data_kind2.loc[i,'最大回撤'] = max_down_value
        except:
            pass
    data_kind2=data_kind2.dropna(axis=0).reset_index(drop=True)
    data_kind2['排名']=data_kind2['最大回撤'].rank(ascending=True, method = 'max')/len(data_kind2['最大回撤'])
    recorddate=data_kind2[data_kind2['基金代码']==code]['最大回撤'].tolist()
    if (len(recorddate) == 0):
        return 0, 0
    else:
        return data_kind2[data_kind2['基金代码']==code]['最大回撤'].tolist()[0],(1-data_kind2[data_kind2['基金代码']==code]['排名'].tolist()[0])

def shouyi_rate_rank(code = '001677',start_date = '20150528', end_date = '20190225'):
    # print(code,start_date,end_date)
    fund_kind  = data_kind[data_kind.基金代码==code]['二级分类'].tolist()[0]

    print(data_kind.head())
    print('++++++', data_kind.index)

    data_kind2 = data_kind[data_kind.二级分类==fund_kind].reset_index(drop=True)

    print(data_kind2.head())

    print('--------------',  data_kind2.index)

    for i in data_kind2.index:
        try:
            shouyi_rate_value = fund_performance(data_kind2.基金代码[i],start_date,end_date)
            if(shouyi_rate_value<100):
                data_kind2.loc[i,'收益率'] = shouyi_rate_value
        except:
            pass
    data_kind2=data_kind2.dropna(axis=0).reset_index(drop=True)
    data_kind2['排名']=data_kind2['收益率'].rank(ascending=False, method = 'min')/len(data_kind2['收益率'])
    print('同类个数',len(data_kind2['收益率']))

    recorddate=data_kind2[data_kind2['基金代码']==code]['收益率'].tolist()

    return data_kind2[data_kind2['基金代码']==code]['收益率'].tolist()[0],data_kind2[data_kind2['基金代码']==code]['排名'].tolist()[0]

def zhoubodong_rank(code = '163807', start_date = '20150528', end_date = '20190225'):
    # 函数作用是提取区间内基金的波动率排名的数值
    # 提取同类型基金代码
    # print(code,start_date,end_date)
    fund_kind  = data_kind[data_kind.基金代码==code]['二级分类'].tolist()[0]
    data_kind2 = data_kind[data_kind.二级分类==fund_kind].reset_index(drop=True)
    for i in data_kind2.index:
        try:
            zhoubodong_value = zhoubodong(data_kind2.基金代码[i],start_date,end_date)
            if(zhoubodong_value<100):
                data_kind2.loc[i,'波动率年化'] = zhoubodong_value
        except:
            pass
    data_kind2=data_kind2.dropna(axis=0).reset_index(drop=True)
    data_kind2['排名']=data_kind2['波动率年化'].rank(ascending=True, method = 'min')/len(data_kind2['波动率年化'])
    # print(len(data_kind2['波动率年化']))
    recorddate=data_kind2[data_kind2['基金代码']==code]['波动率年化'].tolist()
    if(len(recorddate)==0):
        return 0,0
    else:
       return data_kind2[data_kind2['基金代码']==code]['波动率年化'].tolist()[0],data_kind2[data_kind2['基金代码']==code]['排名'].tolist()[0]

def get_tradedate(zrr):
    cu1.execute('select jyr from pra_info.txtjyr t where t.zrr=:rq', rq=zrr)
    try:
        rs = cu1.fetchall()[0][0]
    except:
        rs=0
    return rs


starttime = datetime.datetime.now()
print(starttime)

fund_result = pd.DataFrame(
    columns=['基金代码', '基金简称', '二级分类','基金经理','任职以来最大回撤','任职以来波动率', '任职以来回报'])

from sql_con import *

sql = """select cpdm, jjjc, yjfl, ejfl, clr from fund_classify"""

data = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                       columns=['基金代码','基金简称','一级分类','二级分类','成立日'])



# data = pd.read_excel('D:/分类-0213.xlsx', dtype=str)


data_kind = data[['基金代码','基金简称','一级分类','二级分类','成立日']]
# data_kind = data_kind[(data_kind['一级分类']=='股票型')|(data_kind['一级分类']=='沪港深')|(data_kind['一级分类']=='配置型')|(data_kind['一级分类']=='配置型')|(data_kind['一级分类']=='QDII')]
# data_kind = data_kind[(data_kind['基金代码']=='260103')]

# data_kind = data_kind[(data_kind['二级分类']=='激进配置型')|(data_kind['二级分类']=='保守配置型')|(data_kind['二级分类']=='标准配置型')]
# data_kind = data_kind[(data_kind['二级分类']=='灵活配置型')]
# data_kind = data_kind[(data_kind['一级分类']=='沪港深')]


# for i in data_kind.index:
#     fund_code=data_kind.基金代码[i]
#     if fund_code == "260103":
#         print(fund_code)
#         query_sql = '''SELECT t1.f16_1090, t.f2_1272, nvl(t.f3_1272,0), nvl(t.f4_1272,0), t.F11_1272
#                 from wind.TB_OBJECT_1272 t, wind.tb_object_1090 t1
#                 where t.f1_1272 = t1.f2_1090  and t1.f4_1090 = 'J'
#                    and t1.f16_1090 = '%(fund_code)s'    order by t.f3_1272
#                 '''%{'fund_code':fund_code}
#         fund_manager = pd.DataFrame(cu.execute(query_sql).fetchall())
#         print(fund_manager)
#         for j in range(len(fund_manager.values)):
#             begin_date=fund_manager.values[j][2]
#             end_date = fund_manager.values[j][3]
#             if(end_date=='0' and begin_date[0:4]==(datetime.datetime.now().year)):
#                 print('不计算')
#             else:
#                 if(data_kind.成立日[i]==begin_date):
#                     begin_date=get_tradedate(begin_date)
#                 else:
#                     tansferdate=(datetime.datetime.strptime(begin_date,'%Y%m%d')+timedelta(days=-1)).strftime('%Y%m%d')
#                     if(get_tradedate(tansferdate)==0):
#                         begin_date=tansferdate
#                     else:
#                         begin_date=get_tradedate(tansferdate)
#                 if(str(begin_date)[0:4]!=str(end_date)[0:4]):
#                     end_date=get_tradedate(str(begin_date)[0:4]+'1231')
#                 exists_sql = '''select  *  from MANAGER_INFO_RANK_bak t
#                                where t.fundmanager = '%(fundmanager)s' and t.fundcode = '%(fund_code)s' and t.cycle_value = '%(cycle_value)s'
#                             ''' % {'fundmanager': fund_manager.values[j][1],'fund_code': fund_code,'cycle_value': str(begin_date)[0:4]}
#                 if(len(cu1.execute(exists_sql).fetchall())==0):
#                     print('11111'+str(end_date))
#                     try:
#                         result_hc = get_max_down_rank(fund_manager.values[j][0], begin_date, end_date)
#                         result_nhbd = zhoubodong_rank(fund_manager.values[j][0], begin_date, end_date)
#                         result_hb = shouyi_rate_rank(fund_manager.values[j][0], begin_date, end_date)
#                         print(result_hc)
#                         print(result_nhbd)
#                         print(result_hb)
#                         rrin_hc = result_hc[0]
#                         rrin_hc_rank = result_hc[1]
#                         rrin_nhbd = result_nhbd[0]
#                         rrin_nhbd_rank = result_nhbd[1]
#                         rrin_hb = result_hb[0]
#                         rrin_hb_rank = result_hb[1]
#                         rec=[]
#                         rec.append(data_kind.基金代码[i])
#                         rec.append(data_kind.基金简称[i])
#                         rec.append(data_kind.二级分类[i])
#                         rec.append(fund_manager.values[j][1])
#                         rec.append(0)
#                         rec.append(str(begin_date)[0:4])
#                         rec.append(result_hc[0])
#                         rec.append(result_hc[1])
#                         rec.append(result_nhbd[0])
#                         rec.append(result_nhbd[1])
#                         rec.append(result_hb[0])
#                         rec.append(result_hb[1])
#                         rec.append(fund_manager.values[j][2])
#                         rec.append(fund_manager.values[j][3])
#                         rec.append(data_kind.成立日[i])
#                         rec.append(fund_manager.values[j][4])
#                         insert_sql = "INSERT INTO MANAGER_INFO_RANK_bak( fundcode, fundname,fundtype, fundmanager,cycle_type,cycle_value,rrin_hc,rrin_hc_rank," \
#                                                      "rrin_nhbd,rrin_nhbd_rank,rrin_hb,rrin_hb_rank,manager_startdate,manager_enddate,founddate,fund_manager_id) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)"
#                         cu1.execute(insert_sql, rec)
#                         fund_db_pra.commit()
#                     except:
#                         pass
#                 else:
#                     print('已计算')
cu.close()
cu1.close()
fund_db_pra.close()
fund_db_wind.close()
endtime = datetime.datetime.now()
print((endtime-starttime).seconds)









