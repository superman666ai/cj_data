#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/18 10:10
# @Author  : GaoJian
# @File    : qingpan_data.py

from sql_con import *
from func_tools import *
import pandas as pd
from zz_script_hub.season_label import *


SQL_INDEX = """
                    INSERT INTO tb_qingpan_fund
                    ( FUNDID, name, start_date, end_date, fundreturn, maxdrawdown) 
                    VALUES(:fundId, :name, :start_date,:end_date,:fundreturn,:maxdrawdown)"""
db_insert_session = DBSession(SQL_INDEX)

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



def get_tradedate(zrr):
    cu1.execute('select jyr from pra_info.txtjyr t where t.zrr=:rq', rq=zrr)
    rs = cu1.fetchall()[0][0]
    return rs


def main():
    sql = '''select substr(t2.f_info_windcode, 0, 6),
          t2.f_info_name,
          '已清盘',
          t2.f_info_setupdate,
          t1.f3_1272 , t1.f4_1272
          from wind.tb_object_1272 t1, wind.CHINAMUTUALFUNDDESCRIPTION t2,windcustomcode t3 
          where t1.f1_1272 = t3.s_info_asharecode and t3.s_info_windcode=t2.f_info_windcode
          and t2.f_info_maturitydate is not null and t2.f_info_maturitydate<=to_char(sysdate,'yyyymmdd')

      '''
    df = pd.DataFrame(sql_oracle.cu_pra_sel.execute(sql).fetchall(),
                      columns=['fundid', 'c_name', 'type', 'clr', 'start_date', 'end_date'])

    for index, row in df.iterrows():
        try:

            start_date = get_tradedate(row.start_date)
            end_date = get_tradedate(row.end_date)

            fundid = '260103'
            start_date = get_tradedate('20150101')
            end_date = get_tradedate('20151231')


            # 区间收益
            fundreturn = interval_profit(fundid, start_date, end_date)[1]
            # # 最大回撤
            maxdrawdown = max_draw_down(fundid, start_date, end_date)

            print(fundreturn, '------', maxdrawdown)

            rec = (row.fundid, row.c_name, row.start_date, row.end_date, fundreturn, maxdrawdown)
            print(rec)
            # FUNDID, TRADEDATE, rptcycle, timeoptabilityhs300, INDUSTRYABILITYHS300

            # db_insert_session.add_info(rec)
        except Exception as e:
            print(e)
            continue
        break
    # db_insert_session.finish()


if __name__ == '__main__':
    main()
