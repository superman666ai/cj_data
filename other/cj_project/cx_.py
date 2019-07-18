#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/8 16:55
# @Author  : GaoJian
# @File    : cx_.py

import pandas as pd
import numpy as np
from datetime import datetime
from sql import Sql_oracle
from sql import *

SQL_INDEX_INSERT =  "INSERT INTO fund_index_info( fundId, indexCode, indexValue, reportdate, algid, batchno,  createUser, createDate ) VALUES(:fundId, :indexCode, :indexValue,:reportdate, :algid, :batchno, :createuser, :createDate)"


class DBSession(object):
    def __init__(self, sql_string='', batch_size=500):
        self.cu_wind = sql_oracle.cu
        self.cu_pra = sql_oracle.cu_pra_sel
        self.fund_db_pra = sql_oracle.fund_db_pra
        self.rec_list = []
        self.sql_string = sql_string
        self.batch_size = 500

    def __del__(self):
        self.finish()

    def finish(self):
        if self.rec_list:
            self.db_insert(self.sql_string, self.rec_list)
            self.rec_list = []
        return

    def add_info(self, rec):
        self.rec_list.append(rec)
        if len(self.rec_list) > self.batch_size:
            self.db_insert(self.sql_string, self.rec_list)
            self.rec_list = []
        return

    def db_insert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            #其他错误处理
            raise(e)

    def DBInsert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            #其他错误处理
            raise(e)




class ChenXingEva(object):

    def load_his_data(self):
        """读取历史评级"""
        star = pd.read_excel("历史星级.xlsx", dtype={'基金代码': str})
        star = star.fillna(0)
        return star


    def chen_xing_eva(self):
        star = self.load_his_data()

        star_5 = star[star.iloc[:, -1] == 11111]
        star_5 = star_5.fillna(0)
        for i in range(len(star_5)):
            index = star_5.index[i]
            l2 = star_5.iloc[i, -24:-1].mean()
            l3 = star_5.iloc[i, -36:-1].mean()
            l4 = star_5.iloc[i, -48:-1].mean()
            l5 = star_5.iloc[i, -60:-1].mean()
            if l2 == 11111:
                star_5.loc[index, '连续两年晨星5星（3年）'] = '连续两年晨星5星（3年）'
            if l3 == 11111:
                star_5.loc[index, '连续三年晨星5星（3年）'] = '连续三年晨星5星（3年）'
            if l4 == 11111:
                star_5.loc[index, '连续四年晨星5星（3年）'] = '连续四年晨星5星（3年）'
            if l5 == 11111:
                star_5.loc[index, '连续五年晨星5星（3年）'] = '连续五年晨星5星（3年）'
        if '连续五年晨星5星（3年）' in star_5.columns:
            pass
        else:
            star_5['连续五年晨星5星（3年）'] = np.nan
        col = star.columns[-1]
        star.loc[star[col] == 1, '晨星3年评级'] = '晨星3年评级1星'
        star.loc[star[col] == 11, '晨星3年评级'] = '晨星3年评级2星'
        star.loc[star[col] == 111, '晨星3年评级'] = '晨星3年评级3星'
        star.loc[star[col] == 1111, '晨星3年评级'] = '晨星3年评级4星'
        star.loc[star[col] == 11111, '晨星3年评级'] = '晨星3年评级5星'


        # 选取所有基金 成立年限大于三年的基金

        Agency_rating_ms = pd.merge(star, star_5, on='基金代码', how='left')


        # 当前算法id
        algid = '0000000003'
        # 当天日期
        todaydate = datetime.now().strftime('%Y%m%d')
        # 当前文件名称
        import os
        thisfilename = os.path.basename(__file__)

        db_insert_session = DBSession(SQL_INDEX_INSERT)


        rec = []
        # 评级 标签入库
        pdData = Agency_rating_ms[['基金代码', '连续两年晨星5星（3年）']]
        rec.clear()
        for value in pdData.values:
            print((value[0], '0004001004', str(value[1]), None, algid, "20190331", thisfilename, todaydate))

        """
              rec = (
                  code, 'AlphaCategroyBenchmarkAll', str(rst_value), None, self.algid, report_date, self.thisfilename,
                  self.todaydate)
              """


        # DBInsert(sql_tag_insert, rec)
        #
        # pdData = Agency_rating_ms[['基金代码', '连续三年晨星5星（3年）']]
        # rec.clear()
        # for value in pdData.values:
        #     rec.append((value[0], '0004001005', str(value[1]), algid, batchno, 'Y', thisfilename, todaydate))
        # DBInsert(sql_tag_insert, rec)
        #
        # pdData = Agency_rating_ms[['基金代码', '连续四年晨星5星（3年）']]
        # rec.clear()
        # for value in pdData.values:
        #     rec.append((value[0], '0004001006', str(value[1]), algid, batchno, 'Y', thisfilename, todaydate))
        # DBInsert(sql_tag_insert, rec)
        #
        # pdData = Agency_rating_ms[['基金代码', '连续五年晨星5星（3年）']]
        # rec.clear()
        # for value in pdData.values:
        #     rec.append((value[0], '0004001007', str(value[1]), algid, batchno, 'Y', thisfilename, todaydate))
        # DBInsert(sql_tag_insert, rec)
        #
        # pdData = Agency_rating_ms[['基金代码', '晨星3年评级']]
        # rec.clear()
        # for value in pdData.values:
        #     rec.append((value[0], '0004001001', str(value[1]), algid, batchno, 'Y', thisfilename, todaydate))
        # DBInsert(sql_tag_insert, rec)


if __name__ == '__main__':

    a = ChenXingEva()
    a.chen_xing_eva()


