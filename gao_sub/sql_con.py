#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/8 14:07
# @Author  : GaoJian
# @File    : sql_con.py


import cx_Oracle
from other.functions import single


@single
class Sql_oracle:
    """sql工具"""

    def __init__(self):
        self.connect_db()

    def connect_db(self):
        """链接db"""
        # wind 库
        [userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
        self.fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
        self.cu = self.fund_db.cursor()

        # pra库
        # 本地
        # '172.16.126.23:1521'
        # 线上
        # '172.16.125.222'

        [userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
        self.fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
        self.cu_pra_sel = self.fund_db_pra.cursor()

    def close(self):
        """断开链接"""
        self.cu.close()
        self.cu_pra_sel.close()

        self.fund_db.close()
        self.fund_db_pra.close()

    def sql_cu(self, sql):
        """
        wind库的查询方法
        :param sql: sqk语句
        :return: 二维数组
        """
        res = self.cu.execute(sql).fetchall()
        return res

    def sql_pra(self, sql):
        """
        pra库的查询方法
        :param sql: sqk语句
        :return: 二维数组
        """
        res = self.cu_pra_sel.execute(sql).fetchall()
        return res

    def get_value_fund(self, code, start, end):
        pass

    def __del__(self):
        print('退出，断开数据库')
        self.close()



sql_oracle = Sql_oracle()




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
            # print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            # 其他错误处理
            raise (e)

    def DBInsert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            # print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            # 其他错误处理
            raise (e)

