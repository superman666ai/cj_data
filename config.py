#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/8 13:47
# @Author  : GaoJian
# @File    : config.py


class Config:
    sql_tag_insert = "INSERT INTO fund_tag_info( fundId, tagId, tagValue, algid, batchno, endableFlag, createUser, createDate ) VALUES(:fundId, :tagId, :tagValue, :algid, :batchno, :endableFlag, :createuser, :createDate)"

    SQL_INDEX_INSERT = "INSERT INTO fund_index_info( fundId, indexCode, indexValue, reportdate, algid, batchno,  createUser, createDate ) VALUES(:fundId, :indexCode, :indexValue,:reportdate, :algid, :batchno, :createuser, :createDate)"

    # net_act
    DOWN_STD_FILTER = ['标准配置型', '可转债型', '环球股票', '普通债券型', '股票型', '纯债型', '灵活配置型', '激进配置型', '激进债券型', '保守配置型', '沪港深股票型',
                       '沪港深配置型', ' 纯债型']
    # net_act_plus
    ALPHA_FILTER = ['标准配置型', '可转债型', '环球股票', '普通债券型', '股票型', '纯债型', '灵活配置型', '激进配置型', '激进债券型', '保守配置型', '沪港深股票型',
                    '沪港深配置型',
                    ' 纯债型']

    ALPHA_FILTER_TMP = ['纯债型']

    # 杠杆率过滤条件
    LVEREAGE_FILTER = ['保守配置型', '纯债型', '激进债券型', '普通债券型', '可转债型', ' 纯债型']
