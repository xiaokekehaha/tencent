#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_cal.py
# 功能描述:     sybapp计算
# 输入参数:     yyyymmdd    例如：20151208
# 目标表名:     
# 数据源表:     ieg_tdbank::gqq_dsl_day_login_bill_fht0
# 创建人名:     llianli
# 创建日期:     2015-12-08
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime
import time


def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表写数据
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_syb_month_dau
(
fmonth INT,
uinnum  BIGINT
)
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_syb_month_dau WHERE  fmonth = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##结果写入
    sql = ''' 
    INSERT TABLE tb_syb_month_dau
      SELECT
 %s as statics_date,
 COUNT(DISTINCT isybid)
 FROM 
 ieg_tdbank::gqq_dsl_day_login_bill_fht0
 WHERE tdbank_imp_date BETWEEN '%s0100' AND '%s3123' AND iappid=1 and vdevicetype not like '%%generic%%'

    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    