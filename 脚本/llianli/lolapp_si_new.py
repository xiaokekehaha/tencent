#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lolapp_si.py
# 功能描述:     lolapp每日访问的session数目
# 输入参数:     yyyymmdd    例如：20151208
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_lol
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
      CREATE TABLE IF NOT EXISTS tb_lol_app_si_new
(
fdate INT,
id INT,
si STRING,
uin_mac STRING
)PARTITION BY LIST (fdate)
                (
                partition p_20160108  VALUES IN (20160108),
                partition p_20160109  VALUES IN (20160109)
                ) 

                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql=''' ALTER TABLE   tb_lol_app_si_new DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' ALTER TABLE tb_lol_app_si_new ADD PARTITION p_%s VALUES IN (%s)'''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据配置写入表中
    sql = ''' 
                   INSERT TABLE tb_lol_app_si_new
SELECT
%s AS fdate,
id,
si,
uin_info
FROM
(
SELECT 
id,
si,
concat(ui,mc) AS uin_info
FROM  teg_mta_intf::ieg_lol WHERE sdate = %s  AND id in (1100678382,1200678382) 
AND pi != '-' 
)t
GROUP BY id,si,uin_info

    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    