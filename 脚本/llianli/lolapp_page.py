#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lolapp_page.py
# 功能描述:     lolfapp每日访问页面情况
# 输入参数:     yyyymmdd    例如：20151223
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-23
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
      CREATE TABLE IF NOT EXISTS tb_lol_app_page_view
(
fdate INT,
id INT,
pi STRING,
uin_mac STRING,
pv BIGINT,
du BIGINT
)
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_lol_app_page_view WHERE  fdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据配置写入表中
    sql = ''' 
                   INSERT TABLE tb_lol_app_page_view
SELECT
%s AS fdate,
id,
pi,
uin_info,
COUNT(*) AS pv,
SUM(du) AS du
FROM
(
SELECT 
id,
pi,
concat(ui,mc) AS uin_info,
du
FROM  teg_mta_intf::ieg_lol WHERE sdate = %s  AND id in (1100678382,1200678382) 
and  pi != '-' 
)t
WHERE du <= 21600
GROUP BY id,pi,uin_info


    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    