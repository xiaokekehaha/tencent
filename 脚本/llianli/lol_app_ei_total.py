#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_ei_total.py
# 功能描述:     lolapp每日访问的事件n数目
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


    #tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    #sDate = argv[0]
    
    

    #tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表写数据
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_ei_ana
(
fdate INT,
id INT,
ei STRING,
ieffectflag INT,
ilostflag INT,
pv BIGINT ,
uv BIGINT
)
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##将每日的数据配置写入表中
    sql = ''' 
     INSERT OVERWRITE TABLE tb_lol_app_ei_ana
SELECT 
t1.fdate AS fdate,
t1.id AS id,
t1.ei AS ei,
t.ieffectflag,
t.ilostflag,
sum(t1.pv) AS pv ,
COUNT(DISTINCT t.uin_info) AS uv 
FROM
(
SELECT
DISTINCT
ieffectflag,
ilostflag,
uin_mac,
iuin,
id,
concat(uin_mac,iuin) AS uin_info
FROM
tb_lol_app_effect_static_uin_mac WHERE uin_mac IS NOT NULL
) t
JOIN
(
SELECT 
fdate,
id,
ei,
uin_mac,
pv
FROM  tb_lol_app_ei WHERE fdate >= 20151002 AND fdate <= 20151031
)t1
on (t.uin_mac = t1.uin_mac and t.id = t1.id)
GROUP BY t1.fdate,t1.id,t1.ei,t.ilostflag,t.ieffectflag               
 
 
    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    