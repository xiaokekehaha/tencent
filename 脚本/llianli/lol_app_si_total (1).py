#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_si_total.py
# 功能描述:     lolapp每日启动情况的分布
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


     ##si的平均数据
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_si_ana
(
fdate INT,
id INT,
ieffectflag INT,
ilostflag INT,
sessionnum BIGINT ,
uv BIGINT
)
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##将每日的数据配置写入表中
    sql = ''' 
    INSERT OVERWRITE TABLE tb_lol_app_si_ana
SELECT
t1.fdate AS fdate,
t1.id AS id,
CASE WHEN GROUPING(t.ieffectflag) = 1 THEN -100 ELSE t.ieffectflag END AS ieffectflag,
CASE WHEN GROUPING(t.ilostflag) = 1 THEN -100 ELSE t.ilostflag END AS ilostflag,
COUNT(DISTINCT t1.si) AS sessionnum ,
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
si,
uin_mac
FROM 
tb_lol_app_si WHERE  fdate >= 20151002 AND fdate <= 20151031
)t1
ON (t.id = t1.id AND t.uin_mac = t1.uin_mac)
GROUP BY t1.fdate,t1.id,cube(t.ieffectflag,t.ilostflag)          

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##启动次数的分布
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_si_range_ana
(
fdate INT,
id INT,
ieffectflag INT,
ilostflag INT,
use_times STRING ,
uv BIGINT
)
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##将每日的数据配置写入表中
    sql = ''' 
    INSERT OVERWRITE TABLE tb_lol_app_si_range_ana
SELECT
fdate,
id,
ieffectflag,
ilostflag,
use_times,
COUNT(DISTINCT uin_info) AS uin_cnt
FROM
(
SELECT
fdate,
id,
ieffectflag,
uin_info,
ilostflag,
CASE 
    WHEN sessionnum = 1 THEN '1次'
    WHEN sessionnum = 2 THEN '2次'
    WHEN sessionnum >= 3  AND  sessionnum <= 5 THEN '3-5次'
    WHEN sessionnum >= 6  AND  sessionnum <= 9 THEN '6-9次'
    WHEN sessionnum >= 10  AND  sessionnum <= 19 THEN '10-19次'
    ELSE  '20次以上'
END AS use_times
FROM
(
SELECT
t1.fdate AS fdate,
t1.id AS id,
CASE WHEN GROUPING(t.ieffectflag) = 1 THEN -100 ELSE t.ieffectflag END AS ieffectflag,
CASE WHEN GROUPING(t.ilostflag) = 1 THEN -100 ELSE t.ilostflag END AS ilostflag,
t.uin_info AS uin_info,
COUNT(DISTINCT t1.si) AS sessionnum 
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
si,
uin_mac
FROM 
tb_lol_app_si WHERE  fdate >= 20151002 AND fdate <= 20151031
)t1
ON (t.id = t1.id AND t.uin_mac = t1.uin_mac)
GROUP BY t1.fdate,t1.id,cube(t.ieffectflag,t.ilostflag),t.uin_info
)t2
)t3 
GROUP BY fdate,id,ieffectflag,ilostflag,use_times    

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    