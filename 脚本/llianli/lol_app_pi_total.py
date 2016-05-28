#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_pi_total.py
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



 ##所有页面的平均访问情况
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_pi_total_ana
(
fdate INT,
id INT,
ieffectflag INT,
ilostflag INT,
uv BIGINT,
pv BIGINT ,
du BIGINT
)

                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    sql = ''' 
         
INSERT OVERWRITE TABLE tb_lol_app_pi_total_ana
SELECT 
t1.fdate AS fdate,
t1.id AS id,
CASE WHEN GROUPING(t.ieffectflag) = 1 THEN -100 ELSE t.ieffectflag END AS ieffectflag,
CASE WHEN GROUPING(t.ilostflag) = 1 THEN -100 ELSE t.ilostflag END AS ilostflag,
COUNT(DISTINCT t.uin_info) AS uv,
SUM(t1.pv) AS pv,
SUM(t1.du) AS du 
FROM
(
SELECT
DISTINCT
ieffectflag,
ilostflag,
uin_mac,
iuin,
concat(uin_mac,iuin) AS uin_info,
id
FROM
tb_lol_app_effect_static_uin_mac WHERE uin_mac IS NOT NULL
) t
JOIN
(
SELECT
fdate,
id,
pi,
uin_mac,
pv,
du
FROM 
 tb_lol_app_page_view WHERE fdate >= 20151002 AND fdate <= 20151031
 )t1
 ON (t.id = t1.id AND t.uin_mac = t1.uin_mac)
 GROUP BY t1.fdate,t1.id,cube(t.ieffectflag,t.ilostflag)
    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    #访问页面的个数
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_pi_num_ana
(
fdate INT,
id INT,
ieffectflag INT,
ilostflag INT,
uv BIGINT,
page_num BIGINT 
)


                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    sql = ''' 
         INSERT OVERWRITE TABLE tb_lol_app_pi_num_ana
SELECT 
t1.fdate AS fdate,
t1.id AS id,
CASE WHEN GROUPING(t.ieffectflag) = 1 THEN -100 ELSE t.ieffectflag END AS ieffectflag,
CASE WHEN GROUPING(t.ilostflag) = 1 THEN -100 ELSE t.ilostflag END AS ilostflag,
COUNT(DISTINCT t.uin_info) AS uv,
SUM(page_num) AS page_num
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
uin_mac,
COUNT(DISTINCT pi) AS page_num 
FROM 
 tb_lol_app_page_view WHERE  fdate >= 20151002 AND fdate <= 20151031
 GROUP BY fdate,id,uin_mac
)t1
ON (t.id = t1.id AND t.uin_mac = t1.uin_mac)
GROUP BY t1.fdate,t1.id,cube(t.ieffectflag,t.ilostflag)

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    


     ##每个页面的使用情况
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_pi_ana
(
fdate INT,
id INT,
pi STRING,
ieffectflag INT,
ilostflag INT,
uv BIGINT,
pv BIGINT ,
du BIGINT
)

                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    sql = ''' 
   INSERT OVERWRITE TABLE tb_lol_app_pi_ana
SELECT 
t1.fdate AS fdate,
t1.id AS id,
t1.pi AS pi,
t.ieffectflag AS ieffectflag,
t.ilostflag AS ilostflag,
COUNT(DISTINCT t.uin_info) AS uv,
SUM(t1.pv) AS pv,
SUM(t1.du) AS du 
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
pi,
uin_mac,
pv,
du
FROM 
 tb_lol_app_page_view WHERE fdate >= 20151002 AND  fdate <= 20151031
 )t1
 ON (t.id = t1.id AND t.uin_mac = t1.uin_mac)
 GROUP BY t1.fdate,t1.id,t1.pi,t.ieffectflag,t.ilostflag        

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    