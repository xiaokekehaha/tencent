#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_statics_temp2.py
# 功能描述:     掌盟临时数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_temp_lol_app_black_battle_info_20160302
# 目标表名:     ieg_qt_community_app.tb_temp_lol_app_normal_battle_info_20160302
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2014-10-29
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== temp = " + 'temp' + " ==")

    ##sDate = argv[0];
    ##sDate = '20141201'

    ##tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)


    
    
    
    sql = """
    INSERT OVERWRITE TABLE tb_lol_app_actionid_table

SELECT 
201603 AS statis_month,
a.sdate AS sdate,
a.uin_mac AS uin_mac,
c.uin AS uin,
a.appid AS appid,
a.ihour AS ihour,
a.ct AS ct ,
b.ei AS ei,
b.pv AS pv,
b.function_time AS function_time,
b.app_time AS app_time,
b.sess_num AS sess_num
FROM 
tb_lol_app_actionid_table_cn_temp  a JOIN tb_lol_app_actionid_table_pi_ei_temp b
ON(a.sdate = b.sdate AND a.uin_mac = b.uin_mac AND a.appid = b.appid AND a.ihour = b.ihour)
JOIN
(
SELECT
fdate,
id,
uin_mac,
uin
FROM 
tb_lol_app_uininfo_new WHERE fdate >= 20160229 AND fdate <= 20160313
)c
ON (a.sdate = c.fdate AND a.appid = c.id AND a.uin_mac = c.uin_mac) 
    """
#    tdw.WriteLog(sql)
#    res = tdw.execute(sql)
    
    
    sql = """ INSERT OVERWRITE TABLE tb_lol_app_uin_action_data_width
SELECT
201603 AS statis_month,
sdate,
appid,
uin,
gender,
 age,
 cagerange,
 profession_id,
 ihour,
 ct,
 ei,
 SUM(pv),
 SUM(function_time),
 SUM(app_time),
 SUM(sess_num)
 FROM
(
SELECT
b.sdate AS sdate,
b.appid AS appid,
a.uin AS uin,
a.gender AS gender,
a.age AS age,
a.cagerange AS cagerange,
a.profession_id AS profession_id,
b.ihour AS ihour,
b.ct AS ct,
b.ei AS ei,
b.pv AS pv,
b.function_time AS function_time,
b.app_time AS app_time,
b.sess_num AS sess_num
FROM tb_lol_app_uin_properties_nature PARTITION (p_201603) a
 JOIN tb_lol_app_actionid_table  PARTITION (p_201603) b 
 ON(a.appid = b.appid AND a.uin = b.iuin)
)t
GROUP BY sdate,
appid,
uin,
gender,
 age,
 cagerange,
 profession_id,
 ihour,
 ct,ei"""
#    tdw.WriteLog(sql)
#    res = tdw.execute(sql)
    
    
    sql = """ INSERT OVERWRITE TABLE tb_lol_app_result_temp_1
SELECT 
sdate,
CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END AS appid,
gender,
cagerange,
ei,
COUNT(DISTINCT uin) AS uv,
SUM(pv) AS pv,
SUM(app_time) AS app_time,
SUM(function_time) AS function_time
FROM tb_lol_app_uin_action_data_width GROUP BY 
 sdate,
cube(appid),
gender,
cagerange,
ei
"""

 #   tdw.WriteLog(sql)
 #   res = tdw.execute(sql)
    
    
    sql = """ 
    INSERT OVERWRITE TABLE tb_lol_app_result_temp_2
SELECT
sdate,
CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END AS appid,
shour,
CASE WHEN GROUPING(ct) = 1 THEN '-100' ELSE ct END AS ct,
ei,
COUNT(DISTINCT uin) AS uv,
SUM(pv) AS pv,
SUM(app_time) AS app_time,
SUM(function_time) AS function_time
FROM
( 
SELECT
sdate,
appid,
CASE 
    WHEN CAST(ihour AS INT) >= 5 AND CAST(ihour AS INT) <= 8 THEN '5-8'  
    WHEN CAST(ihour AS INT) >= 9 AND CAST(ihour AS INT) <= 12 THEN '9-12'
    WHEN CAST(ihour AS INT) >= 13 AND CAST(ihour AS INT) <= 16 THEN '13-16'
    WHEN CAST(ihour AS INT) >= 17 AND CAST(ihour AS INT) <= 20 THEN '17-20'
    WHEN CAST(ihour AS INT) IN (21,22,23,0) THEN '21-24'
    WHEN CAST(ihour AS INT) IN (1,2,3,4) THEN '1-4'
END AS shour,
ct,
ei,
uin,
pv,
app_time,
function_time
FROM tb_lol_app_uin_action_data_width
)t GROUP BY sdate,cube(appid,ct),shour,ei
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT OVERWRITE TABLE tb_lol_app_result_temp_3
SELECT
sdate,
gender,
cagerange,
shour,
CASE WHEN GROUPING(ct) = 1 THEN '-100' ELSE ct END AS ct,
ei,
COUNT(DISTINCT uin) AS uv,
SUM(pv) AS pv,
SUM(app_time) AS app_time,
SUM(function_time) AS function_time
FROM
( 
SELECT
sdate,
appid,
gender,
cagerange,
CASE 
    WHEN CAST(ihour AS INT) >= 5 AND CAST(ihour AS INT) <= 8 THEN '5-8'  
    WHEN CAST(ihour AS INT) >= 9 AND CAST(ihour AS INT) <= 12 THEN '9-12'
    WHEN CAST(ihour AS INT) >= 13 AND CAST(ihour AS INT) <= 16 THEN '13-16'
    WHEN CAST(ihour AS INT) >= 17 AND CAST(ihour AS INT) <= 20 THEN '17-20'
    WHEN CAST(ihour AS INT) IN (21,22,23,0) THEN '21-24'
    WHEN CAST(ihour AS INT) IN (1,2,3,4) THEN '1-4'
END AS shour,
ct,
ei,
uin,
pv,
app_time,
function_time
FROM tb_lol_app_uin_action_data_width
)t GROUP BY sdate,cube(ct),shour,ei,gender,cagerange 
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    
   

    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    