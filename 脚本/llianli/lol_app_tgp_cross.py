#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tgp_cross.py
# 功能描述:     lolapp与tgp交叉数据
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
 CREATE TABLE IF NOT EXISTS tb_use_lol_app_tgp_lol_cross_mac_effect
(
iuin BIGINT,
iflag BIGINT,
uin_mac STRING,
id INT
)                   '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##写入结果
    sql = ''' 
       INSERT OVERWRITE TABLE tb_use_lol_app_tgp_lol_cross_mac_effect
SELECT 
t.*
FROM 
(
SELECT
uinmac
FROM 
(
SELECT
uin_mac as uinmac ,
COUNT(DISTINCT iuin ) AS uin_cnt
FROM tb_use_lol_app_tgp_lol_cross_mac
GROUP BY uin_mac
)tmp
WHERE uin_cnt <= 2
)t1
JOIN
  tb_use_lol_app_tgp_lol_cross_mac t
ON(t.uin_mac = t1.uinmac)

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##用户使用各种行为表创建
    sql = '''
CREATE TABLE IF NOT EXISTS tb_lol_app_tgp_corss_ei
(
ei STRING,
iflag INT,
use_days INT,
use_uin_cnt BIGINT
)              '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##写入结果
    sql = ''' 
    INSERT OVERWRITE TABLE tb_lol_app_tgp_corss_ei

SELECT
ei,
iflag,
use_days,
COUNT(DISTINCT iuin)
FROM
(
SELECT 
t1.ei AS ei,
t.iflag AS iflag,
t.iuin AS iuin,
COUNT(DISTINCT t1.fdate) AS use_days
FROM
(
SELECT
DISTINCT
iflag,
uin_mac,
iuin,
id
FROM
tb_use_lol_app_tgp_lol_cross_mac_effect WHERE uin_mac IS NOT NULL
) t
JOIN
(
SELECT 
fdate,
id,
ei,
uin_mac,
pv
FROM  tb_lol_app_ei WHERE fdate >= 20151201 AND fdate <= 20151231
)t1
on (t.uin_mac = t1.uin_mac and t.id = t1.id)
GROUP BY t.iuin,t1.ei,t.iflag
)t2
GROUP BY ei,iflag,use_days
              

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    