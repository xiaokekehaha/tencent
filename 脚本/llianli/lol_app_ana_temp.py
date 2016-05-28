#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_ana_temp.py
# 功能描述:     lolapp游戏相关交叉
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
 CREATE TABLE IF NOT EXISTS tb_lol_game_app_ana
(
ieffectflag INT,
ilostflag INT,
win INT,
battle_times BIGINT
)                     '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##写入结果
    sql = ''' 
    INSERT OVERWRITE TABLE tb_lol_game_app_ana
SELECT 
CASE WHEN GROUPING(t.ieffectflag) = 1 THEN -100 ELSE t.ieffectflag END AS ieffectflag,
CASE WHEN GROUPING(t.ilostflag) = 1 THEN -100 ELSE t.ilostflag END AS ilostflag,
t1.win,
COUNT(DISTINCT t1.game_id)
FROM tb_app_login_data_from_tlog t
JOIN 
(
SELECT
qq_uin,
game_id,
win,
champion_id
FROM
tb_lol_game_info WHERE dtstatdate BETWEEN 20151002 AND 20151031
)t1
ON(t.iuin = t1.qq_uin)
GROUP BY t1.win,cube(t.ieffectflag,t.ilostflag)
              

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##流失用户数据创建表写数据
    sql = '''
 CREATE TABLE IF NOT EXISTS tb_lol_game_app_lost_ana
(
ieffectflag INT,
ilostflag INT,
cross_uin_cnt BIGINT
)                 '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    


 
    
    ##写入结果
    sql = ''' 
    INSERT OVERWRITE TABLE tb_lol_game_app_lost_ana
SELECT 
t3.ieffectflag,
t3.ilostflag,
COUNT(DISTINCT t3.iuin)
FROM 
(SELECT ilostflag,ieffectflag,iuin FROM tb_app_login_data_from_tlog)t3
JOIN
(
SELECT 
t.qq_uin as qq_uin
FROM 
(
SELECT
qq_uin 
FROM tb_lol_game_info WHERE dtstatdate BETWEEN 20151002 AND 20151031
)t
LEFT OUTER JOIN
(
SELECT
qq_uin 
FROM tb_lol_game_info WHERE dtstatdate BETWEEN 20151101 AND 20151131
)t1
ON(t.qq_uin = t1.qq_uin)
WHERE t1.qq_uin IS NULL 
)t2
ON(t2.qq_uin = t3.iuin)
GROUP BY t3.ilostflag,t3.ieffectflag
              

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    