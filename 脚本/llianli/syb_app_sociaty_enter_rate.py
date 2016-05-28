#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_sicialty_enter_rate.py
# 功能描述:     手游宝炫斗公会入驻率统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-11-03
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，公会入住率表
    sql = '''
            CREATE TABLE IF  NOT EXISTS tb_syb_app_ttxd_sociaty_enter
    (
    dtstatdate INT COMMENT '统计日期',
    platid INT COMMENT '平台类型',
    areaid INT COMMENT '大区ID',
    gameid STRING COMMENT '游戏ID',
    sopertype STRING COMMENT '操作类型', 
    guild_num INT COMMENT '公会数量'
    )'''
            
    res = tdw.execute(sql)


    sql=''' delete from  tb_syb_app_ttxd_sociaty_enter where dtstatdate = %s''' % (today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    

 
    
    ##写入入住率数据
    sql = ''' 
    INSERT TABLE tb_syb_app_ttxd_sociaty_enter
        SELECT
        %s AS dtstatdate,
        platid,
        areaid,
        gameid,
        CASE WHEN GROUPING(sopertype) = 1 THEN '-100' ELSE sopertype END AS  sopertype,
        COUNT(DISTINCT guild_id) AS guild_num
        FROM 
        (
        SELECT 
        platid,
        areaid,
        gameid,
        CASE WHEN oper_type IN (1,2) THEN 'public' ELSE CAST(oper_type AS STRING) END AS sopertype,
        guild_id
        FROM  ieg_tdbank::qtalk_dsl_guildexcitsvr_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
        )t
        GROUP BY platid,
        areaid,
        gameid,cube(sopertype)
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    