#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_click_action_water_pv_uv.py
# 功能描述:     油点赞数据统计——自定义事件的统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_mta_click_action_pv_uv
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-04-01
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


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20141201'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)
  
    
    sql = """
    CREATE TABLE IF NOT EXISTS tb_ydz_mta_click_action_pv_uv
    (
    dtstatdate INT COMMENT '统计日期',
    appid INT COMMENT 'appid',
    accounttype INT COMMENT '账户类型',
    machineflag INT COMMENT '是否是机器账号',
    moduleid STRING COMMENT '模块ID-页面ID',
    saction STRING COMMENT '用户行为',
    mac_uv BIGINT COMMENT 'mac 设备计算UV',
    account_uv BIGINT COMMENT '账号计算UV',
    pv BIGINT COMMENT '访问PV',
    time BIGINT COMMENT '访问时长'
    )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    DELETE FROM tb_ydz_mta_click_action_pv_uv WHERE dtstatdate = %s
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
   
    
    sql = """
    INSERT TABLE tb_ydz_mta_click_action_pv_uv
    SELECT
    %s AS dtstatdate,
    CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END AS appid,
    CASE WHEN GROUPING(accounttype) = 1 THEN -100 ELSE accounttype END AS accounttype,
    CASE WHEN GROUPING(machineflag) = 1 THEN -100 ELSE machineflag END AS machineflag,
    moduleid ,
    saction ,
    COUNT(DISTINCT uin_mac) AS mac_uv,
    COUNT(DISTINCT accountid) AS account_uv,
    COUNT(*) AS pv,
    SUM(du) AS du
    FROM
    (
    SELECT
    appid ,
    accounttype ,
    machineflag ,
    uin_mac ,
    accountid ,
    moduleid ,
    saction ,
    du 
    FROM tb_ydz_mta_click_action_width_table PARTITION (p_%s) a WHERE dtstatdate = %s
    )t 
    GROUP BY cube(appid,accounttype,machineflag),moduleid,saction

     """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    