#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_pageview_pv_uv.py
# 功能描述:     油点赞数据统计——自定义事件的统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_app_page_view_pv_uv
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
    CREATE TABLE IF NOT EXISTS tb_ydz_app_page_view_pv_uv
    (
    dtstatdate INT COMMENT '统计时间',
    appid INT COMMENT '应用ID',
    accounttype INT COMMENT '账户类型，0未登录，1登录',
    machineflag INT COMMENT '是否是机器人账户 0 不是 1 是',
    moduleid STRING COMMENT '模块ID',
    kv STRING COMMENT '对应的模块参数，目前只有 tag_sub_feeds，game_sub_feeds有意义',
    pv BIGINT COMMENT '页面访问PV',
    mac_uv BIGINT COMMENT 'MAC地址统计UV',
    account_uv BIGINT COMMENT '账户统计UV',
    time BIGINT COMMENT '总时长'
    )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    DELETE FROM tb_ydz_app_page_view_pv_uv WHERE dtstatdate = %s
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT TABLE tb_ydz_app_page_view_pv_uv
    SELECT
    %s AS dtstatdate,
    CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END AS appid,
    CASE WHEN GROUPING(accounttype) = 1 THEN -100 ELSE accounttype END AS accounttype,
    CASE WHEN GROUPING(machineflag) = 1 THEN -100 ELSE machineflag END AS machineflag,
    moduleid,
    kv,
    COUNT(*) AS pv,
    COUNT(DISTINCT uin_mac) AS mac_uv,
    COUNT(DISTINCT accountid) AS account_uv,
    SUM(du) AS time
    FROM 
    (
    SELECT
    appid,
    accounttype,
    machineflag,
    moduleid,
    '-100' AS kv,
    uin_mac,
    accountid,
    du 
    FROM tb_ydz_mta_pageview_action_width_table PARTITION (p_%s) a 
    
    
    UNION ALL 
    
    SELECT
    appid,
    accounttype,
    machineflag,
    '-100' AS moduleid,
    '-100' AS kv,
    uin_mac,
    accountid,
    du 
    FROM tb_ydz_mta_pageview_action_width_table PARTITION (p_%s) b
    
    
    UNION ALL 
    
    SELECT
    appid,
    accounttype,
    machineflag,
    moduleid,
    kv,
    uin_mac,
    accountid,
    du 
    FROM tb_ydz_mta_pageview_action_width_table PARTITION (p_%s) c WHERE moduleid IN ('tag_sub_feeds','game_sub_feeds') 
    )t
    GROUP BY cube(appid,accounttype,machineflag),moduleid,kv
     """%(sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    