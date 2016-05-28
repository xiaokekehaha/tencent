#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_pageview_accumulate.py
# 功能描述:     油点赞数据统计——页面访问
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_mta_pageview_action_accumulate
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-04-18
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module
import datetime

# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    today_str = sDate
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d") 
    
    
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
    CREATE TABLE IF NOT EXISTS tb_ydz_mta_pageview_action_accumulate
    (
    dtstatdate INT COMMENT '统计日期',
    appid INT COMMENT 'APPID',
    machineflag INT COMMENT '用户是否是机器登录的标记',
    moduleid STRING COMMENT '页面ID',
    kv STRING COMMENT '只对game_sub_feeds 和 tag_sub_feeds有效',
    accountid BIGINT COMMENT '用户的账号ID',
    pv BIGINT COMMENT '用户点击PV',
    du BIGINT COMMENT '时长信息'
    )
    PARTITION BY LIST (dtstatdate)
    (
        PARTITION p_20160401  VALUES IN (20160401),
        PARTITION p_20160402  VALUES IN (20160402)
    )
    
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    ALTER TABLE tb_ydz_mta_pageview_action_accumulate DROP PARTITION (p_%s)
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    ALTER TABLE tb_ydz_mta_pageview_action_accumulate ADD PARTITION p_%s VALUES IN (%s)
     """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT TABLE tb_ydz_mta_pageview_action_accumulate
    SELECT
    %s AS dtstatdate,
    appid,
    machineflag,
    moduleid,
    kv,
    accountid,
    SUM(pv) AS pv,
    SUM(time) AS time
    FROM
    (
    SELECT 
    appid,
    machineflag,
    moduleid,
    kv,
    accountid,
    pv,
    du AS time
    FROM   tb_ydz_mta_pageview_action_accumulate PARTITION (p_%s) tmp
    
    UNION ALL 
    
    SELECT
    CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END AS appid,
    CASE WHEN GROUPING(machineflag) = 1 THEN -100 ELSE machineflag END AS machineflag,
    CASE WHEN GROUPING(moduleid) = 1 THEN '-100' ELSE moduleid END AS moduleid,
    kv,
    accountid,
    COUNT(*) AS pv,
    SUM(du) AS time
    FROM 
     (
     SELECT
    appid,
    machineflag,
    moduleid,
    '-100' AS kv,
    uin_mac,
    accountid,
    du 
    FROM tb_ydz_mta_pageview_action_width_table PARTITION (p_%s) a WHERE accounttype = 1
    
    
    UNION ALL 
    
    SELECT
    appid,
    machineflag,
    '-100' AS moduleid,
    '-100' AS kv,
    uin_mac,
    accountid,
    du 
    FROM tb_ydz_mta_pageview_action_width_table PARTITION (p_%s) b WHERE accounttype = 1
    
    
    UNION ALL 
    
    SELECT
    appid,
    machineflag,
    moduleid,
    kv,
    uin_mac,
    accountid,
    du 
    FROM tb_ydz_mta_pageview_action_width_table PARTITION (p_%s) c WHERE moduleid IN ('tag_sub_feeds','game_sub_feeds') AND accounttype = 1
     )t
     GROUP BY cube(appid,machineflag,moduleid),accountid,kv
    )t1 GROUP BY appid,machineflag,moduleid,kv,accountid
     """%(sDate,pre_date_str,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    