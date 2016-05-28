#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_click_action_data.py
# 功能描述:     油点赞数据统计——自定义事件点击上报统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_click_data
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
    CREATE TABLE IF NOT EXISTS tb_ydz_click_data
    (
    dtstatdate INT COMMENT '统计日期',
    appid INT COMMENT '操作系统类型',
    iaccounttype INT COMMENT '标记用户是否登录',
    saction STRING COMMENT '点击行为',
    feed_id STRING COMMENT 'feedid',
    pv BIGINT COMMENT '行为点击量',
    uv BIGINT COMMENT '行为点击的UV',
    time BIGINT COMMENT '触发行为时长'
    ) 
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    DELETE FROM tb_ydz_click_data WHERE dtstatdate = %s
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    sql = """
    INSERT TABLE tb_ydz_click_data 
    SELECT
    sdate,
    CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END  AS appid,
    CASE WHEN GROUPING(iaccounttype) = 1 THEN -100 ELSE iaccounttype END  AS iaccounttype,
    ei,
    feed_id,
    SUM(pv) AS pv,
    COUNT(DISTINCT uin_mac) AS uv,
    SUM(time) AS time
    FROM 
    (
    SELECT
    sdate,
    appid,
    ei,
    CASE WHEN GROUPING(feed_id) = 1 THEN '-100' ELSE feed_id END  AS feed_id,
    iaccounttype,
    uin_mac,
    COUNT(*) AS pv,
    SUM(du) AS time
    FROM 
    (
    SELECT
    sdate,
    id AS appid,
    concat(ei,'#',get_json_object(kv,'$.module_id'),'#',get_json_object(kv,'$.sub_module_id')) as ei,
    CASE WHEN get_json_object(kv,'$.account') IS NULL THEN 0 ELSE 1 END AS iaccounttype ,
    concat(ui,mc) AS uin_mac,
    CASE WHEN get_json_object(kv,'$.feed_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.feed_id') END AS  feed_id,
    
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate = %s AND et = 1000 AND av NOT LIKE '1%%'
    )tt1
    
    GROUP BY sdate,appid,ei,cube(feed_id),iaccounttype,uin_mac
    
    )t
    GROUP BY sdate,ei,feed_id,cube(appid,iaccounttype)

     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    