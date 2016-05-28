#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_staics_uin_ei.py
# 功能描述:     油点赞数据统计——自定义事件的统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_app_page_action_width_table
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
    CREATE TABLE IF NOT EXISTS tb_ydz_app_click_data_width_table
    (
    sdate INT COMMENT '统计日期',
    appid BIGINT COMMENT '用户的应用ID',
    ei STRING COMMENT '统计的事件或页面情况',
    game_category_id STRING COMMENT '参数1情况',
    feed_id STRING COMMENT '参数2情况',
    tag_id STRING COMMENT '参数3情况',
    game_id STRING COMMENT '参数4情况',
    expression_id STRING COMMENT '参数5情况',
    big_font_id STRING COMMENT '参数6情况',
    bubble_id STRING COMMENT '参数7请况',
    accountid STRING COMMENT '用户的账号信息',
    pv BIGINT COMMENT '功能点的点击量',
    time BIGINT COMMENT '页面使用时长'
    )PARTITION BY LIST (sdate)
    (
    PARTITION p_20160318 VALUES IN (20160318),
    PARTITION p_20160319 VALUES IN (20160319)
    ) 
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    ALTER TABLE tb_ydz_app_click_data_width_table DROP PARTITION (p_%s)
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """ ALTER TABLE tb_ydz_app_click_data_width_table ADD PARTITION p_%s VALUES IN (%s)"""%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    INSERT TABLE tb_ydz_app_click_data_width_table
    SELECT
    sdate,
    appid,
    ei,
    game_category_id,
    feed_id,
    tag_id,
    game_id,
    expression_id,
    big_font_id,
    bubble_id,
    accountid,
    COUNT(*) AS pv,
    SUM(du) AS time
    FROM 
    (
    SELECT
    sdate,
    id AS appid,
    concat(ei,'#',get_json_object(kv,'$.module_id'),'#',get_json_object(kv,'$.sub_module_id')) as ei,
    get_json_object(kv,'$.account') AS accountid ,
    CASE WHEN get_json_object(kv,'$.game_category_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.game_category_id') END AS  game_category_id,
    CASE WHEN get_json_object(kv,'$.feed_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.feed_id') END AS  feed_id,
    CASE WHEN get_json_object(kv,'$.tag_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.tag_id') END AS  tag_id,
    CASE WHEN get_json_object(kv,'$.game_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.game_id') END AS  game_id,
    CASE WHEN get_json_object(kv,'$.expression_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.expression_id') END AS  expression_id,
    CASE WHEN get_json_object(kv,'$.big_font_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.big_font_id') END AS  big_font_id,
    CASE WHEN get_json_object(kv,'$.bubble_id') IS NULL THEN 'none' ELSE get_json_object(kv,'$.bubble_id') END AS  bubble_id,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate = %s AND et = 1000 AND av NOT LIKE '1%%'
    )t WHERE accountid IS NOT NULL 
    GROUP BY sdate,appid,ei,game_category_id,feed_id,tag_id,game_id,expression_id,big_font_id,bubble_id,accountid


     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    