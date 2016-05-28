#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_staics_uin_pi.py
# 功能描述:     油点赞数据统计——页面ID的统计
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
import datetime

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
    today_str = sDate
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d") 

    sql = """
            CREATE TABLE IF NOT EXISTS tb_ydz_app_page_action_width_table
        (
        sdate INT COMMENT '统计日期',
        appid BIGINT COMMENT '用户的应用ID',
        ei STRING COMMENT '统计的事件或页面情况',
        accountid BIGINT COMMENT '用户的账户信息',
        pv BIGINT COMMENT '功能点的点击量',
        time BIGINT COMMENT '页面使用时长'
        )PARTITION BY LIST (sdate)
        (
        PARTITION p_20160318 VALUES IN (20160318),
        PARTITION p_20160319 VALUES IN (20160319),
        PARTITION p_20160320 VALUES IN (20160320),
        PARTITION p_20160321 VALUES IN (20160321),
        PARTITION p_20160322 VALUES IN (20160322),
        PARTITION p_20160323 VALUES IN (20160323),
        PARTITION p_20160324 VALUES IN (20160324),
        PARTITION p_20160325 VALUES IN (20160325),
        PARTITION p_20160326 VALUES IN (20160326),
        PARTITION p_20160327 VALUES IN (20160327),
        PARTITION p_20160328 VALUES IN (20160328),
        PARTITION p_20160329 VALUES IN (20160329),
        PARTITION p_20160330 VALUES IN (20160330),
        PARTITION p_20160331 VALUES IN (20160331)
        ) 
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ ALTER TABLE tb_ydz_app_page_action_width_table DROP PARTITION (p_%s)"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """ ALTER TABLE tb_ydz_app_page_action_width_table ADD PARTITION p_%s VALUES IN (%s)"""%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    INSERT  TABLE tb_ydz_app_page_action_width_table
  
    SELECT
    b.sdate AS sdate,
    CASE WHEN GROUPING(a.appid) = 1 THEN -100 ELSE a.appid END  AS appid,
    CASE WHEN GROUPING(b.module_info) = 1 THEN '-100' ELSE b.module_info END  AS module_info,
    a.iaccountid AS iaccountid,
    COUNT(*) AS pv,
    SUM(b.du) AS time
    FROM
    (
    SELECT 
    sdate ,
    appid,
    uin_mac ,
    iaccountid 
    FROM tb_ydz_app_uin_mac_reflection PARTITION (p_%s) c WHERE iaccountid IS NOT NULL
    )a
    JOIN
    (
    SELECT 
    sdate,
    id AS appid,
    CASE WHEN INSTR(lower(pi),lower('page')) = 0 THEN lower(pi) ELSE 'other_page' END AS module_info,
    concat(ui,mc) AS uin_mac,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate = %s AND et = 1 AND av NOT LIKE '1%%'
    )b
    ON (a.sdate = b.sdate AND a.appid = b.appid AND a.uin_mac = b.uin_mac)
    GROUP BY b.sdate,a.iaccountid,cube(a.appid,b.module_info)
    
    
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    