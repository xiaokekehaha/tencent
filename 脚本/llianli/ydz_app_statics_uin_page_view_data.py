#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_page_view_data.py
# 功能描述:     油点赞数据统计——页面ID的访问量统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_page_view_data
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-04-08
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
            CREATE TABLE IF NOT EXISTS tb_ydz_page_view_data
            (
            dtstatdate INT COMMENT '统计日期',
            appid INT COMMENT '操作系统类型',
            iaccounttype INT COMMENT '标记用户是否登录',
            module_info STRING COMMENT '模块信息',
            pv BIGINT COMMENT '页面访问量',
            uv BIGINT COMMENT '页面的',
            time BIGINT COMMENT '访问时长'
            )  
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DELETE FROM tb_ydz_page_view_data WHERE dtstatdate = %s """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    
    sql = """
    INSERT TABLE tb_ydz_page_view_data 
    SELECT
    sdate,
    CASE WHEN GROUPING(appid) = 1 THEN -100 ELSE appid END  AS appid,
    CASE WHEN GROUPING(iaccounttype) = 1 THEN -100 ELSE iaccounttype END  AS iaccounttype,
    CASE WHEN GROUPING(module_info) = 1 THEN '-100' ELSE module_info END  AS module_info,
    COUNT(*) AS pv,
    COUNT(DISTINCT uin_mac) AS uv,
    SUM(du) AS time
    FROM 
    (
    SELECT
    b.sdate AS sdate,
    b.appid AS appid,
    CASE WHEN a.iaccountid IS NULL THEN 0 ELSE 1 END AS iaccounttype,
    b.uin_mac AS uin_mac,
    b.module_info AS module_info,
    b.du AS du
    FROM
    (
    SELECT 
    sdate,
    id AS appid,
    CASE WHEN INSTR(lower(pi),lower('page')) = 0 THEN lower(pi) ELSE 'other_page' END AS module_info,
    concat(ui,mc) AS uin_mac,
    du
    FROM teg_mta_intf::ieg_youxishengjing PARTITION (p_%s) tmp WHERE sdate = %s AND et = 1 AND av NOT LIKE '1%%'
    )b 
    LEFT OUTER JOIN 
    (
    SELECT 
    sdate ,
    appid,
    uin_mac ,
    iaccountid 
    FROM tb_ydz_app_uin_mac_reflection PARTITION (p_%s) c WHERE iaccountid IS NOT NULL
    )a
    ON (a.sdate = b.sdate AND a.appid = b.appid AND a.uin_mac = b.uin_mac)
    )t
    GROUP BY sdate,cube(appid,module_info,iaccounttype)
    
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    