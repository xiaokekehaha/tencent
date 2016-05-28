#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     herotime_tgp_user_accumulate_2days_ago.py
# 功能描述:     手游宝全民突击数据
# 输入参数:     yyyymmdd    例如：20160120
# 目标表名:     ieg_qt_community_app::herotime_tgp_user_accumulate_2days_ago
# 数据源表:     ieg_qt_community_app::herotime_tgp_user_accumulate
# 创建人名:     yaoyaopeng
# 创建日期:     2016-01-20
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
    ##sDate = '20150111'
    
    today_str=sDate
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)



    sql = """
            CREATE TABLE IF NOT EXISTS herotime_tgp_user_accumulate_2days_ago
            (
            f_pvid bigint,
            fdate int
            ) """
    res = tdw.execute(sql)

    sql="""delete from herotime_tgp_user_accumulate_2days_ago where fdate=%s""" %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
#TGP核心数据计算
    tdw.WriteLog("==core data calculation==")

    sql = """
    insert table herotime_tgp_user_accumulate_2days_ago
    SELECT DISTINCT t.f_pvid,
    %s  
    FROM 
        (SELECT 
        DISTINCT f_pvid 
        FROM ieg_qt_community_app::herotime_tgp_user_accumulate
        WHERE fdate=%s 
        UNION ALL 
        SELECT 
        DISTINCT f_pvid
        FROM ieg_qt_community_app::herotime_tgp_user_accumulate_2days_ago
        WHERE fdate=%s)t 
    """%(sDate,sDate,pre_date_str) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

