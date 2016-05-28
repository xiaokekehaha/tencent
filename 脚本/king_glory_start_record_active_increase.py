#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_start_record_active_increase.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_start_record_active_increase
# 数据源表:     ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
# 创建人名:     yaoyaopeng
# 创建日期:     2016-02-24
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

    print "===HELLO TDW=="
    tdw.WriteLog("===statistic upload data===")
    
#接收日期参数
    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")
    sDate = argv[0];
    
    tdw.WriteLog("== sDate = " + sDate + " ==")

#连接tdw， ieg_qt_community_app数据库
    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    
    today_str=sDate
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    
#创建结果表   
    tdw.WriteLog("== create table king_glory_start_record_active_increase==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_start_record_active_increase(
        sdate bigint,
        game_id string , 
        start_record_increase bigint
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_start_record_active_increase success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_start_record_active_increase where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_start_record_active_increase 
    SELECT 
    %s,
    t1.sgameid,
    COUNT(DISTINCT(t1.suuid)) as start_record_increase
    FROM 
        (SELECT 
        DISTINCT suuid,
        sgameid
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding')
        AND (ssubaction='start_recording' OR ssubaction='start_recoding') AND (tdbank_imp_date BETWEEN '%s00' AND '%s23') )t1
        LEFT OUTER JOIN 
        (SELECT 
        DISTINCT suuid,
        game_id
        FROM king_glory_start_record_active_accumulate WHERE sdate=%s)t2 
        ON t1.suuid = t2.suuid AND t1.sgameid = t2.game_id
        WHERE t2.suuid IS NULL 
        GROUP BY t1.sgameid
    """%(sDate,sDate,sDate,pre_date_str) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
