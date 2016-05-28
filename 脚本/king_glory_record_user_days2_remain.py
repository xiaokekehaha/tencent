#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_record_user_days2_remain.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_record_user_days2_remain.py
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
    today_str=sDate
    tdw.WriteLog("sDate = " + sDate + "====" + today_str)
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    tdw.WriteLog("today_str = " + today_str)
    
    day2 = today_date - datetime.timedelta(days = 1)
    day2_str = day2.strftime("%Y%m%d")
    tdw.WriteLog("day2_str = " + day2_str )
    
    day3 = today_date - datetime.timedelta(days = 2)
    day3_str = day3.strftime("%Y%m%d")
    tdw.WriteLog("day3_str = " + day3_str )
     
    day4 = today_date - datetime.timedelta(days = 3)
    day4_str = day4.strftime("%Y%m%d")
    tdw.WriteLog("day4_str = " + day4_str )

    day5 = today_date - datetime.timedelta(days = 4)
    day5_str = day5.strftime("%Y%m%d")
    tdw.WriteLog("day5_str = " + day5_str )
    
    day6 = today_date - datetime.timedelta(days = 5)
    day6_str = day6.strftime("%Y%m%d")
    tdw.WriteLog("day6_str = " + day6_str )

    day7 = today_date - datetime.timedelta(days = 6)
    day7_str = day7.strftime("%Y%m%d")
    tdw.WriteLog("day7_str = " + day7_str )
#连接tdw， ieg_qt_community_app数据库
    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    

#创建结果表   
    tdw.WriteLog("== create table king_glory_record_user_days2_remain==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_record_user_days2_remain(
        sdate bigint,
        game_id string,
        version string,
        platform string,
        system string,
        day2_remain float
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_record_user_days2_remain success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_record_user_days2_remain where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")

#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_record_user_days2_remain 
    SELECT 
    %s,
    t1.sgameid,
    t1.sversion,
    t1.iClientType,
    t1.iAccountType  ,
    t1.days2_user_num/t2.day1_user_num as days2_remain
    FROM 
    (SELECT 
    CASE WHEN grouping(tt1.sgameid) = 1 THEN 'all'
    ELSE tt1.sgameid
    END as sgameid,
    CASE WHEN grouping(tt1.sversion) = 1 THEN 'all'
    ELSE tt1.sversion
    END as sversion,
    CASE WHEN grouping(tt1.iClientType) = 1 THEN 111111
    ELSE tt1.iClientType
    END as iClientType,
    CASE WHEN grouping(tt1.iAccountType) = 1 THEN 111111
    ELSE tt1.iAccountType
    END as iAccountType,
    COUNT(DISTINCT(tt1.suuid)) as days2_user_num
    FROM 
        (SELECT 
        sgameid,
        sversion,
        iClientType,
        iAccountType,
        suuid 
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding') 
        AND (ssubaction='start_recording' OR ssubaction='start_recoding') AND (tdbank_imp_date BETWEEN '%s00' AND '%s24'))tt1
        JOIN 
        (SELECT 
        sgameid,
        sversion,
        iClientType,
        iAccountType,
        suuid 
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding') 
        AND (ssubaction='start_recording' OR ssubaction='start_recoding')  AND (tdbank_imp_date BETWEEN '%s00' AND '%s24'))tt2
        ON 
        tt1.sgameid = tt2.sgameid AND tt1.sversion = tt2.sversion AND tt1.iClientType = tt2.iClientType  AND tt1.iAccountType = tt2.iAccountType  AND tt1.suuid = tt2.suuid
        GROUP BY cube(tt1.sgameid), cube(tt1.sversion),cube(tt1.iClientType),cube(tt1.iAccountType))t1
    JOIN 
    (SELECT 
    CASE WHEN grouping(sgameid) = 1 THEN 'all'
    ELSE sgameid
    END AS sgameid,
    CASE WHEN grouping(sversion) = 1 THEN 'all'
    ELSE sversion
    END AS sversion,
    CASE WHEN grouping(iClientType) = 1 THEN 111111
    ELSE iClientType
    END AS iClientType,
    CASE WHEN grouping(iAccountType) = 1 THEN 111111
    ELSE iAccountType
    END as iAccountType,
    COUNT(DISTINCT(suuid)) as day1_user_num  
    FROM 
    ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
    WHERE (saction='video_recording' OR saction='video_recoding') 
        AND (ssubaction='start_recording' OR ssubaction='start_recoding') AND (tdbank_imp_date BETWEEN '%s00' AND '%s24')
    GROUP BY cube(sgameid),cube(sversion),cube(iClientType),cube(iAccountType))t2
    ON t1.sgameid = t2.sgameid AND t1.sversion = t2.sversion AND t1.iClientType = t2.iClientType AND t1.iAccountType = t2.iAccountType
    """%(sDate,sDate,sDate,day2_str,day2_str,day2_str,day2_str) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
