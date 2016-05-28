#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_record_active_day7_increase_remain.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_record_active_day7_increase_remain
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
    
    day8 = today_date - datetime.timedelta(days = 7)
    day8_str = day8.strftime("%Y%m%d")
    tdw.WriteLog("day8_str = " + day8_str )
    
#连接tdw， ieg_qt_community_app数据库
    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    


#创建结果表   
    tdw.WriteLog("== create table king_glory_record_active_day7_increase_remain==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_record_active_day7_increase_remain(
        sdate bigint,
        game_id string,
        day7_remain float 
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_record_active_day7_increase_remain success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_record_active_day7_increase_remain where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_record_active_day7_increase_remain 
    SELECT 
    %s,
    t1.game_id,
    t2.user_remain_num / t1.user_num
    FROM 
    (SELECT tt1.sgameid as game_id,
    COUNT(DISTINCT(tt1.suuid)) as user_num 
    FROM 
        (SELECT 
        DISTINCT suuid,
        sgameid
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding')
        AND (ssubaction='start_recording' OR ssubaction='start_recoding')  AND (tdbank_imp_date BETWEEN '%s00' AND '%s24'))tt1
        LEFT OUTER JOIN 
        (SELECT 
        DISTINCT suuid,
        game_id
        FROM ieg_qt_community_app::king_glory_start_record_active_accumulate WHERE sdate=%s)tt2
        ON tt1.suuid = tt2.suuid AND tt1.sgameid=tt2.game_id
    WHERE tt2.suuid IS NULL 
    GROUP BY tt1.sgameid)t1 
    JOIN 
    (SELECT 
    r1.sgameid as sgameid,
    COUNT(DISTINCT(r1.suuid)) as user_remain_num 
    FROM 
        (SELECT rr1.suuid as suuid ,
        rr1.sgameid as sgameid
        FROM 
        (SELECT 
        DISTINCT suuid,
        sgameid
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding')
        AND (ssubaction='start_recording' OR ssubaction='start_recoding')  AND (tdbank_imp_date BETWEEN '%s00' AND '%s24'))rr1
        LEFT OUTER JOIN 
        (SELECT 
        DISTINCT suuid,
        game_id
        FROM ieg_qt_community_app::king_glory_start_record_active_accumulate WHERE sdate=%s)rr2
        ON rr1.suuid = rr2.suuid AND rr1.sgameid = rr2.game_id 
        WHERE rr2.suuid IS NULL )r1
        JOIN 
        (SELECT 
        DISTINCT suuid,
        sgameid
        FROM ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding')
        AND (ssubaction='start_recording' OR ssubaction='start_recoding')  AND (tdbank_imp_date BETWEEN '%s00' AND '%s24'))rr3
        ON rr3.suuid = r1.suuid  AND rr3.sgameid = r1.sgameid
        GROUP BY r1.sgameid)t2 
        ON t1.game_id=t2.sgameid
    """%(sDate,day7_str,day7_str,day8_str,day7_str,day7_str,day8_str,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
