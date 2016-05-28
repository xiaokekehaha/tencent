#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_record_time_dis.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_record_time_dis
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
    

#创建结果表   
    tdw.WriteLog("== create table king_glory_record_time_dis==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_record_time_dis(
        sdate bigint,
        game_id string,
        version string,
        platform string,
        system string,
        time_zone string,
        user_num bigint   
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_record_time_dis success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_record_time_dis where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_record_time_dis 
    SELECT 
    %s,
    CASE WHEN grouping(t.sgameid)=1 THEN 'all'
    ELSE t.sgameid
    END AS sgameid,
    CASE WHEN grouping(t.sversion)=1 THEN 'all'
    ELSE t.sversion
    END AS sversion,
    CASE WHEN grouping(t.platform)=1 THEN 'all'
    ELSE t.platform
    END AS platform,
    CASE WHEN grouping(t.system)=1 THEN 'all'
    ELSE t.system
    END AS system,
    t.time_zone,
    COUNT(DISTINCT(t.suuid)) as user_num 
    FROM 
        (SELECT 
        sgameid,
        sversion,
         CASE 
            WHEN iAccountType=1 THEN 'qq'
            WHEN iAccountType=2 THEN 'weixin'
            ELSE 'other'
            END AS system,
        CASE 
            WHEN iClientType=43 THEN 'android'
            WHEN iClientType=44 THEN 'IOS'
            ELSE 'other'
            END AS platform,
        CASE
            WHEN sreserve3 > 0 AND sreserve3 <= 30 THEN '0~30s'
            WHEN sreserve3 > 30 AND sreserve3 <= 60 THEN '30s~1min'
            WHEN sreserve3 > 60 AND sreserve3 <= 300 THEN '1min~5min'
            WHEN sreserve3 > 300 AND sreserve3 <= 600 THEN '5min~10min'
            WHEN sreserve3 > 600 AND sreserve3 <= 1200 THEN '10min~20min'
            WHEN sreserve3 > 1200 THEN '>20min'
            ELSE 'other'
        END AS time_zone,
        suuid
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        where (saction='video_recording' OR saction='video_recoding') 
        AND (ssubaction='recording_success' OR ssubaction='recoding_success') AND (tdbank_imp_date BETWEEN '%s00' AND '%s24'))t 
    WHERE t.system != 'other' AND t.platform != 'other' AND t.time_zone != 'other'
    GROUP BY  cube(t.sgameid),cube(t.sversion),cube(t.platform),cube(t.system),t.time_zone
    """%(sDate,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
