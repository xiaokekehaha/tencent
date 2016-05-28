#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_record_success_active_user.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_record_success_active_user
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
    tdw.WriteLog("== create table king_glory_record_success_active_user==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_record_success_active_user(
        sdate bigint,
        game_id string,
        version string,
        platform string,
        system string,
        smachinetype string,
        record_success_usernum bigint  ,
        record_success_times bigint
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_record_success_active_user success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_record_success_active_user where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_record_success_active_user 
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
    CASE WHEN grouping(t.smachinetype) = 1 THEN 'all'
    ELSE t.smachinetype
    END AS smachinetype,
    COUNT(DISTINCT(suuid)) as record_success_usernum ,
    COUNT(*) as record_success_times
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
        smachinetype,
        suuid
        FROM 
        ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
        WHERE (saction='video_recording' OR saction='video_recoding') 
        AND (ssubaction='recording_success' OR ssubaction='recoding_success') AND (tdbank_imp_date BETWEEN '%s00' AND '%s24') )t 
    GROUP BY cube(t.sgameid),cube(t.sversion),cube(t.platform),cube(t.system),cube(t.smachinetype)
    """%(sDate,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
