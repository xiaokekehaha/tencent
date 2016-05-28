#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_share_active.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_share_active
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
    
    sql = """drop table king_glory_share_active"""
    res = tdw.execute(sql)

#创建结果表   
    tdw.WriteLog("== create table king_glory_share_active==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_share_active(
        sdate bigint,
        platform string,
        share_video_usenum bigint,
        share_video_times bigint  ,
        share_video_num bigint
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_share_active success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_share_active where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_share_active 
    SELECT 
    %s,
    t.platform,
    COUNT(DISTINCT(t.suuid)) as share_active_usenum,
    COUNT(*) as share_active_times,
    COUNT(DISTINCT(t.svideoid)) as share_video_num
    FROM 
        (SELECT 
        CASE 
            WHEN iClientType=43 THEN 'android'
            WHEN iClientType=44 THEN 'IOS'
            ELSE 'other'
        END AS platform,
        suuid,
        svideoid
    FROM 
    ieg_tdbank::qtalk_dsl_mobilelupinreport_fht0
    WHERE saction='video_share' AND ssubaction='share' AND (tdbank_imp_date BETWEEN '%s00' AND '%s24') )t
    GROUP BY t.platform
    """%(sDate,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
