#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_upload_active.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_upload_active
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
    tdw.WriteLog("== create table king_glory_upload_active==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_upload_active(
        sdate bigint,
        game_id string,
        share_active_usenum bigint,
        vedio_num bigint  ,
        avg_video_size bigint,
        sum_video_size bigint 
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_upload_active success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_upload_active where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_upload_active 
    SELECT 
    %s,
    game_id,
    COUNT(DISTINCT(uuid)) as share_active_usenum,
    COUNT(DISTINCT(vid)) as vedio_num,
    avg(video_size) as avg_video_size,
    sum(video_size) as sum_video_size
    FROM 
    ieg_tdbank::qtalk_dsl_sytimeinfo_fht0
    WHERE tdbank_imp_date BETWEEN '%s00' AND '%s24'
    GROUP BY game_id
    """%(sDate,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
