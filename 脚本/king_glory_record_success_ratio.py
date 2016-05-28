#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     king_glory_record_success_ratio.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::king_glory_record_success_ratio
# 数据源表:     ieg_qt_community_app::king_glory_record_success_active_user, ieg_qt_community_app::king_glory_record_active_user
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
    tdw.WriteLog("== create table king_glory_record_success_ratio==")
    sql = """CREATE TABLE IF NOT EXISTS king_glory_record_success_ratio(
        sdate bigint,
        game_id string,
        version string,
        platform string,
        system string,
        success_ratio float  
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table king_glory_record_success_ratio success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from king_glory_record_success_ratio where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  king_glory_record_success_ratio 
    SELECT 
    %s,
    t1.game_id,
    t1.version,
    t1.platform,
    t1.system,
    t1.record_success_usernum/t2.start_record_usernum as success_record_ratio
    FROM 
        (
        SELECT 
        game_id,
        version,
        platform,
        system,
        record_success_usernum
        FROM ieg_qt_community_app::king_glory_record_success_active_user WHERE sdate=%s
        )t1
        JOIN 
        (
        SELECT 
        game_id,
        version,
        platform,
        system,
        start_record_usernum
        FROM ieg_qt_community_app::king_glory_start_record_active_user WHERE sdate=%s
        )t2
        ON t1.game_id = t2.game_id AND t1.version = t2.version AND t1.platform=t2.platform AND t1.system = t2.system
    """%(sDate,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
