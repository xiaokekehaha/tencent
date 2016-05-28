#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     zhangmeng_zhanghuo_watch_time_dis.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::zhangmeng_zhanghuo_watch_time_dis
# 数据源表:     teg_mta_intf::ieg_lol
# 创建人名:     yaoyaopeng
# 创建日期:     2015-07-24
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
    tdw.WriteLog("===count topic detail page pv uv===")
    
#接收日期参数
    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")
    sDate = argv[0];
    
    tdw.WriteLog("== sDate = " + sDate + " ==")

#连接tdw， ieg_qt_community_app数据库
    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    

#创建结果表   
    tdw.WriteLog("== create table zhangmeng_zhanghuo_watch_time_dis==")
    sql = """CREATE TABLE IF NOT EXISTS zhangmeng_zhanghuo_watch_time_dis(
        sdate int,
        platform string,
        time_zone string,
        usernum bigint ,
        times bigint 
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table zhangmeng_zhanghuo_watch_time_dis success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from zhangmeng_zhanghuo_watch_time_dis where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  zhangmeng_zhanghuo_watch_time_dis 
    SELECT 
    %s,
    t.platform,
    t.time_zone,
    COUNT(DISTINCT(t.uin)) as user_num,
    COUNT(*) as times 
    FROM  
        (SELECT
        CASE 
        WHEN  ei='火线_视频播放页面时长' THEN 'cf'
        WHEN (id ='1100678382' AND ei='hero_time_video') OR (id ='1200678382' AND ei='herotime_play_time' )THEN 'lol'
        ELSE 'other'
        END AS platform,
        CASE
        WHEN du > 0 AND du <= 5 THEN '0s~5s'
        WHEN du > 5 AND du<= 10 THEN '5s~10s'
        WHEN du > 10 AND du <= 30 THEN '10s~30s'
        WHEN du > 30 AND du <= 60 THEN '30s~60s'
        WHEN du > 60 AND du <= 90 THEN '60s~90s'
        WHEN du > 90 AND du <= 120 THEN '90s~120s'
        WHEN du > 120 AND du <= 180 THEN '120s~180s'
        WHEN du > 180 AND du <= 270 THEN '180s~270s'
        WHEN du > 270 AND du <= 360 THEN '270s~360s'
        WHEN du > 360 AND du <= 600 THEN '360s~600s'
        WHEN du > 600 THEN '>600s'
        ELSE 'other'
        END AS time_zone, 
        get_json_object(kv,'$.uin') as uin  
        FROM  
        teg_mta_intf::ieg_lol where sdate=%s AND du<500 )t
    WHERE t.platform != 'other' AND t.time_zone != 'other'
    GROUP BY t.platform,t.time_zone    
    """%(sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
