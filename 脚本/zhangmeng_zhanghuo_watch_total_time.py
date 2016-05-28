#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     zhangmeng_zhanghuo_watch_total_time.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::zhangmeng_zhanghuo_watch_total_time
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
    tdw.WriteLog("== create table zhangmeng_zhanghuo_watch_total_time==")
    sql = """CREATE TABLE IF NOT EXISTS zhangmeng_zhanghuo_watch_total_time(
        sdate string,
        platform string,
        time float
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table zhangmeng_zhanghuo_watch_total_time success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from zhangmeng_zhanghuo_watch_total_time where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  zhangmeng_zhanghuo_watch_total_time 
    SELECT 
    %s,
    t.platform,
    sum(t.du)/600000 as time
    FROM  
        (select 
        sdate,
        CASE 
        WHEN  ei='火线_视频播放页面时长' THEN 'cf'
        WHEN (id ='1100678382' AND ei='hero_time_video') OR (id ='1200678382' AND ei='herotime_play_time' )THEN 'lol'
        ELSE 'other'
        END AS platform,
        du
        FROM  
        teg_mta_intf::ieg_lol where sdate=%s AND du<500 AND du>10)t
    WHERE t.platform != 'other'
    GROUP BY t.sdate, t.platform    
    """%(sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
