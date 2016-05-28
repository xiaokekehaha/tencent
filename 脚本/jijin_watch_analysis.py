#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     jijin_player_watch_analysis.py
# 功能描述:     手游宝各功能pvuv数据计算
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.topic_detail_pvuv_test
# 数据源表:     teg_mta_intf::ieg_shouyoubao
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


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
#集锦视频各渠道观看人数/次数　整体
    tdw.WriteLog("== vid_watch_num_times_20160307_20160313 ==")
    sql = """INSERT TABLE vid_watch_num_times_20160307_20160313
    SELECT 
    t1.platform,
    COUNT(DISTINCT(t1.uin)) as user_num,
    COUNT(*) as times ,
    SUM(du) as watch_time 
    FROM 
    (SELECT 
    vid ,
    platform ,
    uin,
    du
    FROM original_vid_watch_num_times_20160307_20160313)t1 
    JOIN 
    (SELECT 
    vid 
    FROM jijin_vid)t2 
    ON t1.vid = t2.vid 
    GROUP BY t1.platform """
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
   
#集锦视频各渠道观看次数分布区间
    tdw.WriteLog("== jijin_vid_watch_times_20160307_20160313 ==")
    sql = """INSERT TABLE jijin_vid_watch_times_20160307_20160313
    SELECT 
    t1.vid,
    COUNT(*) as watch_times 
    FROM 
    (SELECT 
    vid ,
    uin
    FROM original_vid_watch_num_times_20160307_20160313)t1 
    JOIN 
    (SELECT 
    vid 
    FROM jijin_vid)t2 
    ON t1.vid = t2.vid 
    GROUP BY t1.vid """
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """INSERT TABLE jijin_watch_times_vid_dis_20160307_20160313
    SELECT 
    t1.watch_times_zone,
    COUNT(DISTINCT(t1.vid)) as vid_num
    FROM 
    (SELECT 
    vid,
    CASE 
    WHEN watch_times = 0 THEN '0'
    WHEN watch_times > 0 AND watch_times <= 10 THEN '1~10'
    WHEN watch_times > 10 AND watch_times <= 20 THEN '11~20'
    WHEN watch_times > 20 AND watch_times <= 50 THEN '21~50'
    WHEN watch_times > 50 AND watch_times <= 100 THEN '51~100'
    WHEN watch_times > 100 AND watch_times <= 200 THEN '101~200'
    WHEN watch_times > 200 AND watch_times <= 500 THEN '200~500'
    WHEN watch_times > 500 AND watch_times <= 1000 THEN '501~1000'
    WHEN watch_times > 1000 AND watch_times <= 2000 THEN '1001~2000'
    WHEN watch_times > 2000 AND watch_times <= 5000 THEN '2001~5000'
    WHEN watch_times > 5000 AND watch_times <= 10000 THEN '5001~10000'
    WHEN watch_times > 10000 AND watch_times <= 50000 THEN '10001~50000'
    WHEN watch_times > 50000 AND watch_times <= 100000 THEN '50001~100000'
    WHEN watch_times > 100000 THEN '>100000'
    ELSE 'other'
    END AS watch_times_zone 
    FROM jijin_vid_watch_times_20160307_20160313)t1  """  
   
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
#   观看集锦视频时长分布 
    tdw.WriteLog("== jijin_watch_time_zone ==")
    sql = """INSERT TABLE jijin_watch_time_zone
SELECT 
t1.platform,
t1.watch_time_zone,
COUNT(DISTINCT(t1.uin)) as user_num,
COUNT(*) as watch_times 
FROM 
(SELECT 
platform,
CASE 
WHEN du >0 AND du <=5 THEN '0~5s'
WHEN du >5 AND du <=10 THEN '5s~10s'
WHEN du >10 AND du <=30 THEN '10s~30s'
WHEN du >30 AND du <=60 THEN '30s~60s'
WHEN du >60 AND du <=90 THEN '60s~90s'
WHEN du >90 AND du <=120 THEN '90s~120s'
WHEN du >120 AND du <=180 THEN '120s~180s'
WHEN du >180 AND du <=270 THEN '180s~270s'
WHEN du >270 AND du <=360 THEN '270s~360s'
WHEN du >360 AND du <=600 THEN '360s~600s'
ELSE 'other'
END AS watch_time_zone, 
vid,
uin
FROM original_vid_watch_num_times_20160307_20160313)t1
JOIN 
(SELECT 
vid 
FROM jijin_vid)t2 
ON t1.vid = t2.vid 
GROUP BY t1.platform,t1.watch_time_zone """

    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    