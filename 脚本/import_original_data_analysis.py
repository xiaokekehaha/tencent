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
#原始数据
    tdw.WriteLog("== original_vid_watch_num_times_20160307_20160313 ==")
    sql = """INSERT TABLE original_vid_watch_num_times_20160307_20160313
SELECT 
t1.sdate as sdate,
t1.platform as platform,
t1.uin as uin,
t1.vid as vid,
t1.du as du  
FROM 
(SELECT 
sdate,
CASE 
WHEN  ei='火线_视频播放页面时长' THEN 'cf'
WHEN (id ='1100678382' AND ei='hero_time_video') OR (id ='1200678382' AND ei='herotime_play_time' )THEN 'lol'
ELSE 'other'
END AS platform,
get_json_object(kv,'$.uin') as uin ,
get_json_object(kv,'$.vid') as vid,
du 
FROM teg_mta_intf::ieg_lol WHERE sdate>=20160307 and sdate<=20160313 AND du<600)t1 
WHERE t1.platform != 'other' """
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
   
#PC端
    tdw.WriteLog("== PC端 ==")
    sql = """INSERT TABLE original_vid_watch_num_times_20160307_20160313
SELECT 
r2.statis_date as statis_date,
r2.source as platform,
r2.uin as uin,
r2.vid as vid,
r2.pt as du 
FROM 
(SELECT 
r1.statis_date as statis_date,
r1.source as source,
r1.viewid as viewid,
r1.vid as vid,
r1.uid as uin,
max(r1.pt) as pt 
FROM 
(SELECT
statis_date, 
source,
viewid,
vid,
uid,
pt 
FROM 
hy::t_dw_mkt_qt_record_video_watch_uin_water
WHERE statis_date<=20160313 AND statis_date >= 20160307 AND pt<=600 AND 
(source=1 OR source=2 OR source=4 OR source=5 OR source=6 OR source=9 OR source=10 ))r1
GROUP BY r1.statis_date,r1.source,r1.viewid,r1.vid,r1.uid)r2"""
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
 