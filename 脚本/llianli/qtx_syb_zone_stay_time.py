#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zone_stay_time.py
# 功能描述:     手游宝专区停留时长统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_shouyoubao 
# 创建人名:     llianli
# 创建日期:     2015-10-27
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")


    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表,专区停留时长的表
    sql = '''
            CREATE TABLE IF NOT EXISTS tb_syb_zone_stay_time
            (
            dtstatdate INT,
            id int,
            sgameid STRING,
            stab_name STRING,
            uin_num BIGINT,
            total_time BIGINT
            )
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_zone_stay_time WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##每个专区的整体数据
    sql = ''' 
    INSERT TABLE tb_syb_zone_stay_time
            SELECT
    %s AS dtstatdate,
    CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
    CASE WHEN GROUPING(gameid) = 1 THEN '-100' ELSE gameid END AS gameid,
    '-100' AS tab_name,
    COUNT(DISTINCT uin) AS uin_num,
    SUM(du) AS total_time
    FROM 
    (
    SELECT 
    id,
    CASE 
        WHEN id = 1200679337 THEN get_json_object(kv,'$.syb_id') 
        WHEN id = 1100679302 THEN get_json_object(kv,'$.uin') 
        ELSE NULL
    END AS uin,
    CASE WHEN ei = 'MGC_PREFECTURE_DURATION' THEN get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_ZONE_STAY_TIME' THEN get_json_object(kv,'$.gameId') 
    END AS gameid,
    du
    FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND ei in ('MGC_PREFECTURE_DURATION','MGC_ZONE_STAY_TIME') AND du <= 1800
    )t
    GROUP BY cube(id,gameid)
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##每个专区的各个TAB数据数据
    sql = '''
    INSERT TABLE tb_syb_zone_stay_time
            SELECT
        %s AS dtstatdate,
        CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
        CASE WHEN GROUPING(gameid) = 1 THEN '-100' ELSE gameid END AS gameid,
        tab_name,
        COUNT(DISTINCT uin) AS uin_num,
        SUM(du) AS total_time
        FROM
        (
        SELECT
        id,
        CASE 
            WHEN id = 1200679337 THEN concat(ui,mc)
            WHEN id = 1100679302 THEN get_json_object(kv,'$.uin') 
            ELSE NULL
        END AS uin,
        CASE  WHEN ei = 'MGC_ZONE_TAB_STAY_TIME' THEN get_json_object(kv,'$.gameId') 
            ELSE 'unknow'
        END AS gameid,
        CASE  WHEN ei = 'MGC_ZONE_TAB_STAY_TIME' THEN get_json_object(kv,'$.tabName') 
              WHEN pi = 'MGCHomeViewController' THEN '首页'
              WHEN pi = 'MGCRaidersViewController' THEN '攻略'
              WHEN pi = 'MGCNoticeViewController' THEN '公告'
              WHEN pi = 'MGCNewVideoViewController' THEN '视频'
              WHEN pi = 'MGCTopicViewController' THEN '话题'
        ELSE 'unknow'
        END AS tab_name,
        du
        FROM 
        teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND ( ei = 'MGC_ZONE_TAB_STAY_TIME' 
        OR 
            pi in (
            'MGCHomeViewController',
            'MGCRaidersViewController',
            'MGCNoticeViewController',
            'MGCNewVideoViewController',
            'MGCTopicViewController' 
            )
        )
        AND du <= 1800
        
        )t GROUP BY tab_name,cube(id,gameid)
   '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    