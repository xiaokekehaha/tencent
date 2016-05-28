#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_content_time.py
# 功能描述:     手游宝APP内容耗费时长统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     teg_mta_intf :: ieg_shouyoubao 
# 创建人名:     llianli
# 创建日期:     2015-11-30
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

    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##建立表，
    sql = '''
              CREATE TABLE IF NOT EXISTS syb_app_content_time
    (
    dtstatdate INT COMMENT '统计时间',
    id BIGINT COMMENT 'APPID',
    av STRING COMMENT '版本号',
    game_id STRING COMMENT '游戏ID',
    article_id STRING COMMENT '文章ID',
    ei STRING COMMENT '事件类型',
    time BIGINT COMMENT '耗费时长(s)',
    pv BIGINT COMMENT '阅读PV',
    uv BIGINT COMMENT '阅读UV'
    )
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM syb_app_content_time WHERE  dtstatdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据写入表中
    sql = ''' 
            INSERT TABLE syb_app_content_time
        SELECT
        %s AS dtstatdate,
        id,
        CASE WHEN GROUPING(av) = 1 THEN '-100' ELSE av END AS av,
        CASE WHEN GROUPING(game_id) = 1 THEN '-100' ELSE game_id END AS game_id,
        CASE WHEN GROUPING(article_id) = 1 THEN '-100' ELSE article_id END AS article_id,
        ei,
        SUM(du) AS du,
        COUNT(*) AS pv,
        COUNT(DISTINCT uin_info) AS uv
        FROM
        (
        SELECT
        id,
        av,
        CASE WHEN id = 1100679302 THEN get_json_object(kv,'$.gameId')  ELSE  get_json_object(kv,'$.gameid') END AS game_id,
        CASE
            WHEN id = 1100679302 AND ei = 'TOPIC_STAY_TIME' THEN get_json_object(kv,'$.topicId')
            WHEN id = 1100679302 AND ei = 'MGC_ZONE_NEWS_DETAIL_STAY_TIME' THEN get_json_object(kv,'$.newsId')
            
            WHEN id =1200679337 AND ei = 'MGC_TOPIC_TIME_STAY' THEN get_json_object(kv,'$.topicid')
            WHEN id =1200679337 AND ei = 'MGC_LIST_DETAIL_DURATION' THEN get_json_object(kv,'$.articleID')
            
        END AS article_id,
         
        CASE 
            WHEN (id = 1100679302 AND ei = 'TOPIC_STAY_TIME')  OR (id =1200679337 AND ei = 'MGC_TOPIC_TIME_STAY') THEN '话题停留时长'
            WHEN id = 1100679302 AND ei = 'MGC_ZONE_NEWS_DETAIL_STAY_TIME' AND get_json_object(kv,'$.newsType')  = '0' THEN '资讯-攻略时长'
            WHEN id = 1100679302 AND ei = 'MGC_ZONE_NEWS_DETAIL_STAY_TIME' AND get_json_object(kv,'$.newsType')  = '1' THEN '资讯-公告时长'
            WHEN id = 1200679337 AND ei = 'MGC_LIST_DETAIL_DURATION'  AND get_json_object(kv,'$.articleType') = '303' THEN '资讯-攻略时长'
            WHEN id = 1200679337 AND ei = 'MGC_LIST_DETAIL_DURATION'  AND get_json_object(kv,'$.articleType') = '304' THEN '资讯-公告时长'
        END AS ei,
        du, 
        concat(ui,mc) AS uin_info
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND ei in ('TOPIC_STAY_TIME','MGC_ZONE_NEWS_DETAIL_STAY_TIME','MGC_TOPIC_TIME_STAY','MGC_LIST_DETAIL_DURATION') AND du < 7200
        )t
        WHERE ei IS NOT NULL
        GROUP BY id,ei,cube(av,game_id,article_id)
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    sql = ''' 
            INSERT TABLE syb_app_content_time
        SELECT
        %s AS dtstatdate,
        id,
        CASE WHEN GROUPING(av) = 1 THEN '-100' ELSE av END AS av,
        CASE WHEN GROUPING(game_id) = 1 THEN '-100' ELSE game_id END AS game_id,
        CASE WHEN GROUPING(article_id) = 1 THEN '-100' ELSE article_id END AS article_id,
        ei,
        SUM(du) AS du,
        COUNT(*) AS pv,
        COUNT(DISTINCT uin_info) AS uv
        FROM
        (
        SELECT
        id,
        av,
        CASE WHEN id = 1100679302 THEN get_json_object(kv,'$.gameId')  ELSE  get_json_object(kv,'$.gameid') END AS game_id,
        CASE
            
            WHEN id = 1100679302 AND ei = 'MGC_ZONE_NEWS_DETAIL_STAY_TIME' THEN get_json_object(kv,'$.newsId')
            WHEN id =1200679337 AND ei = 'MGC_LIST_DETAIL_DURATION' THEN get_json_object(kv,'$.articleID')
            
        END AS article_id,
         
        CASE 
            WHEN id = 1100679302 AND ei = 'MGC_ZONE_NEWS_DETAIL_STAY_TIME' AND get_json_object(kv,'$.newsType')  in ( '0','1') THEN '资讯时长'
            WHEN id = 1200679337 AND ei = 'MGC_LIST_DETAIL_DURATION'  AND get_json_object(kv,'$.articleType') in ( '303','304') THEN '资讯时长'
        END AS ei,
        du, 
        concat(ui,mc) AS uin_info
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND ei in ('TOPIC_STAY_TIME','MGC_ZONE_NEWS_DETAIL_STAY_TIME','MGC_TOPIC_TIME_STAY','MGC_LIST_DETAIL_DURATION') AND du < 7200
        )t
        WHERE ei IS NOT NULL
        GROUP BY id,ei,cube(av,game_id,article_id)
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    