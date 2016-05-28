#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_topic_all_btn.py
# 功能描述:     手游宝内feeds页面等相关按钮点击
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.syb_topic_btn_pvuv
# 数据源表:     teg_mta_intf::ieg_shouyoubao
# 创建人名:     llianli
# 创建日期:     2015-07-30
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    ##手游宝话题数据feeds页面等相关按钮点击结果库
    sql = """
            CREATE TABLE IF NOT EXISTS syb_topic_btn_click_water
            (
            fdate string,
            id string,
            game_id bigint,
            topic_id string,
            btn string,
            pv bigint,
            uv bigint
            ) """
    res = tdw.execute(sql)


    sql = """
         INSERT TABLE syb_topic_btn_click_water
         SELECT 
         %s as fdate,
         id,
         CASE WHEN GROUPING(game_id) = 1 THEN '-100' ELSE game_id END AS game_id,
         CASE WHEN GROUPING(topic_id) = 1 THEN '-100' ELSE topic_id END AS topic_id,
         ei as btn,
         COUNT(*) as pv,
         COUNT(DISTINCT uin) as uv 
         FROM  
         (
         SELECT 
        id,
        CASE 
            WHEN  id = 1100679302 THEN get_json_object(kv,'$.uin')
            WHEN  id = 1200679337 THEN concat(ui,mc) 
            ELSE NULL
        END AS uin,
        CASE 
            WHEN  id = 1100679302 AND ei = 'TOPIC_ACT_ENTRY'  THEN get_json_object(kv,'$.GameID')
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_CLICK'  THEN get_json_object(kv,'$.GameID')
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_TREND'  THEN get_json_object(kv,'$.gameId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_MORE_HOT'  THEN get_json_object(kv,'$.gameId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_MORE_TIME'  THEN get_json_object(kv,'$.gameId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_REFRESH_FEEDS'  THEN get_json_object(kv,'$.gameId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_LIKE_TREND'  THEN get_json_object(kv,'$.gameId')
            
            WHEN id = 1200679337 AND ei = 'TOPIC_VIEW_LOAD' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPIC_SHARE_CLICK' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei in ( 'TOPICITEM_SHARE','TOPICITEM_SHARE_CLICK') THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPIC_CLICK_MORE_HOT' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPIC_PULLMORE_DOWN' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPIC_PULLMORE_UP' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPIC_FAVOUR_CLICK' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPICITEM_FAVOUR_COMMENT' THEN get_json_object(kv,'$.gameid')
            
            ELSE NULL
        END AS game_id,
        
        CASE 
            WHEN  id = 1100679302 AND ei = 'TOPIC_ACT_ENTRY'  THEN get_json_object(kv,'$.topicid')
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_CLICK'  THEN get_json_object(kv,'$.topicid')
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_TREND'  THEN get_json_object(kv,'$.topicId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_MORE_HOT'  THEN get_json_object(kv,'$.topicId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_MORE_TIME'  THEN get_json_object(kv,'$.topicId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_REFRESH_FEEDS'  THEN get_json_object(kv,'$.topicId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_LIKE_TREND'  THEN get_json_object(kv,'$.topicId')
            
            
            WHEN id = 1200679337 AND ei = 'TOPIC_VIEW_LOAD' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPIC_SHARE_CLICK' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei in ( 'TOPICITEM_SHARE','TOPICITEM_SHARE_CLICK') THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPIC_CLICK_MORE_HOT' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPIC_PULLMORE_DOWN' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPIC_PULLMORE_UP' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPIC_FAVOUR_CLICK' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPICITEM_FAVOUR_COMMENT' THEN get_json_object(kv,'$.topicid')
            
            
            ELSE NULL
        END AS topic_id,
        
        
        CASE 
            WHEN ( id = 1100679302 AND ei = 'TOPIC_ACT_ENTRY' ) OR (id = 1200679337 AND ei = 'TOPIC_VIEW_LOAD')  THEN 'topic_view'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_SHARE_CLICK' ) OR (id = 1200679337 AND ei = 'TOPIC_SHARE_CLICK')  THEN 'topic_feeds_share'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_SHARE_TREND' ) OR (id = 1200679337 AND ei in ( 'TOPICITEM_SHARE','TOPICITEM_SHARE_CLICK'))  THEN 'topic_detail_share'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_MORE_HOT' ) OR (id = 1200679337 AND ei = 'TOPIC_CLICK_MORE_HOT')  THEN 'topic_more_hot'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_MORE_TIME' ) OR (id = 1200679337 AND ei = 'TOPIC_PULLMORE_DOWN')  THEN 'topic_push_load'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_REFRESH_FEEDS' ) OR (id = 1200679337 AND ei = 'TOPIC_PULLMORE_UP')  THEN 'topic_pull_refresh'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_LIKE_TREND' AND ( get_json_object(kv,'$.clickPos') in( '3','ON_TOPIC_TREND','null') or get_json_object(kv,'$.clickPos') is null ) ) OR (id = 1200679337 AND ei = 'TOPIC_FAVOUR_CLICK')  THEN 'topic_feeds_zan'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_LIKE_TREND' AND get_json_object(kv,'$.clickPos') in ('1','2','ON_TREND','BOTTOM_BAR')) OR (id = 1200679337 AND ei = 'TOPICITEM_FAVOUR_COMMENT')  THEN 'topic_detail_zan'
            
             
            ELSE NULL 
        END AS ei
        
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s
        )t 
        WHERE game_id is NOT NULL and topic_id is NOT NULL and ei IS NOT NULL
        GROUP BY ei,id,CUBE(game_id,topic_id)
   """%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##计算总体的点赞和分享
    sql = """
         INSERT TABLE syb_topic_btn_click_water
         SELECT 
         %s as fdate,
         id,
         CASE WHEN GROUPING(game_id) = 1 THEN '-100' ELSE game_id END AS game_id,
         CASE WHEN GROUPING(topic_id) = 1 THEN '-100' ELSE topic_id END AS topic_id,
         ei as btn,
         COUNT(*) as pv,
         COUNT(DISTINCT uin) as uv 
         FROM  
         (
                 SELECT 
        id,
        CASE 
            WHEN  id = 1100679302 THEN get_json_object(kv,'$.uin')
            WHEN  id = 1200679337 THEN ext4 
            ELSE NULL
        END AS uin,
        CASE 
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_CLICK'  THEN get_json_object(kv,'$.GameID')
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_TREND'  THEN get_json_object(kv,'$.gameId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_LIKE_TREND'  THEN get_json_object(kv,'$.gameId')
            
            WHEN id = 1200679337 AND ei = 'TOPIC_SHARE_CLICK' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei in ( 'TOPICITEM_SHARE','TOPICITEM_SHARE_CLICK') THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPIC_FAVOUR_CLICK' THEN get_json_object(kv,'$.gameid')
            WHEN id = 1200679337 AND ei = 'TOPICITEM_FAVOUR_COMMENT' THEN get_json_object(kv,'$.gameid')
            
            ELSE NULL
        END AS game_id,
        
        CASE 
           
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_CLICK'  THEN get_json_object(kv,'$.topicid')
            WHEN  id = 1100679302 AND ei = 'TOPIC_SHARE_TREND'  THEN get_json_object(kv,'$.topicId')
            WHEN  id = 1100679302 AND ei = 'TOPIC_LIKE_TREND'  THEN get_json_object(kv,'$.topicId')
            
            
            WHEN id = 1200679337 AND ei = 'TOPIC_SHARE_CLICK' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei in ( 'TOPICITEM_SHARE','TOPICITEM_SHARE_CLICK') THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPIC_FAVOUR_CLICK' THEN get_json_object(kv,'$.topicid')
            WHEN id = 1200679337 AND ei = 'TOPICITEM_FAVOUR_COMMENT' THEN get_json_object(kv,'$.topicid')
            
            
            ELSE NULL
        END AS topic_id,
        
        
        CASE 
            WHEN ( id = 1100679302 AND ei = 'TOPIC_SHARE_CLICK' ) OR (id = 1200679337 AND ei = 'TOPIC_SHARE_CLICK')  THEN 'topic_all_share'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_SHARE_TREND' ) OR (id = 1200679337 AND ei in ( 'TOPICITEM_SHARE','TOPICITEM_SHARE_CLICK') )  THEN 'topic_all_share'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_LIKE_TREND' AND get_json_object(kv,'$.clickPos') = '3') OR (id = 1200679337 AND ei = 'TOPIC_FAVOUR_CLICK')  THEN 'topic_all_zan'
            WHEN ( id = 1100679302 AND ei = 'TOPIC_LIKE_TREND' AND get_json_object(kv,'$.clickPos') != '3') OR (id = 1200679337 AND ei = 'TOPICITEM_FAVOUR_COMMENT')  THEN 'topic_all_zan' 
            ELSE NULL 
        END AS ei
        
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s
        )t 
        WHERE game_id is NOT NULL and topic_id is NOT NULL and ei IS NOT NULL
        GROUP BY ei,id,CUBE(game_id,topic_id)
   """%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    