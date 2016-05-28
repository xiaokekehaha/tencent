#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_ttkp_data.py
# 功能描述:     手游宝天天炫斗数据
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_ttxd_click_data
# 数据源表:     teg_mta_intf::ieg_shouyoubao 
# 创建人名:     llianli
# 创建日期:     2015-04-30
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



    sql = """
            CREATE TABLE IF NOT EXISTS tb_syb_ttxd_click_data
            (
            fdate int,
            id  string,
            ei string,
            pv bigint,
            uv bigint,
            total_time bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_syb_ttxd_click_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = ''' 
    insert table tb_syb_ttxd_click_data
    SELECT 
%s,
id,
ei,
COUNT(*) as pv,
COUNT(DISTINCT uin) as uv,
0
FROM  
(
SELECT 
id,
CASE 
        WHEN id = 1200679337 THEN get_json_object(kv, '$.syb_id')
        WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
        ELSE '0' 
     END AS uin,
CASE
     WHEN 
     id = 1100679302 AND 
     (
      ei = 'TTXD_TAB_SELECTED' OR
      ei = 'TTXD_HOME_FIGHT'  OR
      ei = 'TTXD_HOME_PK'  OR
      ei = 'TTXD_AdsRead'  OR
     ( ei = 'WF_news_check' AND get_json_object(kv,'$.gameid') = '102') OR
     ( ei = 'VIDEO_ITEM_CLICK' AND get_json_object(kv,'$.game_name') = 'TTXD') OR
     ( ei = 'VIDEO_AD_CLICK' AND get_json_object(kv,'$.game_name') = 'TTXD') OR
     ( ei = 'Strategy_tab_Cleck' AND get_json_object(kv,'$.gameid') = '102') OR 
     ei = 'TTXD_MTA_MGC_VIDEO_LABEL_CLICK' OR
     (ei = 'TOPIC_ACT_ENTRY' AND get_json_object(kv,'$.GameID') = '102' ) 
     ) 
    THEN 'TTXD_DAU'
 
 
     WHEN 
     id = 1200679337 AND
     (
     ei = 'TTXD_INFONEWS_LIST_TAB_CLICK'  OR
     ei = 'TTXD_ANNOUNCE_LIST_TAB_CLICK' OR
     ei = 'TOPIC_SQUARE_TAB_CLICK' OR
     ei = 'TTXD_STRATEGY_LIST_TAB_CLICK' OR
     ei = 'TTXD_VIDEO_LIST_TAB_CLICK' OR
     
     ei = 'TTXD_HOME_FIGHT' OR 
     ei = 'TTXD_PK_Show_Battle_Detail' OR
     ei = 'TTXD_GIFT_Guide_Click_Num' OR
     ei = 'TTXD_AdsRead' OR
     
     (ei = 'MGC_LIST_DETAIL_VISITED'  AND get_json_object(kv,'$.gameid') = '102') OR
     ei = 'TTXD_VIDEO_LIST_ITEM_CLICK'  OR
     ei = 'TTXD_STRATEGY_LIST_ITEM_LABEL_CLICK' OR
     ei = 'TTXD_VIDEO_LIST_AD_CLICK'  OR
     ei = 'TTXD_VIDEO_LIST_TAG_CLICK' OR
     ( ei = 'TOPIC_ACT_ENTRY' AND get_json_object(kv,'$.gameid') = '102' ) 
     ) 
     THEN 'TTXD_DAU'
     
     ELSE 'other_ei'
     
 END AS ei 
 FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s and cdate = %s 
 )t GROUP by id,ei
    '''%(sDate,sDate,sDate)
    
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    sql = '''
    insert table tb_syb_ttxd_click_data
            SELECT 
         %s,
         id,
         ei,
         COUNT(*) as pv,
         COUNT(DISTINCT uin) as uv ,
         SUM(du)
         FROM  
         (
         SELECT 
         id,
         CASE 
                WHEN id = 1200679337 THEN get_json_object(kv, '$.syb_id')
                WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
                ELSE '0' 
        END AS uin,
         du,
         CASE 
            WHEN ei = 'TTXD_TAB_SELECTED'  THEN    concat(ei,'-',get_json_object(kv,'$.selectIndex'))
            WHEN ei = 'TTXD_HOME_FIGHT' AND id =1100679302   THEN ei
            WHEN ei = 'TTXD_HOME_PK'   THEN ei
            WHEN ei = 'GENERAL_SIGN_CLICK' AND get_json_object(kv,'$.gameID') = '102' THEN ei
            WHEN ei = 'TTXD_AdsRead' AND id = 1100679302 THEN concat(ei,'-',get_json_object(kv,'$.location'))
            WHEN ei = 'WF_news_check' AND get_json_object(kv,'$.gameid') = '102' then ei
            WHEN ei = 'VIDEO_ITEM_CLICK' AND get_json_object(kv,'$.game_name') = 'TTXD' THEN ei
            WHEN ei in ('TTXD_PK_LIST_ITEM_CLICK','TTXD_PK_LIST_TIPS_SHOW','TTXD_PK_INFO_HEADER_CLICK','TTXD_PAGE_TIME','TTXD_PK_LIST_SHARE','TTXD_PK_INFO_SHARE') THEN ei
            WHEN ei = 'news_url_share' AND get_json_object(kv,'$.gameid') = '102' THEN ei
            
            WHEN ei in ('TTXD_INFONEWS_LIST_TAB_CLICK', 
                        'TTXD_ANNOUNCE_LIST_TAB_CLICK',
                        'TTXD_STRATEGY_LIST_TAB_CLICK',
                        'TTXD_VIDEO_LIST_TAB_CLICK',
                        'TTXD_PK_Show_Battle_Detail',
                        'TTXD_GIFT_Guide_Click_Num',
                        'TTXD_VIDEO_LIST_ITEM_CLICK',
                        'TTXD_PK_Flow_Click_Battle_Detail',
                        'TTXD_PK_Appear_Raiders',
                        'TTXD_PAGE_TIME',
                        'TTXD_Fighting_Trend_Share',
                        'TTXD_PK_Flow_Share',
                        'TTXD_PK_Detail_Share'
                            ) THEN ei 
                            
            WHEN ei = 'TOPIC_SQUARE_TAB_CLICK' AND get_json_object(kv,'$.qtGameId') = '102' THEN ei
            WHEN ei = 'TTXD_HOME_FIGHT' AND id = 1200679337 THEN ei
            WHEN ei = 'TTXD_AdsRead' AND id = 1200679337  THEN concat(ei,'-',get_json_object(kv,'$.index'))
            WHEN ei = 'MGC_LIST_DETAIL_VISITED' AND get_json_object(kv,'$.gameid') = '102' THEN ei
            WHEN ei = 'TTXD_PK_Show_Flow' AND get_json_object(kv,'$.visitor_state') = '1' THEN ei
            END   
            
            AS ei
         FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s and cdate = %s 
         )t GROUP BY id,ei 
    ''' %(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    tdw.WriteLog("== end OK ==")
    
    
    
    