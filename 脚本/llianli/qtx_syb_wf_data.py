#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_wf_data.py
# 功能描述:     手游宝全民突击数据
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_wf_click_data
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
            CREATE TABLE IF NOT EXISTS tb_syb_wf_click_data
            (
            fdate int,
            id  string,
            av  string,
            ei string,
            pv bigint,
            uv bigint,
            total_time bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_syb_wf_click_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = ''' 
    insert table tb_syb_wf_click_data
    SELECT 
    %s,
    id,
    av,
    ei,
    COUNT(*) as pv,
    COUNT(DISTINCT uin) as uv,
    0
    FROM  
    (    
    SELECT 
    id,
    av,
    CASE 
        WHEN id = 1200679337 THEN ext3
        WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
        ELSE '0' 
     END AS uin,
CASE
     WHEN 
     ( ei = 'WF_HOME_AD_CLICK' and get_json_object(kv,'$.position') = '0') or 
     ( ei = 'AD_CLICK' and get_json_object(kv,'$.position') = '0') or 
     ei = 'WF_Guide_Gift' or 
     ( ei = 'GENERAL_AD_CLICK' and get_json_object(kv,'$.game_id') = '101' and get_json_object(kv,'$.position') = '0' )
    THEN 'adv_1'  
        
    WHEN 
     ( ei = 'WF_HOME_AD_CLICK' and get_json_object(kv,'$.position') = '1') or 
     ( ei = 'AD_CLICK' and get_json_object(kv,'$.position') = '1') or 
     ( ei = 'GENERAL_AD_CLICK' and get_json_object(kv,'$.game_id') = '101' and get_json_object(kv,'$.position') = '1' )
    THEN 'adv_2' 
    
    WHEN 
     ( ei = 'WF_HOME_AD_CLICK' and get_json_object(kv,'$.position') = '2') or 
     ( ei = 'AD_CLICK' and get_json_object(kv,'$.position') = '2') or 
     ( ei = 'GENERAL_AD_CLICK' and get_json_object(kv,'$.game_id') = '101' and get_json_object(kv,'$.position') = '2' )
    THEN 'adv_3' 
    
    
    WHEN 
     ( ei = 'WF_HOME_AD_CLICK' and get_json_object(kv,'$.position') = '3') or 
     ( ei = 'AD_CLICK' and get_json_object(kv,'$.position') = '3') or 
     ( ei = 'GENERAL_AD_CLICK' and get_json_object(kv,'$.game_id') = '101' and get_json_object(kv,'$.position') = '3' )
    THEN 'adv_4'  
    
    
    
    WHEN 
     ( ei = 'WF_slides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '0' and get_json_object(kv,'$.tab') = '首页') or 
     ( ei = 'Sides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '0') 
    THEN 'first_page_slides_1'
    
    
    
    WHEN 
     ( ei = 'WF_slides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '1' and get_json_object(kv,'$.tab') = '首页') or 
     ( ei = 'Sides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '1') 
    THEN 'first_page_slides_2'
    
    
    WHEN 
     ( ei = 'WF_slides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '2' and get_json_object(kv,'$.tab') = '首页') or 
     ( ei = 'Sides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '2') 
    THEN 'first_page_slides_3'
    
    
    
    WHEN 
     ( ei = 'WF_slides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '0' and get_json_object(kv,'$.tab') = '攻略') 
    THEN 'stragey_slides_1'
    
    
    
    WHEN 
     ( ei = 'WF_slides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '1' and get_json_object(kv,'$.tab') = '攻略')
    THEN 'stragey_slides_2'
    
    
    WHEN 
     ( ei = 'WF_slides_check' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '2' and get_json_object(kv,'$.tab') = '攻略') 
    THEN 'stragey_slides_3'
    
    WHEN 
     ei in  ('SIGN_CLICK','WF_HOME_SIGN_CLICK' ) or 
     (ei = 'GENERAL_SIGN_CLICK' and get_json_object(kv,'$.game_id') = '101' ) 
    THEN 'sign'
    
    
    WHEN 
     ei = 'WF_TAB_SELECTED' and get_json_object(kv,'$.gameID') = '101' 
    THEN concat ('tab_',get_json_object(kv,'$.tabId'))
    
    
    WHEN 
     ei = 'VIDEO_AD_CLICK' and (get_json_object(kv,'$.game_name') = 'WEFIRE' or get_json_object(kv,'$.game_name') = 'WF')
    THEN 'video_adv'
    
    
    WHEN 
     ei = 'VIDEO_ITEM_CLICK' and ( get_json_object(kv,'$.game_name') = 'WEFIRE' or get_json_object(kv,'$.game_name') = 'WF') 
    THEN 'video_item_click'

    
    WHEN 
    ei = 'WF_news_check' and get_json_object(kv,'$.gameid') = '101' 
    THEN concat('news_item',get_json_object(kv,'$.tab'))
    
    WHEN 
    ( ei in ('WF_advs_check') and get_json_object(kv,'$.index') = '1' ) or 
    ei = 'WF_GIFT_CLICK' or 
    (ei = 'MGC_PROMOTE_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '1' )
    THEN 'adv_1'
    
    WHEN 
    ( ei in ('WF_advs_check') and get_json_object(kv,'$.index') = '2' ) or 
    (ei = 'MGC_PROMOTE_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '2' )
    THEN 'adv_2'
    
    WHEN 
    ( ei in ('WF_advs_check') and get_json_object(kv,'$.index') = '3' ) or 
    (ei = 'MGC_PROMOTE_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.index') = '3' )
    THEN 'adv_3'
    
    
    WHEN 
    ( ei in ('WF_slides_check') and get_json_object(kv,'$.index') = '0' ) or 
    (ei = 'MGC_SCROLLER_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.tabId') = '1' and get_json_object(kv,'$.index') = '0' )
    THEN 'first_page_slides_1'
    
    WHEN 
    ( ei in ('WF_slides_check') and get_json_object(kv,'$.index') = '1' ) or 
    (ei = 'MGC_SCROLLER_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.tabId') = '1' and get_json_object(kv,'$.index') = '1' )
    THEN 'first_page_slides_2'
    
    WHEN 
    ( ei in ('WF_slides_check') and get_json_object(kv,'$.index') = '2' ) or 
    (ei = 'MGC_SCROLLER_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.tabId') = '1' and get_json_object(kv,'$.index') = '2' )
    THEN 'first_page_slides_3'
    
    
    WHEN 
    ( ei in ('WF_strategy_slides_check') and get_json_object(kv,'$.index') = '0' ) or 
    (ei = 'MGC_SCROLLER_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.tabId') = '2' and get_json_object(kv,'$.index') = '0' )
    THEN 'stragey_slides_1'
    
    WHEN 
    ( ei in ('WF_strategy_slides_check') and get_json_object(kv,'$.index') = '1' ) or 
    (ei = 'MGC_SCROLLER_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.tabId') = '2' and get_json_object(kv,'$.index') = '1' )
    THEN 'stragey_slides_2'
    
    WHEN 
    ( ei in ('WF_strategy_slides_check') and get_json_object(kv,'$.index') = '2' ) or 
    (ei = 'MGC_SCROLLER_CLICKED' and get_json_object(kv,'$.gameid') = '101' and get_json_object(kv,'$.tabId') = '2' and get_json_object(kv,'$.index') = '2' )
    THEN 'stragey_slides_3'
    
    WHEN 
     ei = 'WF_MAINVIEW_CLICK_SIGNIN' or 
     ei = 'MGC_T_SIGN'  and get_json_object(kv,'$.gameid') = '101'
    THEN 'sign'
    
    WHEN 
       ei = 'WF_INFONEWS_LIST_TAB_CLICK' or 
       ( ei = 'MGC_TEMPLATE_TAB_EVENT' and  get_json_object(kv,'$.gameId') = '101' and get_json_object(kv,'$.tabId') = '1') 
    THEN 'tab_1'
    
    WHEN 
       ei = 'WF_STRATEGY_LIST_TAB_CLICK' or 
       ( ei = 'MGC_TEMPLATE_TAB_EVENT' and  get_json_object(kv,'$.gameId') = '101' and get_json_object(kv,'$.tabId') = '2') 
    THEN 'tab_2'
    
    WHEN 
       ( ei = 'MGC_TEMPLATE_TAB_EVENT' and  get_json_object(kv,'$.gameId') = '101' and get_json_object(kv,'$.tabId') != '1' and get_json_object(kv,'$.tabId') != '2') 
    THEN  concat ('tab_',get_json_object(kv,'$.tabId'))
    
    WHEN 
       ei = 'WF_VIDEO_LIST_AD_CLICK' or 
       ( ei = 'MGC_VIDEO_TOP_CLICK' and get_json_object(kv,'$.gameid') = '101' ) 
    THEN 'video_adv'
    
    
    WHEN 
       ei = 'WF_VIDEO_LIST_ITEM_CLICK' or 
       ( ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' and get_json_object(kv,'$.gameId') = '101' and get_json_object(kv,'$.tabId') = '4' ) 
    THEN 'video_item_click'
    
    
    WHEN 
        ei = 'WF_INFONEWS_LIST_ITEM_CLICK' or 
        ( ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' and get_json_object(kv,'$.gameId') = '101' and get_json_object(kv,'$.tabId') = '1' ) 
    THEN 'news_item_1'
    
    
    WHEN 
        ei = 'WF_STRATEGY_LIST_ITEM_CLICK' or 
        ( ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' and get_json_object(kv,'$.gameId') = '101' and get_json_object(kv,'$.tabId') = '2' ) 
    THEN 'news_item_2'
    
    
    WHEN 
        ( ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' and get_json_object(kv,'$.gameId') = '101' and ( get_json_object(kv,'$.tabId') != '1'  and get_json_object(kv,'$.tabId') != '2') ) 
    THEN concat('news_item_',get_json_object(kv,'$.tabId'))
    
     
     ELSE 'other_ei'
     
 END AS ei 
 FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s and cdate = %s 
 )t where ei != 'other_ei' GROUP by id,ei,av
    '''%(sDate,sDate,sDate)
    
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##总的资讯点击量
    sql = '''
    insert table tb_syb_wf_click_data
             SELECT 
        %s,
         id,
        av,
         ei,
         COUNT(*) as pv,
         COUNT(DISTINCT uin) as uv ,
         SUM(du)
         FROM  
         (
         SELECT 
         id,
         av,
         CASE 
                WHEN id = 1200679337 THEN ext3 
                WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
                ELSE '0' 
        END AS uin,
         du,
         CASE 
            WHEN ei = 'WF_news_check'   and get_json_object(kv,'$.gameid') = '101' THEN 'news_total'
            WHEN ei in ( 'WF_INFONEWS_LIST_ITEM_CLICK','WF_ANNOUNCE_LIST_ITEM_CLICK','WF_STRATEGY_LIST_ITEM_CLICK') or 
                 ( ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' and  get_json_object(kv,'$.gameId') = '101' )THEN 'news_total'
            
            else 'other_ei'
            end
            AS ei
         FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s and cdate = %s 
         )t where ei != 'other_ei' GROUP BY id,ei ,av
    ''' %(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    tdw.WriteLog("== end OK ==")
    
    
    
    ##总的DAU
    sql = '''
    insert table tb_syb_wf_click_data
            SELECT 
         %s,
         id,
        av,
         ei,
         COUNT(*) as pv,
         COUNT(DISTINCT uin) as uv ,
         SUM(du)
         FROM  
         (
         SELECT 
         id,
         av,
         CASE 
                WHEN id = 1200679337 THEN ext3 
                WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
                ELSE '0' 
        END AS uin,
         du,
         CASE 
            WHEN 
              ei = 'WF_HOME_DAU' or   
              ( ei = 'HOME_DAU' and get_json_object(kv,'$.gameID') = '101' ) or 
              ( ei = 'WF_TAB_SELECTED' and get_json_object(kv,'$.gameID') = '101' ) or 
              (ei in ('WF_HOME_AD_CLICK','AD_CLICK','WF_slides_check','Strategy_tab_Cleck')) or 
              (ei = 'WF_news_check' and get_json_object(kv,'$.gameid') = '101' ) or 
              ( ei = 'VIDEO_AD_CLICK' and get_json_object(kv,'$.game_name') = 'wf') or 
              ( ei = 'VIDEO_LABLE_CLICK' and get_json_object(kv,'$.game_name') = 'wf') or 
               ( ei = 'VIDEO_ITEM_CLICK' and get_json_object(kv,'$.game_name') = 'wf')
           THEN 'dau'
           
           
            WHEN ei in ( 'WF_INFONEWS_LIST_TAB_CLICK','WF_STRATEGY_LIST_TAB_CLICK','WF_ANNOUNCE_LIST_TAB_CLICK','WF_ENTERTAIN_LIST_TAB_CLICK','WF_VIDEO_LIST_TAB_CLICK',
            'WF_INFONEWS_LIST_ITEM_CLICK','WF_ANNOUNCE_LIST_ITEM_CLICK','WF_STRATEGY_LIST_ITEM_CLICK','WF_ENTERTAIN_LIST_ITEM_CLICK',
            'WF_slides_check','WF_strategy_slides_check','WF_STRATEGY_LIST_ITEM_LABEL_CLICK','WF_VIDEO_LIST_AD_CLICK','WF_VIDEO_LIST_TAG_CLICK',
            'WF_VIDEO_LIST_ITEM_CLICK') or       
            ( ei in ( 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' ,'MGC_TEMPLATE_TAB_EVENT','MGC_TEMPLATE_TAB_LABEL_EVENT','MGC_MTA_DAU_REPORT_EVENT') and  get_json_object(kv,'$.gameId') = '101' ) or
            ( ei in ( 'MGC_PROMOTE_CLICKED','MGC_SCROLLER_CLICKED','MGC_VIDEO_TOP_CLICK')  and  get_json_object(kv,'$.gameid') = '101' )   
            THEN 'dau'
         else 'other_ei'
         end 
            AS ei
         FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s and cdate = %s 
         )t where ei != 'other_ei' GROUP BY id,ei ,av
    ''' %(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    tdw.WriteLog("== end OK ==")
    
    
    
    