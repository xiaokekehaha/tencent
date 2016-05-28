#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_ttkp_data.py
# 功能描述:     手游宝推送日报数据表计算
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_ttkp_click_data
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


    ##计算游戏分开日新增和总体日新增与游戏之间的交叉关系
    sql = """
            CREATE TABLE IF NOT EXISTS tb_syb_ttkp_click_data
            (
            fdate int,
            id  string,
            ei string,
            pv bigint,
            uv bigint,
            total_time bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_syb_ttkp_click_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = ''' 
    insert table tb_syb_ttkp_click_data
    SELECT 
    sdate,
    id,
    ei,
    count(*) as pv,
    count(DISTINCT uin) as uv,
    SUM(du) as total_time 
    from 
    (
    SELECT 
    sdate,
    id,
    CASE 
        WHEN id = 1200679337 THEN get_json_object(kv, '$.syb_id')
        WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
        else '0' 
     END AS uin,
     du ,
     CASE 
         WHEN ei in ('TTKP_MTA_MGC_LAUNCH_ZONE_OPE','TTKP_MTA_MGC_LAUNCH_ZONE_DETAIL_OPE') then  'TTKP_MTA_MGC_LAUNCH_ZONE_OPE'
         WHEN ei = 'TTKP_TAB_SELECTED' then  concat(ei,'-',get_json_object(kv,'$.selectIndex'))
         WHEN ei = 'TTKP_ACTION_BAR_BACK' then ei  
         WHEN ei = 'WF_news_check' then concat(ei,'-',get_json_object(kv,'$.gameid'),'-',get_json_object(kv,'$.tab'))
         WHEN ei = 'Sides_check' then concat(ei,'-',get_json_object(kv,'$.gameid'))
		 WHEN ei = 'GENERAL_AD_CLICK' and get_json_object(kv,'$.ad_type') = 'HOME_AD1' then concat(ei,'-',get_json_object(kv,'$.game_name'),'-',get_json_object(kv,'$.position'))
		 WHEN ei = 'GENERAL_AD_CLICK' and get_json_object(kv,'$.ad_type') = 'STRATEGY_AD' then concat(ei,'-','STRATEGY_AD','-',get_json_object(kv,'$.game_name'),'-',get_json_object(kv,'$.position'))         
		 WHEN ei = 'TOPIC_ACT_ENTRY' then concat(ei,'-',get_json_object(kv,'$.GameID'))
         WHEN ei = 'news_url_share' then concat(ei,'-',get_json_object(kv,'$.gameid'),'-',get_json_object(kv,'$.newsType'))
         WHEN ei = 'TOPIC_SHARE_CLICK' THEN concat(ei,'-',get_json_object(kv,'$.GameID'))
         WHEN ei = 'TTKP_PAGE_TIME' THEN concat(ei,'-',get_json_object(kv,'$.pageClassName'))
         WHEN ei = 'TTKP_VIDEO_AD_CLICK' THEN ei
         
         WHEN ei in ('MGC_DAU_STATISTICS','MGC_HEAD_TAP_T','MGC_CHANGE_GAME_ZONE_T') THEN ei
         WHEN ei = 'MGC_TEMPLATE_TAB_EVENT' THEN concat(ei,'-',get_json_object(kv,'$.gameId'),'-',get_json_object(kv,'$.tabId'))
         WHEN ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' THEN concat(ei,'-',get_json_object(kv,'$.gameId'),'-',get_json_object(kv,'$.tabId'))
         WHEN ei = 'MGC_BANNER_CLICKED' THEN concat(ei,'-',get_json_object(kv,'$.gameid'),'-',get_json_object(kv,'$.index'))
         WHEN ei = 'MGC_LIST_DETAIL_DURATION' THEN concat(ei,'-',get_json_object(kv,'$.gameid'))
         WHEN ei = 'MGC_VIDEO_TOP_CLICK' THEN concat(ei,'-',get_json_object(kv,'$.gameid'))
         WHEN ei = 'MGC_LIST_DETAIL_SHARE_CLICKED' THEN concat(ei,'-',get_json_object(kv,'$.gameid'))
         ELSE 'other_ei' 
     end as ei 
    from teg_mta_intf::ieg_shouyoubao where sdate =%s
    )t
    GROUP by sdate,ei,id
    '''%(sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    