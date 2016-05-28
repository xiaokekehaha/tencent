#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_none_special_zone_data.py
# 功能描述:     手游宝非专区数据表计算
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_none_special_zone_data
# 数据源表:     teg_mta_intf::ieg_shouyoubao 
# 创建人名:     llianli
# 创建日期:     2015-06-22
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
            CREATE TABLE IF NOT EXISTS tb_syb_none_special_zone_data
            (
            fdate int,
            id  string,
            gameid bigint,
            ei string,
            pv bigint,
            uv bigint,
            total_time bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_syb_none_special_zone_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = ''' 
    insert table tb_syb_none_special_zone_data
    SELECT 
    sdate,
    id,
    game_id,
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
         WHEN ei in ('QUICK_ENTRY_GIFT','QUICK_ENTRY_BBS','QUICK_ENTRY_AREA') then get_json_object(kv,'$.gameid')
         WHEN ei = 'MTA_MGC_CLICK_RECOMMEND_ARTICLE' then  get_json_object(kv,'$.gameID')
         WHEN ei = 'MTA_MGC_DOWNLOAD_BUTTON' then  get_json_object(kv,'$.gameID')
         WHEN ei = 'TAB_SELECTED' then  get_json_object(kv,'$.gameID')
         WHEN ei = 'WF_news_check' then  get_json_object(kv,'$.gameid')
         WHEN ei = 'VIDEO_ITEM_CLICK' then  get_json_object(kv,'$.gameId')
         WHEN ei = 'GENERAL_AD_CLICK' then   get_json_object(kv,'$.gameID')
         WHEN ei = 'HOME_DAU' then get_json_object(kv,'$.gameID')
         
         
         WHEN ei = 'MGC_GAME_DETAIL_ADVER_ENTRANCE_CLICK' then get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_RECOMMEND_CLICK' then get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_DOWNLOAD_CLICK' then get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_LANUCH' then get_json_object(kv,'$.gameid')         
         WHEN ei = 'MGC_TEMPLATE_TAB_EVENT' then get_json_object(kv,'$.gameId')
         WHEN ei = 'MGC_BANNER_CLICKED' THEN get_json_object(kv,'$.gameid')

         else null 
     end as game_id,

     case
         WHEN ei in ('QUICK_ENTRY_GIFT','QUICK_ENTRY_BBS','QUICK_ENTRY_AREA') then concat(ei,'-',get_json_object(kv,'$.inInstalledGameView')) 
         WHEN ei = 'MTA_MGC_CLICK_RECOMMEND_ARTICLE' then ei
         WHEN ei = 'MTA_MGC_DOWNLOAD_BUTTON' then  concat(ei,'-',get_json_object(kv,'$.inInstalledGameView'),'-',get_json_object(kv,'$.state'))
         WHEN ei = 'TAB_SELECTED' then  concat(ei,'-',get_json_object(kv,'$.tabId'))
         WHEN ei = 'WF_news_check' then  concat(ei,'-',get_json_object(kv,'$.tab'))         
         WHEN ei = 'VIDEO_ITEM_CLICK' then  ei
         WHEN ei = 'GENERAL_AD_CLICK' then   concat(ei,'-',get_json_object(kv,'$.position'))     
         WHEN ei = 'HOME_DAU' then   concat(ei,'-',get_json_object(kv,'$.game_installed'))

         WHEN ei = 'MGC_GAME_DETAIL_ADVER_ENTRANCE_CLICK' then concat(ei,'-',get_json_object(kv,'$.IS_INSTALL'),'-',get_json_object(kv,'$.NAME'))
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_RECOMMEND_CLICK' then ei
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_DOWNLOAD_CLICK' then ei
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_LANUCH' then ei     
         WHEN ei = 'MGC_TEMPLATE_TAB_EVENT' then concat(ei,'-',get_json_object(kv,'$.tabId'))
         WHEN ei = 'MGC_BANNER_CLICKED' THEN concat(ei,'-',get_json_object(kv,'$.index'))

         ELSE 'other_ei' 
     end as ei 
    from teg_mta_intf::ieg_shouyoubao where sdate =%s 
    )t 
    where game_id > 1000 and game_id != 200302130 and game_id != 200318727 and game_id != 100001335
    GROUP by sdate,game_id,ei,id
    '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##所有游戏合并在一起
    sql = ''' 
    insert table tb_syb_none_special_zone_data
    SELECT 
    sdate,
    id,
    -100,
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
         WHEN ei in ('QUICK_ENTRY_GIFT','QUICK_ENTRY_BBS','QUICK_ENTRY_AREA') then get_json_object(kv,'$.gameid')
         WHEN ei = 'MTA_MGC_CLICK_RECOMMEND_ARTICLE' then  get_json_object(kv,'$.gameID')
         WHEN ei = 'MTA_MGC_DOWNLOAD_BUTTON' then  get_json_object(kv,'$.gameID')
         WHEN ei = 'TAB_SELECTED' then  get_json_object(kv,'$.gameID')
         WHEN ei = 'WF_news_check' then  get_json_object(kv,'$.gameid')
         WHEN ei = 'VIDEO_ITEM_CLICK' then  get_json_object(kv,'$.gameId')
         WHEN ei = 'GENERAL_AD_CLICK' then   get_json_object(kv,'$.gameID')
         WHEN ei = 'HOME_DAU' then get_json_object(kv,'$.gameID')
         
         
         WHEN ei = 'MGC_GAME_DETAIL_ADVER_ENTRANCE_CLICK' then get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_RECOMMEND_CLICK' then get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_DOWNLOAD_CLICK' then get_json_object(kv,'$.gameid')
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_LANUCH' then get_json_object(kv,'$.gameid')         
         WHEN ei = 'MGC_TEMPLATE_TAB_EVENT' then get_json_object(kv,'$.gameId')
         WHEN ei = 'MGC_BANNER_CLICKED' THEN get_json_object(kv,'$.gameid')

         else null 
     end as game_id,
     
     CASE 
         WHEN ei in ('QUICK_ENTRY_GIFT','QUICK_ENTRY_BBS','QUICK_ENTRY_AREA') then concat(ei,'-',get_json_object(kv,'$.inInstalledGameView')) 
         WHEN ei = 'MTA_MGC_CLICK_RECOMMEND_ARTICLE' then  ei
         WHEN ei = 'MTA_MGC_DOWNLOAD_BUTTON' then  concat(ei,'-',get_json_object(kv,'$.inInstalledGameView'),'-',get_json_object(kv,'$.state'))
         WHEN ei = 'TAB_SELECTED' then  concat(ei,'-',get_json_object(kv,'$.tabId'))
         WHEN ei = 'WF_news_check' then  concat(ei,'-',get_json_object(kv,'$.tab'))         
         WHEN ei = 'VIDEO_ITEM_CLICK' then  ei
         WHEN ei = 'GENERAL_AD_CLICK' then   concat(ei,'-',get_json_object(kv,'$.position'))     
         WHEN ei = 'HOME_DAU' then   concat(ei,'-',get_json_object(kv,'$.game_installed'))
         
                  
         WHEN ei = 'MGC_GAME_DETAIL_ADVER_ENTRANCE_CLICK' then concat(ei,'-',get_json_object(kv,'$.IS_INSTALL'),'-',get_json_object(kv,'$.NAME'))
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_RECOMMEND_CLICK' then ei
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_DOWNLOAD_CLICK' then ei
         WHEN ei = 'MGC_GAME_DETAIL_UNINSTALL_LANUCH' then ei        
         WHEN ei = 'MGC_TEMPLATE_TAB_EVENT' then concat(ei,'-',get_json_object(kv,'$.tabId'))
         WHEN ei = 'MGC_BANNER_CLICKED' THEN concat(ei,'-',get_json_object(kv,'$.index'))

         ELSE 'other_ei' 
     end as ei 
    from teg_mta_intf::ieg_shouyoubao where sdate =%s   
    )t 
    where game_id > 1000 and game_id != 200302130 and game_id != 200318727 and game_id != 100001335
    GROUP by sdate,ei,id
    '''%(sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    