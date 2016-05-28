#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_pv_uv.py
# 功能描述:     手游宝各功能pvuv数据计算
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.syb_pv_uv
# 数据源表:     teg_mta_intf.ieg_shouyoubao
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


    ##分别按照产品的需求进行统计
    ##统计停留时长
    sql = """
            CREATE TABLE IF NOT EXISTS syb_stay_time
            (
            fdate int,
            appid bigint,
            game_id bigint,
            account_type bigint,
            total_syb_account bigint,
            total_time bigint,
            uv_device bigint,
            pv bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from syb_stay_time where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    #全民突击游戏专区
    game_id = '101'
    
    #MT2游戏专区
    mt2_game_id = '100'
    
    ##炫斗游戏专区
    ttxd_game_id = '102'
    
    #统计安卓停留时长
    android_appid = '1100679302'
    
    sql = """
            insert  table syb_stay_time
            select 
            '%s',
            '%s',
            game_id,
            account_type,
            count(distinct uin) as total_syb_account,
            sum(staytime)  as total_time,
            count(distinct uin_info) as device,
            count(*) as pv
            from  
            (
            select 
            case 
                 when ei = 'WF_ZONE_TIME' then %s 
                 when ei = 'MT2_ZONE_TIME' then %s 
                 else %s 
            end as game_id,
            uin_info,
            case when uin < 3000000000 then 1 else 2 end as account_type,
            uin,
            staytime
            from 
            (
            select 
            ei,
            concat(ui,mc) as uin_info, 
            get_json_object(kv, '$.uin') as uin,
            du as staytime 
            from  teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s and  id = %s and ei in ( 'WF_ZONE_TIME','MT2_ZONE_TIME','TTXD_ZONE_TIME')
            )t
            )t1 group by game_id,account_type
    
                    """ % (sDate,android_appid,game_id,mt2_game_id,ttxd_game_id,sDate,sDate,android_appid)
                    
                    
                    
    #统计IOS停留时长
    ios_appid = '1200679337'
    
    sql = """
    insert  table syb_stay_time
            select 
            '%s',
            id,
            game_id,
            account_type,
            count(distinct uin) as total_syb_account,
            sum(staytime)  as total_time,
            count(distinct uin_info) as uv_device,
            count(*) as pv 
            from
            (
            select 
            uin_info,
            id,
            case when ei = 'WF_DURATION' or ei = 'WF_PAGE_TIME' then %s else %s end as game_id,
            case when uin < 3000000000 then 1 else 2 end as account_type,
            uin,
            staytime
            from  
            (
            select
            ei, 
            id,
            concat(ui,mc) as uin_info, 
            case 
                when id = %s then get_json_object(kv, '$.syb_id')
            else  
                get_json_object(kv, '$.uin')
            end as uin,
            cast(du as bigint) as staytime 
            from  teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s  and ei in ( 'WF_DURATION','MT2_DURATION','WF_PAGE_TIME','MT2_PAGE_TIME')
            )t
            )t1 
            group by game_id,id,account_type
    
                    """ % (sDate,game_id,mt2_game_id,ios_appid,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    #统计几个广告位点击的情况
    sql = """
     CREATE TABLE IF NOT EXISTS syb_adv_click
            (
            fdate int,
            appid bigint,
            game_id bigint,
            account_type bigint,
            adv_pos bigint,
            total_syb_account bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_adv_click where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    
    sql = """
    insert  table  syb_adv_click
    select 
    '%s',
    id,
    game_id,
    account_type,
    adv_pos,
    count(distinct uin) as total_syb_account,
    count(distinct uin_info) as uv_device,
    count(*) as pv
    from 
    
    (
    select 
    id,
    uin_info,
    case 
        when ei in ( 'WF_HOME_AD_CLICK' , 'AD_CLICK' , 'WF_advs_check', 'WF_Guide_Gift','WF_GIFT_CLICK') then %s 
        when ei = 'MT2_advs_check' then %s 
        else %s 
    end as game_id,
    case when uin < 3000000000 then 1 else 2 end as account_type,
    uin,
    adv_pos 
    from 
    ( 
    select 
    concat(ui,mc) as uin_info, 
    id,
    ei, 
    case 
        when id = '%s' and ei in ('WF_HOME_AD_CLICK' , 'AD_CLICK' ,'TTXD_AdsRead','WF_Guide_Gift') then get_json_object(kv, '$.uin')
        when id = '%s' and ei in ('WF_advs_check','MT2_advs_check','WF_GIFT_CLICK') then get_json_object(kv, '$.syb_id')
        else null 
    end as uin,
    case 
        when  id = '%s' and ei in ('WF_HOME_AD_CLICK' , 'AD_CLICK' ) then get_json_object(kv, '$.position')  
        when  id = '%s' and ei in ('WF_advs_check','MT2_advs_check') then get_json_object(kv, '$.index')
        when id = '%s' and ei = 'TTXD_AdsRead' then get_json_object(kv, '$.location')  
        when id = '%s' and ei = 'WF_Guide_Gift' then '0'
        when id = '%s' and ei = 'WF_GIFT_CLICK' then '1'   
        else null 
    end as adv_pos
    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s  and ei in ( 'WF_HOME_AD_CLICK' , 'AD_CLICK' , 'WF_advs_check','MT2_advs_check','TTXD_AdsRead','WF_Guide_Gift','WF_GIFT_CLICK')
    )t
    )t1 
    group by game_id,id,account_type, adv_pos
    """%(sDate,game_id,mt2_game_id,ttxd_game_id,android_appid,ios_appid,android_appid,ios_appid,android_appid,android_appid,ios_appid,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    
    ##轮播图点击
    
    ##安卓轮播图点击
    sql = """
     CREATE TABLE IF NOT EXISTS syb_slides_data
            (
            fdate int,
            appid bigint,
            game_id bigint,
            account_type bigint,
            slider_idx bigint,
            slider_tab string,
            total_syb_account bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_slides_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    
    sql = """
    insert  table  syb_slides_data
    select 
    '%s',
    id,
    '%s',
    account_type,
    slider_idx,
    slider_tab,
    count(distinct uin) as total_syb_account,
    count(distinct uin_info) as uv_device,
    count(*) as pv
    from 
    (
    select 
    id,
    uin_info,
    case when uin < 3000000000 then 1 else 2 end as account_type,
    uin,
    slider_idx,
    slider_tab 
    from 
    ( 
    select 
    id,
    concat(ui,mc) as uin_info, 
    case 
        when id = '%s' then  get_json_object(kv, '$.uin')
        when id = '%s' then  get_json_object(kv, '$.syb_id')
        else null 
    end as uin,
    get_json_object(kv, '$.index') as slider_idx,
    case 
        when id = '%s' then  get_json_object(kv, '$.tab') 
        when id = '%s' and ei = 'WF_slides_check' then '首页'
        when id = '%s' and ei = 'WF_strategy_slides_check' then '攻略'
    else null
    end as slider_tab 
    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s  and ei in ('WF_slides_check','WF_strategy_slides_check')
    )t
    )t1 
    group by id,account_type, slider_idx,slider_tab
    """%(sDate,game_id,android_appid,ios_appid,android_appid,ios_appid,ios_appid,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    


    
    ##安卓相关ei事件点击 pv uv
    sql = """
     CREATE TABLE IF NOT EXISTS syb_click_data
            (
            fdate int,
            appid bigint,
            ei string,
            account_type bigint,
            total_syb_account bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_click_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    sql = """
    insert table syb_click_data
    select 
    '%s',
    id,
    ei,
    account_type,
    count(distinct uin) as total_syb_account,
    count(distinct uin_info) as uv_device,
    count(*) as pv
    from 
    (
    select 
    id,
    ei ,
    uin_info,
    case 
        when ei = 'TTXD_HOME_DAU' and   get_json_object(kv, '$.account_type') is not null then cast (get_json_object(kv, '$.account_type') as int)
        else 
        case 
            when  uin < 3000000000 then 1 
            when   uin >= 3000000000 then 2
        end 
        
    end as account_type,
    uin
    from 
    ( 
    select 
    id,
    kv,
    concat(ui,mc) as uin_info ,
    case 
        when id = %s then get_json_object(kv, '$.uin')
        when id = %s then get_json_object(kv, '$.syb_id')
        else null
    end as  uin,
    case 
       when ei in ('VIDEO_AD_CLICK','VIDEO_LABLE_CLICK','VIDEO_ITEM_CLICK') 
       then concat(ei,'-',get_json_object(kv, '$.game_name')) 
       when ei in ('WF_news_check') 
       then concat(ei,'-',get_json_object(kv, '$.gameid'))
       when ei in ('GENERAL_SIGN_CLICK')  
       then concat(ei,'-',get_json_object(kv, '$.gameID'))
       when ei in ('TOPIC_ACT_ENTRY')  
       then concat(ei,'-',get_json_object(kv, '$.GameID'))
       else ei 
       
    end as ei 
    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s 
    )t  
    )t1 
    group by id,ei,account_type
    """%(sDate,android_appid,ios_appid,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##复合ei事件上报
    sql = """
     CREATE TABLE IF NOT EXISTS syb_combine_click_data
            (
            fdate int,
            appid bigint,
            ei string,
            account_type bigint,
            total_syb_account bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_combine_click_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    sql = """
    insert  table  syb_combine_click_data
    select 
    '%s',
    id,
    ei,
    account_type,
    count(distinct uin) as total_syb_account,
    count(distinct uin_info) as uv_device,
    count(*) as pv
    from 
    (
    select 
    id,
    ei ,
    uin_info,
    case when uin < 3000000000 then 1 else 2 end as account_type,
    uin
    from 
    ( 
    select 
    id,
    concat(ui,mc) as uin_info ,
    case 
        when id = %s then get_json_object(kv, '$.uin')
        when id = %s then get_json_object(kv, '$.syb_id')
        else null
    end as  uin,
    case 
       when ei in ('WF_INFONEWS_LIST_ITEM_CLICK',
                 'WF_ANNOUNCE_LIST_ITEM_CLICK',
                 'WF_STRATEGY_LIST_ITEM_CLICK',
                 'WF_ENTERTAIN_LIST_ITEM_CLICK') 
       then 'wf_news_click' 
       when ei in ('MT2_INFONEWS_LIST_ITEM_CLICK',
                 'MT2_ANNOUNCE_LIST_ITEM_CLICK',
                 'MT2_STRATEGY_LIST_ITEM_CLICK',
                 'MT2_ENTERTAIN_LIST_ITEM_CLICK') 
       then 'mt2_news_click'
       
       when id = %s and 
       ( 
          ei in ('WF_TAB_SELECTED','WF_HOME_AD_CLICK','AD_CLICK','WF_slides_check') or
          ( ei = 'WF_news_check' and get_json_object(kv, '$.gameid') = '101' ) or
          ( ei = 'Strategy_tab_Cleck' and get_json_object(kv, '$.gameid') = '101' ) or  
          ( ei in ('VIDEO_AD_CLICK','VIDEO_LABLE_CLICK','VIDEO_ITEM_CLICK') and get_json_object(kv, '$.game_name') = 'WEFIRE')
          
       ) 
        then 'wf_client_dau_android' 
        
        
       when id = %s and ( ei in ('WF_INFONEWS_LIST_TAB_CLICK',
        'WF_STRATEGY_LIST_TAB_CLICK',
        'WF_ANNOUNCE_LIST_TAB_CLICK',
        'WF_ENTERTAIN_LIST_TAB_CLICK',
        'WF_VIDEO_LIST_TAB_CLICK',
        'WF_advs_check',
        'WF_INFONEWS_LIST_ITEM_CLICK',
        'WF_ANNOUNCE_LIST_ITEM_CLICK',
        'WF_STRATEGY_LIST_ITEM_CLICK',
        'WF_ENTERTAIN_LIST_ITEM_CLICK',
        'WF_slides_check',
        'WF_strategy_slides_check',
        'WF_STRATEGY_LIST_ITEM_LABEL_CLICK',
        'WF_VIDEO_LIST_AD_CLICK',
        'WF_VIDEO_LIST_TAG_CLICK',
        'WF_VIDEO_LIST_ITEM_CLICK'
        )
        )
        then 'wf_client_dau_ios'
        
       when id = %s and ( ei in ('MT2_TAB_SELECTED',
                 'MT2_AdsRead',
                 'MT2_CostForceRead',
                 'MT2_JcScoreRead') or 
                 (ei = 'WF_news_check' and get_json_object(kv, '$.gameid') = 100) or 
                 (ei in ('VIDEO_AD_CLICK','VIDEO_LABLE_CLICK','VIDEO_ITEM_CLICK') and get_json_object(kv, '$.game_name') = 'MT2')
                 )
        then 'mt2_client_dau_android'
        
        when id = %s and (ei in (
                  'MT2_INFONEWS_LIST_TAB_CLICK', 
                  'MT2_STRATEGY_LIST_TAB_CLICK', 
                  'MT2_ANNOUNCE_LIST_TAB_CLICK',
                  'MT2_ENTERTAIN_LIST_TAB_CLICK', 
                  'MT2_VIDEO_LIST_TAB_CLICK', 
                  'MT2_advs_check',
                  'MT2_INFONEWS_LIST_ITEM_CLICK',
                  'MT2_ANNOUNCE_LIST_ITEM_CLICK',
                  'MT2_STRATEGY_LIST_ITEM_CLICK',
                  'MT2_ENTERTAIN_LIST_ITEM_CLICK',
                  'MT2_VIDEO_LIST_AD_CLICK',
                  'MT2_VIDEO_LIST_TAG_CLICK',
                  'MT2_VIDEO_LIST_ITEM_CLICK' )
                  )
        then 'mt2_client_dau_ios'
        
        
        when id = %s and  (  (ei in ('GENERAL_SIGN_CLICK') and get_json_object(kv,'$.gameID') = '101') or ei in ('SIGN_CLICK','WF_HOME_SIGN_CLICK')  ) 
        then 'wf_client_sigin'
        
        when id = %s and ei in ('WF_MAINVIEW_CLICK_SIGNIN' )   
        then 'wf_client_sigin'
        
        
        
    else 'unknown'       
    end as ei 
    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s 
    )t  
    )t1 
    group by id,ei,account_type
    """%(sDate,android_appid,ios_appid,android_appid,ios_appid,android_appid,ios_appid,android_appid,ios_appid,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    



    ##安卓TAB点击事件PVUV
    sql = """
     CREATE TABLE IF NOT EXISTS syb_tab_click_data
            (
            fdate int,
            appid bigint,
            game_id bigint,
            account_type bigint,
            tab_idx bigint,            
            total_syb_account bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_tab_click_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    sql = """
    insert  table  syb_tab_click_data
    select 
    '%s',
    '%s',
    game_id,
    account_type,
    tab_idx,
    count(distinct uin) as total_syb_account,
    count(distinct uin_info) as uv_device,
    count(*) as pv
    from 
    
    (
    select 
    uin_info,
    case 
    when ei = 'WF_TAB_SELECTED' then %s
    when ei = 'MT2_TAB_SELECTED' then %s  
    else %s end as game_id,
    case when uin < 3000000000 then 1 else 2 end as account_type,
    uin,
    tab_idx 
    from 
    ( 
    select 
    ei,
    concat(ui,mc) as uin_info, 
    get_json_object(kv, '$.uin') as uin,
    get_json_object(kv, '$.selectIndex') as tab_idx 
    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s and  id = %s and ei in ( 'WF_TAB_SELECTED','MT2_TAB_SELECTED','TTXD_TAB_SELECTED')
    )t
    )t1 
    group by game_id,account_type, tab_idx
    """%(sDate,android_appid,game_id,mt2_game_id,ttxd_game_id,sDate,sDate,android_appid)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    
    ##各类页面停留时长数据
    sql = """
     CREATE TABLE IF NOT EXISTS syb_page_class_time
            (
            fdate int,
            appid bigint,
            game_id bigint,
            page_name string,
            account_type bigint,           
            total_syb_account bigint,
            total_time bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_page_class_time where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    sql = """
    insert  table  syb_page_class_time
    select 
    '%s',
    '%s',
    game_id,
    page_name,
    account_type,
    count(distinct uin) as total_syb_account,
    sum(total_time) as total_time,
    count(distinct uin_info) as uv_device,
    count(*) as pv
    from 
    (
    select 
    uin_info,
    case 
    when ei = 'WF_PAGE_TIME' then %s
    when ei = 'MT2_PAGE_TIME' then %s  
    else %s end as game_id,
    case when uin < 3000000000 then 1 else 2 end as account_type,
    uin ,
    page_name,
    total_time 
    from 
    ( 
    select 
    ei,
    concat(ui,mc) as uin_info, 
    get_json_object(kv, '$.uin') as uin,
    get_json_object(kv, '$.pageClassName') as page_name  ,
    cast(du as bigint) as total_time  
    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s and id = %s and ei in ( 'TTXD_PAGE_TIME','MT2_PAGE_TIME','WF_PAGE_TIME')
    )t
    )t1 
    group by game_id,account_type, page_name
    """%(sDate,android_appid,game_id,mt2_game_id,ttxd_game_id,sDate,sDate,android_appid)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    #统计新的广告位点击情况(模板化统计)
    sql = """
     CREATE TABLE IF NOT EXISTS syb_adv_click_new
            (
            fdate int,
            av string,
            appid bigint,
            game_id bigint,
            ei string,
            adv_pos bigint,
            total_syb_account bigint,
            uv_device bigint,
            pv bigint
            )
    """
    res = tdw.execute(sql)
    
    
    sql="""delete from syb_adv_click_new where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    
    sql = """
    insert  table  syb_adv_click_new
    select 
    %s,
    av,
    id,
    game_id,
    'adv_click' as ei,
    adv_pos,
    COUNT(DISTINCT uin) as total_uin,
    COUNT(DISTINCT uin_info) as total_device,
    COUNT(*) as pv 
    from 
    ( 
    select
    av, 
    concat(ui,mc) as uin_info, 
    id, 
    CASE 
        WHEN id = '1100679302'  then get_json_object(kv, '$.uin') 
        WHEN id = '1200679337'  THEN get_json_object(kv,'$.syb_id')
        ELSE NULL 
    END AS uin,
    
    
    CASE 
        WHEN id = '1100679302' AND ei = 'GENERAL_AD_CLICK'    THEN get_json_object(kv, '$.game_id') 
        WHEN id = '1200679337' AND ei = 'MGC_PROMOTE_CLICKED' THEN get_json_object(kv,'$.gameid')
        ELSE NULL 
    END AS game_id,
    
    
    CASE 
        WHEN id = '1100679302' AND ei = 'GENERAL_AD_CLICK'    THEN get_json_object(kv, '$.position') 
        WHEN id = '1200679337' AND ei = 'MGC_PROMOTE_CLICKED' THEN get_json_object(kv,'$.index')
        ELSE NULL 
    END AS adv_pos 

    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s  and ei in ( 'GENERAL_AD_CLICK' , 'MGC_PROMOTE_CLICKED' )
    )t 
    GROUP BY av,id,game_id,adv_pos
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
    insert  table  syb_adv_click_new
    select 
    %s,
    'all',
    id,
    game_id,
    'adv_click' as ei,
    adv_pos,
    COUNT(DISTINCT uin) as total_uin,
    COUNT(DISTINCT uin_info) as total_device,
    COUNT(*) as pv 
    from 
    ( 
    select
    concat(ui,mc) as uin_info, 
    id, 
    CASE 
        WHEN id = '1100679302'  then get_json_object(kv, '$.uin') 
        WHEN id = '1200679337'  THEN get_json_object(kv,'$.syb_id')
        ELSE NULL 
    END AS uin,
    
    
    CASE 
        WHEN id = '1100679302' AND ei = 'GENERAL_AD_CLICK'    THEN get_json_object(kv, '$.game_id') 
        WHEN id = '1200679337' AND ei = 'MGC_PROMOTE_CLICKED' THEN get_json_object(kv,'$.gameid')
        ELSE NULL 
    END AS game_id,
    
    
    CASE 
        WHEN id = '1100679302' AND ei = 'GENERAL_AD_CLICK'    THEN get_json_object(kv, '$.position') 
        WHEN id = '1200679337' AND ei = 'MGC_PROMOTE_CLICKED' THEN get_json_object(kv,'$.index')
        ELSE NULL 
    END AS adv_pos 

    from teg_mta_intf::ieg_shouyoubao where sdate = %s and cdate = %s  and ei in ( 'GENERAL_AD_CLICK' , 'MGC_PROMOTE_CLICKED' )
    )t 
    GROUP BY id,game_id,adv_pos
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    