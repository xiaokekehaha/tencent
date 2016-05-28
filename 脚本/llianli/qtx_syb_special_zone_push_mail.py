#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_special_push_mail.py
# 功能描述:     手游宝专区点击数据
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_special_push_mail
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
            CREATE TABLE IF NOT EXISTS tb_syb_special_push_mail
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

    sql="""delete from tb_syb_special_push_mail where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = '''
    insert table tb_syb_special_push_mail
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
        WHEN id = 1200679337  THEN ext3
        WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
        else '0'
     END AS uin,

     CASE
         WHEN ei in ( 'MT2_HOME_DAU','MT2_VIDEO_ITEM_CLICK','MT2_AdsRead','MT2_QUICK_ENTRY_GIFT','MT2_GIFT_CLICK') THEN '100'
         WHEN ei in ('WF_HOME_DAU','WF_VIDEO_ITEM_CLICK','WF_HOME_AD_CLICK','WF_Guide_Gift','WF_MAINVIEW_CLICK_SIGNIN','SIGN_CLICK','WF_HOME_SIGN_CLICK','WF_advs_check','WF_GIFT_CLICK',
                     'WF_INFONEWS_LIST_ITEM_CLICK','WF_ANNOUNCE_LIST_ITEM_CLICK','WF_STRATEGY_LIST_ITEM_CLICK','WF_STRATEGY_LIST_ITEM_CLICK','WF_VIDEO_LIST_ITEM_CLICK') THEN '101'
         WHEN ei in ('TTXD_HOME_PK','TTXD_AdsRead','TTXD_HOME_DAU','TTXD_INFONEWS_LIST_ITEM_CLICK','TTXD_ANNOUNCE_LIST_ITEM_CLICK','TTXD_STRATEGY_LIST_ITEM_CLICK','TTXD_VIDEO_LIST_ITEM_CLICK','MGC_TTXD_SIGN','TTXD_VIDEO_ITEM_CLICK') THEN '102'
         WHEN ei in ('TTKP_HOME_DAU') THEN '103'
         WHEN ei in ('WF_news_check') THEN get_json_object(kv,'$.gameid')
         WHEN ei in ('GENERAL_AD_CLICK') THEN get_json_object(kv,'$.game_id')
         WHEN ei in ('TOPIC_ACT_ENTRY') and id = 1100679302 THEN get_json_object(kv,'$.gameID')
         WHEN ei in ('GENERAL_SIGN_CLICK') THEN get_json_object(kv,'$.game_id')
         WHEN ei in ('VIDEO_ITEM_CLICK') THEN get_json_object(kv,'$.gameID')
         WHEN ei in ('HOME_DAU')  THEN get_json_object(kv,'$.gameId')


         WHEN ei in ('MGC_MTA_DAU_REPORT_EVENT') THEN get_json_object(kv,'$.gameId')
         WHEN ei in ('MGC_TEMPLATE_TAB_ARTICLE_EVENT') THEN get_json_object(kv,'$.gameId')
         WHEN ei in ('MGC_PROMOTE_CLICKED','MGC_PROMOTE_GIFT_CLICKED') THEN get_json_object(kv,'$.gameid')
         WHEN ei in ('TOPIC_ACT_ENTRY') and id = 1200679337 THEN get_json_object(kv,'$.gameid')
         WHEN ei in ('MGC_T_SIGN') THEN get_json_object(kv,'$.gameid')
         WHEN ei in ('MGC_VIDEO_ITEM_CLICK') THEN get_json_object(kv,'$.gameid')
         ELSE NULL
     END AS game_id,

     CASE
         WHEN ei in ('MT2_HOME_DAU','MGC_MTA_DAU_REPORT_EVENT','WF_HOME_DAU','TTKP_HOME_DAU','TTXD_HOME_DAU','HOME_DAU') then 'dau'
         WHEN ei in ('WF_news_check','MT2_news_check') or (ei = 'MGC_TEMPLATE_TAB_ARTICLE_EVENT' ) or
              ei in ('TTXD_INFONEWS_LIST_ITEM_CLICK',
                     'TTXD_ANNOUNCE_LIST_ITEM_CLICK',
                     'TTXD_STRATEGY_LIST_ITEM_CLICK',
                     'WF_INFONEWS_LIST_ITEM_CLICK',
                     'WF_ANNOUNCE_LIST_ITEM_CLICK',
                     'WF_STRATEGY_LIST_ITEM_CLICK')
         then 'news_click'
         WHEN ( 
               (ei = 'GENERAL_AD_CLICK'  and get_json_object(kv,'$.game_id') in ('102','103','100001335') and get_json_object(kv,'$.position') = '1') or
               (ei = 'GENERAL_AD_CLICK'  and get_json_object(kv,'$.game_id') not in ('102','103','100001335') and get_json_object(kv,'$.position') = '0') or
               ei = 'MT2_AdsRead'  or
               (ei = 'WF_HOME_AD_CLICK' and get_json_object(kv,'$.position') = '1') or
               ei = 'WF_Guide_Gift' or ei = 'WF_GIFT_CLICK' or
               ei in ( 'MT2_QUICK_ENTRY_GIFT' ,'MT2_GIFT_CLICK') OR
               (ei = 'WF_advs_check' and get_json_object(kv,'$.index') = '1') or
               (ei = 'TTXD_AdsRead' and get_json_object(kv,'$.location') = '1' and id = 1100679302 ) or
               (ei = 'TTXD_AdsRead' and get_json_object(kv,'$.index') = '1' and id = 1200679337) or
               (ei = 'MGC_PROMOTE_GIFT_CLICKED') or

               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001335' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '2') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001335' AND  get_json_object(kv,'$.tagId') = '15' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001335' AND  get_json_object(kv,'$.tagId') = '15' AND get_json_object(kv,'$.index') = '3') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001335' AND  get_json_object(kv,'$.tagId') = '18' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001498' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001557' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100001751' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100002156' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100006188' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100006282' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100006352' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100006564' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100006820' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '100006830' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '101' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '102' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '102' AND  get_json_object(kv,'$.tagId') = '13' AND get_json_object(kv,'$.index') = '2') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '103' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '103' AND  get_json_object(kv,'$.tagId') = '13' AND get_json_object(kv,'$.index') = '2') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '106' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200108954' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200302130' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200318727' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200321396' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200330345' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200332247' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200332284' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1') OR
               (ei = 'MGC_PROMOTE_CLICKED'  and get_json_object(kv,'$.gameid') = '200332444' AND  get_json_object(kv,'$.tagId') = '11' AND get_json_object(kv,'$.index') = '1')

               ) then 'gift_package_click'
        WHEN ei = 'TOPIC_ACT_ENTRY' THEN 'topic_click'
        WHEN ei in ('MT2_VIDEO_ITEM_CLICK','WF_VIDEO_ITEM_CLICK','VIDEO_ITEM_CLICK','TTXD_VIDEO_ITEM_CLICK')  or
             (ei in ('MGC_VIDEO_ITEM_CLICK'))  OR
             ei = 'TTXD_VIDEO_LIST_ITEM_CLICK' or
             ei = 'WF_VIDEO_LIST_ITEM_CLICK'
             then 'video_item_click'

        WHEN ei in ('GENERAL_SIGN_CLICK','MGC_T_SIGN','WF_MAINVIEW_CLICK_SIGNIN','SIGN_CLICK','WF_HOME_SIGN_CLICK','MGC_TTXD_SIGN') then 'sign_click'
        WHEN ei in ('TTXD_HOME_PK') then 'data_module_click'
        ELSE 'other_ei'
    END AS ei,
    du
    from teg_mta_intf::ieg_shouyoubao where sdate =%s and cdate = %s
    )t
    where game_id != 0 and ei != 'other_ei'
    GROUP by sdate,game_id,ei,id
    '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    tdw.WriteLog("== end OK ==")



