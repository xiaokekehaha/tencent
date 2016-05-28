#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_sociaty_client_click_data.py
# 功能描述:     手游宝APP 天天炫斗公会点击数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-11-03
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


     ##创建表，存储炫斗公会数据的结果
    sql = '''
        CREATE TABLE IF NOT EXISTS tb_syb_app_ttxd_sociaty_click_data
        (
        dtstatdate INT COMMENT '统计日期',
        id BIGINT COMMENT 'APPID -100全体，11开头安卓 12开头IOS',
        ei STRING COMMENT '点击事件',
        sybid_uv BIGINT COMMENT '安卓手游宝ID计算的UV',
        mac_uv BIGINT COMMENT '按照设备号计算的UV',
        pv BIGINT COMMENT '点击PV'
        ) '''
            
    res = tdw.execute(sql)


    sql=''' delete from tb_syb_app_ttxd_sociaty_click_data where dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##统计结果
    sql = ''' 
    INSERT TABLE tb_syb_app_ttxd_sociaty_click_data
        SELECT 
        %s AS dtstatdate,
        CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
        ei,
        COUNT(DISTINCT sybid) AS sybid_uv,
        COUNT(DISTINCT uin_info) AS mac_uv,
        COUNT(*) AS pv 
        FROM 
        (
        SELECT
        id,
        CASE WHEN id = 1100679302 THEN get_json_object(kv,'$.uin') ELSE get_json_object(kv,'$.syb_id') END AS sybid,
        concat(ui,mc) AS uin_info,
        CASE 
            WHEN    ( ei = 'GENERAL_AD_CLICK' AND get_json_object(kv,'$.url') LIKE '%%native://sociatyhome?%%' AND get_json_object(kv,'$.game_id') = '102')  
                 OR ( ei = 'MGC_SOCIATY_ENTRANCE_CLICK' AND get_json_object(kv,'$.gameid') = '102' )
            THEN '公会大入口'
            
            WHEN ei = 'TAB_NEWS_SHOWS' 
                OR ( ei = 'MGC_SOCIATY_MAIN_VIEW_TAB_NEW_TREND_EVENT' AND get_json_object(kv,'$.articleType') = '306')
            THEN '新鲜事TAB（首页）'
            
            WHEN ei = 'TAB_PUBPLAZE_SHOWS' 
                 OR (ei = 'MGC_SOCIATY_MAIN_VIEW_TAB_SQUARE_EVENT') 
            THEN '广场TAB'
            
            WHEN ei = 'TAB_MYSOCIATY_SHOWS' 
                 OR ei = 'MGC_SOCIATY_MAIN_VIEW_TAB_MY_GUILD_EVENT'
            THEN '我的公会TAB'
            
            WHEN ei = 'GET_GIFT_CLICK' 
                 OR ( ei = 'MGC_SOCIATY_GET_PRIZE_ENTRANCE_CLICK'  AND get_json_object(kv,'$.gameid') = '102') 
            THEN '领福利入口'
            
            
            WHEN ei = 'FEEDS_CLICK_GUILDNAME' 
                 OR ( ei = 'MGC_SOCIATY_FEEDS_COME_FROM_CLICK'  AND get_json_object(kv,'$.gameid') = '102') 
            THEN '在Feeds流查看来自公会点击量'
            
            
            WHEN ei = 'GUILD_FIRST_VIEWFRESHNEWS' 
                 OR ( ei = 'MGC_SOCIATY_FEEDS_SCROLL_TO_NEWEST_ITEM'  AND get_json_object(kv,'$.gameid') = '102') 
            THEN '在广场浏览下滑到最近新鲜事'
            
            
            WHEN ei = 'SOCIATY_MENBER_CLICK' 
                 OR ( ei = 'MGC_SOCIATY_MEMBER_CLICK'  AND get_json_object(kv,'$.gameid') = '102') 
            THEN '公会信息-公会成员'
            
            
            WHEN ei = 'SOCIATY_WEEK_ACTIVITY_CLICK' 
                 OR ( ei = 'MGC_SOCIATY_ACTIVE_CLICK'  AND get_json_object(kv,'$.gameid') = '102') 
            THEN '公会信息-本周活跃'
            
            
            WHEN ei = 'SOCIATY_MEDAL_ICON_CLICK' 
                 OR ( ei = 'MGC_SOCIATY_MEDAL_CLICK'  AND get_json_object(kv,'$.gameid') = '102') 
            THEN '公会勋章点击量'
            
            
            ELSE 'other'
        END AS ei
            
            
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND id in (1100679302,1200679337)
        )t
        WHERE ei != 'other'
        GROUP BY ei,cube(id)
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    tdw.WriteLog("== end OK ==")
    
    
    
    