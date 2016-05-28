#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_topic_static.py
# 功能描述:     手游宝话题分享页面pv数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.syb_topic_static
# 数据源表:     teg_dw_tcss.tcss_qt_qq_com
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


    ##手游宝话题统计分享页详情提取原始数据
    sql = """
            CREATE TABLE IF NOT EXISTS syb_topic_static_share_page_pv_original_data
            (
            fdate string,
            ftime string,     
            game_id bigint,
            os_type int,
            topic_id string,
            f_pvid string
            ) """
    res = tdw.execute(sql)
    
    ##手游宝话题统计分享页提取union结果数据
    sql = """
            CREATE TABLE IF NOT EXISTS syb_topic_static_share_page_pv_union_data
            (
            fdate string,
            ftime string,     
            game_id bigint,
            os_type int,
            topic_id string,
            f_pvid string
            ) """
    res = tdw.execute(sql)
    
    
    
    
    ##手游宝话题统计分享页统计结果数据
    sql = """
            CREATE TABLE IF NOT EXISTS syb_topic_static_share_page_pv
            (
            fdate string,   
            game_id bigint,
            os_type int,
            topic_id string,
            pv bigint,
            uv bigint
            ) """
    res = tdw.execute(sql)
    
    
    sql = '''
    delete from syb_topic_static_share_page_pv where fdate = '%s' 
    '''%(sDate)
    res = tdw.execute(sql)
    
    
    
    ##将qt.qq.com中对应链接/syb/topic/html/topicDetail.html的数据提取出来游戏id，操作系统类型，话题id进行分析
    sql = '''
    insert overwrite table syb_topic_static_share_page_pv_original_data
    select 
    f_date as fdate,
    f_time as ftime,
    parse_url(url_decode(concat('http://qt.qq.com',f_url,'?',f_arg),'utf-8'),'QUERY','game_id') as game_id,
    parse_url(url_decode(concat('http://qt.qq.com',f_url,'?',f_arg),'utf-8'),'QUERY','os_type') as os_type,
    parse_url(url_decode(concat('http://qt.qq.com',f_url,'?',f_arg),'utf-8'),'QUERY','topic_id') as topic_id, 
    f_pvid 
    from teg_dw_tcss::tcss_qt_qq_com where f_date = '%s' and f_dm = 'QT.QQ.COM' and f_url = '/syb/topic/html/topicDetail.html'
    '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    ##将第一步得到的原始数据进行几步union，得到union之后的数据
    sql = '''
    insert overwrite table  syb_topic_static_share_page_pv_union_data
    select 
    fdate,
    ftime,
    game_id,
    os_type,
    topic_id,
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    union all
    select 
    fdate,
    ftime,
    -100,
    os_type,
    topic_id,
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    union all
    
    select 
    fdate,
    ftime,
    game_id,
    -100,
    topic_id,
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    union all 
    
    select 
    fdate,
    ftime,
    game_id,
    os_type,
    'all',
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    union all
    select 
    fdate,
    ftime,
    -100,
    -100,
    topic_id,
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    
    union all
    select 
    fdate,
    ftime,
    -100,
    os_type,
    'all',
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    union all
    select 
    fdate,
    ftime,
    game_id,
    -100,
    'all',
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    
    
    union all
    select 
    fdate,
    ftime,
    -100,
    -100,
    'all',
    f_pvid 
    from 
    syb_topic_static_share_page_pv_original_data where fdate = '%s' 
    
    '''%(sDate,sDate,sDate,sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##计算所有汇总结果数据
    sql = '''
    insert  table syb_topic_static_share_page_pv 
    select 
    fdate,
    game_id,
    case when os_type = 1 then 301 else os_type end as os_type,
    topic_id,
    count(*) as pv,
    count(distinct f_pvid) as uv from syb_topic_static_share_page_pv_union_data where fdate = '%s' 
    group by fdate,game_id,os_type,topic_id
    '''%(sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    tdw.WriteLog("== end OK ==")
    
    
    
    