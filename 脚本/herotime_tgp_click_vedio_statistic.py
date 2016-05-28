#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     herotime_tgp_click_vedio_statistic.py
# 功能描述:     手游宝全民突击数据
# 输入参数:     yyyymmdd    例如：20160120
# 目标表名:     ieg_qt_community_app::herotime_tgp_click_vedio_statistic
# 数据源表:     teg_dw_tcss::tcss_qt_qq_com(点击流数据)
# 创建人名:     yaoyaopeng
# 创建日期:     2016-01-20
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
            CREATE TABLE IF NOT EXISTS herotime_tgp_click_vedio_statistic
            (
            fdate int,
            pv  int
            ) """
    res = tdw.execute(sql)

    sql="""delete from herotime_tgp_click_vedio_statistic where fdate=%s""" %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
#TGP核心数据计算
    tdw.WriteLog("==core data calculation==")

    sql = """
    insert table herotime_tgp_click_vedio_statistic
    SELECT 
    %s,
    count(*) as pv
    FROM 
    teg_dw_tcss::tcss_qt_qq_com 
    WHERE  f_dm='QT.QQ.COM.HOT' AND f_hottag = 'tgp_hero.play_video.relate.listvideo'  AND f_date=%s
    """%(sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

#TGP点击数据
#carousel_picture_page:轮播图点击
#main_columns:主打栏目
#ranking_list:排行榜
#more_button:更多按钮点击
#video_click:视频点击
#comprehensive_recommend：综合推荐
#new_hero_Russia:新英雄俄洛伊
#cloning_big_battle：克隆大作战
#specimens_collection:精彩集锦
#miracle_time:奇葩时刻
#best_publication：最强投稿
#shooter_update:射手更新
#more_page_click:更多页
#more_video_click：更多页视频点击
#play_page_video_click：播放页相关视频点击
#full_screen_play:自动播放全屏展示
#tag:标签
#column：栏目

#    tdw.WriteLog("==click data calculation==")
 #   sql = """
#    insert table herotime_tgp_click_vedio_statistic
#    SELECT 
#    t.f_date,
#    t.event,
#    count(t.f_pvid) as pv,
#    count(distinct(t.f_pvid)) as uv
#    FROM 
#    (SELECT 
#    f_date,
#    f_pvid,
#    CASE
#    WHEN f_hottag = 'tgp_hero.index.ad.click' THEN 'carousel_picture_page'
#    WHEN f_hottag = 'tgp_hero.index.center.listvideo' THEN 'main_columns'
#    WHEN f_hottag = 'tgp_hero.index.herorank.video' THEN 'ranking_list'
#    WHEN f_hottag = 'tgp_hero.index.videorank.more' THEN 'more_button'
#    WHEN f_hottag = 'tgp_hero.index.videorank.listvideo' THEN 'video_click'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_0' THEN 'comprehensive_recommend'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_8' THEN 'new_hero_Russia'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_6' THEN 'cloning_big_battle'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_3' THEN 'specimens_collection'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_4' THEN 'miracle_time'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_5' THEN 'best_publication'
#    WHEN f_hottag = 'tgp_hero.index.videorank.tag_7' THEN 'shooter_update'
#    WHEN f_hottag = 'tgp_hero.video_rank.nav.tag_0' THEN 'more_page_click'
#    WHEN f_hottag = 'tgp_hero.video_rank.tag_0.listvideo' THEN 'more_video_click'
#    WHEN f_hottag = 'tgp_hero.play_video.relate.listvideo' THEN 'play_page_video_click'
#    WHEN f_hottag = 'tgp_hero.index.center.fullscreen' THEN 'full_screen_play'
 #   WHEN f_hottag like 'tgp_hero.index.goto_hot_tag_%%' THEN 'tag'
##    WHEN f_hottag like 'tgp_hero.index.videorank.tag_%%' THEN 'column'
#    ELSE 'other'
#    END AS event
#    FROM teg_dw_tcss::tcss_qt_qq_com
#    WHERE f_dm = 'QT.QQ.COM.HOT' AND f_date=%s)t 
#    WHERE t.event != 'other'
#    GROUP BY t.f_date,t.event 
#    """%(sDate) 
 #   tdw.WriteLog(sql)
 #   res = tdw.execute(sql)

