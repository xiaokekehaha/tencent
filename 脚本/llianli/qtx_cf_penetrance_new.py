#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_cf_penetrance_new.py
# 功能描述:     掌上穿越火线功能渗透率统计（新）
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_cf_penetrance
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-08-15
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
            CREATE TABLE IF NOT EXISTS qtx_cf_penetrance_new
            (
            sdate int,
            id bigint,
            av string,
            ei string,
            pv bigint,
            total_uin bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_cf_penetrance_new where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table qtx_cf_penetrance_new
            select 
            sdate,
            id,
            av,
            ei,
            pv,
            uin_cnt
            from
            (
            
            select
            sdate,
            id,
            '-100' as av,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uin_cnt
            from
            (
            select
            sdate,
            id,
            av,
            case
                when (id = 1100679031 and ei  in ('每天用户收的消息','每天用户发的消息') )  then '聊天'
                when (id = 1100679031 and ei  = '通讯录TAB点击' )  then '资讯列表'
                
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '资讯') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '资讯列表项') then '情报站-资讯列表项'
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '赛事') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '赛事列表项') then '情报站-赛事列表项'
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '视频') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '视频列表项') then '情报站-视频列表项'
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '活动') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '活动列表项') then '情报站-活动列表项'
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '动漫') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '动漫列表项') then '情报站-动漫列表项'
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '攻略') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '攻略列表项') then '情报站-攻略列表项'
                when (id = 1100679031 and ei = '情报站各分类点击' and get_json_object(kv,'$.type') = '美女') or (id = 1200679031 and ei = '情报站列表项' and get_json_object(kv,'$.info_list') = '美女列表项') then '情报站-美女列表项'
                
                
                when (id = 1100679031 and ei in ('资讯发表评论','资讯回复评论') ) or (id = 1200679031 and ei = '发表评论')  then '发表评论'
                when (id = 1100679031 and ei = '我的仓库点击') or (id = 1200679031 and ei = '我的仓库点击') then '我的仓库点击'
                when (id = 1100679031 and ei = '火线百科点击次数') or (id = 1200679031 and ei = '火线百科点击') then '火线百科点击'
                when (id = 1100679031 and ei = '地图点位模块点击次数') or (id = 1200679031 and ei = '地图点图') then '地图点位'
                when (id = 1100679031 and ei = '军火基地点击次') or (id = 1200679031 and ei = '军火基地点击次') then '军火基地点击次'
                when (id = 1100679031 and ei = '道聚城点击次数') or (id = 1200679031 and ei = '道具城点击') then '道具城点击'
                when (id = 1100679031 and ei = 'CF论坛停留时长 ') or (id = 1200679031 and ei = 'CF论坛点击') then 'CF论坛点击'
                when (id = 1100679031 and ei = 'CF兴趣部落停留时长') or (id = 1200679031 and ei = 'CF兴趣部落') then 'CF兴趣部落'
                when (id = 1100679031 and ei = '我的任务点击次数 ') or (id = 1200679031 and ei = '我的任务') then '我的任务'
                when (id = 1100679031 and ei = '火线助手点击次数') or (id = 1200679031 and ei = '火线助手') then '火线助手'
                when (id = 1100679031 and ei = '基地模块我的荣誉点击次数') or (id = 1200679031 and ei = '我的荣誉点击') then '我的荣誉点击'
                
                when (id = 1100679031 and ei = '占据点点击次数') then '占据点点击次数'
                

                when (id = 1100679031 and ei = '总战绩点击次数' ) or (id = 1200679031 and ei = '战绩流水点击') then '战绩流水点击'
                when (id = 1100679031 and ei = '今日战绩点击次数 ' ) or (id = 1200679031 and ei = '个人战绩记录') then '个人战绩记录'
                when (id = 1100679031 and ei = '模式统计点击' ) or (id = 1200679031 and ei = '模式统计点击') then '模式统计点击'
                
                
                when ( id = 1100679031 and ei = '竞猜大厅入口' ) or ( id = 1200679031 and ei = '竞猜大厅入口点击次数 ') then '竞猜大厅入口'
                when ( id = 1100679031 and ei = '竞猜大厅-赛程点击次数' ) or ( id = 1200679031 and ei = '竞猜大厅-赛程点击次数 ') then '竞猜大厅-赛程点击次数'
                when ( id = 1100679031 and ei = '竞猜大厅-商城点击次数' ) or ( id = 1200679031 and ei = '竞猜大厅-商城点击次数 ') then '竞猜大厅-商城点击次数'
                when ( id = 1100679031 and ei = '竞猜大厅-我点击次数' ) or ( id = 1200679031 and ei = '竞猜大厅-我点击次数 ') then '竞猜大厅-我点击次数 '
                when ( id = 1100679031 and ei = '收取积分按钮点击次数' ) or ( id = 1200679031 and ei = '收取积分按钮点击次数 ') then '收取积分按钮点击次数'
                when ( id = 1100679031 and ei = '下注按钮点击次数' ) or ( id = 1200679031 and ei = '去下注按钮点击次数 ') then '去下注按钮点击次数'
                


                when ( id = 1100679031 and ei = '专题_赛事资讯_资讯击次数' ) or ( id = 1200679031 and ei = '赛事资讯单条 ') then '赛事资讯单条'
                when ( id = 1100679031 and ei = '专题_各分类查看次数' and get_json_object(kv,'$.type') = '赛事专题' ) or ( id = 1200679031 and ei = '专题比赛视频') then '赛事专题'
                when ( id = 1100679031 and ei = '专题_各分类查看次数' and get_json_object(kv,'$.type') = '赛事资讯' ) or ( id = 1200679031 and ei = '专题赛事资讯') then '赛事资讯'
                when ( id = 1100679031 and ei = '专题_各分类查看次数' and get_json_object(kv,'$.type') = '赛事安排' ) or ( id = 1200679031 and ei = '专题赛事安排') then '赛事安排'
                when ( id = 1100679031 and ei = '专题_比赛视频_查看次数' ) or ( id = 1200679031 and ei = '视频点击播放 ' and get_json_object(kv,'$.type') like '比赛视频-%%') then '比赛视频'
                when ( id = 1100679031 and ei = '情报站各分类点击'  and get_json_object(kv,'$.type') = '视频') or ( id = 1200679031 and ei = '视频点击播放' and get_json_object(kv,'$.type') = '最新视频') then '最新视频'
                when ( id = 1100679031 and ei = '视频主页TAB按钮点击'  and get_json_object(kv,'$.type') = '直播') or ( id = 1200679031 and ei = '视频点击播放' and get_json_object(kv,'$.type') = '热门直播-全部') then '热门视频'
                when ( id = 1100679031 and ei = '视频主页TAB按钮点击'  and get_json_object(kv,'$.type') = '高手') or ( id = 1200679031 and ei = '视频点击播放' and get_json_object(kv,'$.type') = '高手视频-全部') then '高手视频'
                when ( id = 1100679031 and ei = '视频主页TAB按钮点击'  and get_json_object(kv,'$.type') = '教学') or ( id = 1200679031 and ei = '视频点击播放' and get_json_object(kv,'$.type') like '教学视频-%%') then '教学视频'
                
                else concat(cast(id as string),ei) 
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            )t where ei is not null group by sdate,id, ei

            union all 
            
            select
            sdate,
            id,
            '-100' as av,
            'mac_dau' as ei,
            COUNT(*) as pv,
            count(distinct uin_info) as uin_cnt
            from
            (
            select
            sdate,
            id,
            av, 
            concat(ui,mc) as uin_info
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            )t1   group by sdate,id   
            )t2
            """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    