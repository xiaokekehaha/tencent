#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_cf_penetrance_v3.py
# 功能描述:     掌上穿越火线功能渗透率统计-新版UI
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_cf_penetrance_v3
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-11-20
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
            CREATE TABLE IF NOT EXISTS qtx_cf_penetrance_v3
            (
            sdate int,
            id bigint,
            av string,
            ei string,
            pv bigint,
            total_uin bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_cf_penetrance_v3 where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table qtx_cf_penetrance_v3
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
                when (id = 1100679031 and ei  in ('情报站列表项点击','资讯广告点击','视频播放次数') ) or (id = 1200679031 and ei = '情报站列表项') then '情报站'
                when (id = 1100679031 and ei  in  ('查看评论点击次数' ,'写评论点击次数')) or (id = 1200679031 and ei in ( '进入评论页面','发表评论')) then '评论'
                
                when (id = 1100679031 and ei = '我模块点击次数' ) or (id = 1200679031 and ei = '情报站社区基地我TAB点击次数' and get_json_object(kv,'$.type') = '我') then '我-战绩'
                when (id = 1100679031 and ei = '我_战绩资产记录展示次数' and get_json_object(kv,'$.tab') = '装备') or (id = 1200679031 and ei = '战绩资产记录TAB点击次数' and get_json_object(kv,'$.type') = '资产') then '我-资产'
                when (id = 1100679031 and ei = '我_战绩资产记录展示次数' and get_json_object(kv,'$.tab') = '记录') or (id = 1200679031 and ei = '战绩资产记录TAB点击次数' and get_json_object(kv,'$.type') = '记录') then '我-记录'
                
                when (id = 1100679031 and ei = '客态资料' ) then '客态资料'
                
                when (id = 1100679031 and ei = '道聚城点击次数') or (id = 1200679031 and ei = '道具城点击') then '道聚城'
                when (id = 1100679031 and ei = '火线_视频点击次数') or (id = 1200679031 and ei = '火线时刻视频点击次数') then '火线时刻'
                
                when (id = 1100679031 and ei = '我的仓库点击' ) or (id = 1200679031 and ei = '我的仓库点击') then '我的仓库'
                
                when (id = 1100679031 and ei = '军火基地点击次' ) or (id = 1200679031 and ei = '军火基地点击次') then '军火基地'
                
                when (id = 1100679031 and ei= '基地WEB页面点击次数' and get_json_object(kv,'$.title') = '周边商城') then '周边商城'
                
                when (id = 1100679031 and ei = '竞猜大厅入口' ) or (id = 1200679031 and ei = '竞猜大厅入口点击次数') then '赛事竞猜'
                
                
                when (id = 1100679031 and ei = '火线百科点击次数' ) or (id = 1200679031 and ei = '火线百科点击') then '火线百科' 
                when (id = 1100679031 and ei = '火线助手点击次数' ) or (id = 1200679031 and ei = '火线助手') then '火线助手'
                
                when (id = 1100679031 and ei = '我的任务点击次数' ) or (id = 1200679031 and ei = '我的任务点击') then '我的任务'
                when (id = 1100679031 and ei = '地图点位模块点击次数' ) or (id = 1200679031 and ei = '地图点图') then '地图点位'
                when (id = 1100679031 and ei in ('每天用户发的消息' ,'每天用户发的消息')) then '聊天'
                when (id = 1100679031 and ei = '社区_CF论坛点击次数' ) or (id = 1200679031 and ei = 'CF论坛点击') then 'CF论坛'
                when (id = 1100679031 and ei = '社区_CF手游论坛点击次数' ) or (id = 1200679031 and ei = '点击CF手游论坛') then 'CF手游论坛'
                when (id = 1100679031 and ei = '社区_兴趣部落点击次数' ) or (id = 1200679031 and ei = 'CF兴趣部落') then '兴趣部落'

                
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
    
    
    
    
    