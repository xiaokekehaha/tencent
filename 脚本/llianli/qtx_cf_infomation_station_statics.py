#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_cf_infomation_station_statics.py
# 功能描述:     掌上穿越火线情报站数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_cf_infomation_station_statics
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-11-23
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
            CREATE TABLE IF NOT EXISTS qtx_cf_infomation_station_statics
            (
            sdate int,
            id bigint,
            ei string,
            pv bigint,
            uv bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_cf_infomation_station_statics where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table qtx_cf_infomation_station_statics
            
            select 
            sdate,
            id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv 
            from 
            (
            select
            sdate, 
            id,
            case 
                when (id = 1100679031 and ei in ('情报站列表项点击','资讯广告点击','视频播放次数') )   or 
                     (id = 1200679031 and ei in ('情报站列表项'))
                then '情报站整体'
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei

            union all 
            
            select 
            sdate,
            id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv 
            from 
            (
            select
            sdate, 
            id,
            case 
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') not in ('图片','手机','论坛','电脑','游戏'))   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '资讯列表项')
                then '资讯'
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei
            
            union all 
            
            select 
            sdate,
            id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv 
            from 
            (
            select
            sdate, 
            id,
            case 
                when (id = 1100679031 and ( ei in ('视频播放次数')  or (ei = '资讯广告点击' and get_json_object(kv,'$.type') = '视频') ) )   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '视频列表项')
                then '视频'
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei
            
            
            
            union all 
            
            select 
            sdate,
            id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv 
            from 
            (
            select
            sdate, 
            id,
            case 
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') ='图片')   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '图片列表项')
                then '图片'
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei
            
            
            
            union all 
            
            select 
            sdate,
            id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv 
            from 
            (
            select
            sdate, 
            id,
            case 
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') in ('手机','电脑','论坛','游戏'))   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '活动列表项')
                then '活动'
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei
            
            
            union all 
            
            select 
            sdate,
            id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv 
            from 
            (
            select
            sdate, 
            id,
            case 
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') not in ('活动','手机','论坛','官网','游戏'))   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.infoType') not in ('活动','手机','论坛','官网','游戏'))
                then '运营内容'
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei
            
            
            """ % (sDate,sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)





    tdw.WriteLog("== end OK ==")
    
    
    
    
    