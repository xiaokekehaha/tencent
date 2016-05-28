#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_lol_live_data.py
# 功能描述:     LOL赛事直播数据
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_ttxd_click_data
# 数据源表:     teg_mta_intf::ieg_lol 
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



    sql = """
            CREATE TABLE IF NOT EXISTS tb_qtx_lol_live_data
            (
            fdate int,
            id   bigint,
            pv bigint,
            uv bigint,
            uv2 bigint,
            itotaltime bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_qtx_lol_live_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = ''' 
    insert table tb_qtx_lol_live_data
    select 
    %s as fdate,
    id,
    sum(pv) as pv,
    sum(uv) as uv,
    sum(uv2) as uv2,
    sum(time) as time 
    from 
    (
    select 
    id,
    case when ei = 'LivePlaying'  then pv else 0 end as pv,
    case when ei = 'LivePlaying'  then uv else 0 end as uv,
    case when ei = 'LiveTime'  then uv else 0 end as uv2,
    case when ei = 'LiveTime'  then time else 0 end as time 
    from 
    (
    select 
    id,
    ei,
    count(*) as pv,
    count(distinct uin_info) as uv,
    sum(time) as time 
    from 
    (
    select
    id, 
    ei,
    concat(ui,mc) as uin_info, 
    du as time
    from teg_mta_intf::ieg_lol where sdate = %s and ei in ('LiveTime','LivePlaying') and du < 3*3600
    )t  group by id,ei
    )t1
    )t2
    group by id
    '''%(sDate,sDate)
    
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

  
    tdw.WriteLog("== end OK ==")
    
    
    
    