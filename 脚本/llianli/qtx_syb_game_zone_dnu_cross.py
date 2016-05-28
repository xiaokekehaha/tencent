#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_game_zone_dnu_corss.py
# 功能描述:     手游宝游戏专区各个新增用户与手游宝全部新增用户交叉
# 输入参数:     yyyymmdd    例如：20150520
# 目标表名:     ieg_qt_community_app.tb_syb_game_zone_dnu_cross
# 数据源表:     ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount  hy::t_dw_mkt_syb_active_user
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


    ##计算游戏分开日新增和总体日新增与游戏之间的交叉关系
    sql = """
            CREATE TABLE IF NOT EXISTS qtx_syb_game_zone_dnu_corss
            (
            fdate int,
            game_id bigint,
            dnu_cross_uin bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_syb_game_zone_dnu_corss where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
    insert table  qtx_syb_game_zone_dnu_corss 
    select 
    t.sdate,
    t.game_id,
    count(distinct user_uuid)
    from 
    (
    select 
    sdate,
    user_uuid,
    game_id 
     from hy::t_dw_mkt_syb_active_user where statis_date = %s and flag = 1 and game_id != 0
     group by sdate,user_uuid,game_id
     )t 
     join 
     (
     select 
      
     distinct suin,iregdate  from  ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount where  dtstatdate = %s  and iregdate = %s and saccounttype = '-100' 
     )t1 
     on (t.sdate = t1.iregdate and t.user_uuid = t1.suin) 
     group by t.sdate,    t.game_id """ %(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    insert table  qtx_syb_game_zone_dnu_corss 
    select 
    t.sdate,
    -100,
    count(distinct user_uuid)
    from 
    (
    select 
    sdate,
    user_uuid 
     from hy::t_dw_mkt_syb_active_user where statis_date = %s and flag = 1 and game_id = 0
     group by sdate,user_uuid
     )t 
     join 
     (
     select 
     distinct suin,iregdate  from  ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount where dtstatdate = %s and iregdate = %s and saccounttype = '-100' 
     )t1 
     on (t.sdate = t1.iregdate and t.user_uuid = t1.suin) 
     group by t.sdate""" %(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##计算总体的活跃，总体的新增，各分区游戏的活跃及新增
    sql = """
            CREATE TABLE IF NOT EXISTS qtx_syb_game_zone_dau_dnu
            (
            fdate int,
            game_id bigint,
            dau bigint,
            dnu bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_syb_game_zone_dau_dnu where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    
    
    ##各个游戏活跃新增
    sql = """
    insert table  qtx_syb_game_zone_dau_dnu
    select 
    sdate,
    game_id,
    count(distinct dau_uin) as dau,
    count(distinct dnu_uin) as dnu 
    from 
    ( 
    select 
    sdate,
    game_id,
    case when flag = 0 then user_uuid else null  end as dau_uin,
    case when flag = 1 then user_uuid else null  end as dnu_uin 
    from  hy::t_dw_mkt_syb_active_user where statis_date = %s  and game_id != 0
    )t 
    group by sdate,game_id
    """% (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##全体活跃新增
    sql = """
    insert table  qtx_syb_game_zone_dau_dnu
    select 
    sdate,
    -100,
    count(distinct dau_uin) as dau,
    count(distinct dnu_uin) as dnu 
    from 
    ( 
    select 
    sdate,
    case when flag = 0 then user_uuid else null  end as dau_uin,
    case when game_id = 0 and flag = 1 then user_uuid else null  end as dnu_uin 
    from  hy::t_dw_mkt_syb_active_user where statis_date = %s  
    )t 
    group by sdate
    """% (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    