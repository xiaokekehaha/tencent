#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_bbs_static.py
# 功能描述:     手游宝各功能pvuv数据计算
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.syb_bbs_source_statics
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
            CREATE TABLE IF NOT EXISTS syb_bbs_source_data
            (
            fdate int,
            plat string,
            src string,
            pv bigint,
            uv bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from syb_bbs_source_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)

    ##全体数据不分平台部分来源
    sql = """
    insert  table  syb_bbs_source_data
    select 
    %s,
    'all',
    'all',
    count(*) as pv,
    count(distinct pvi) as uv 
    from  dw_ta::ta_log_bbs_g_qq_com 
    where daytime = %s and rt = 'forum'
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    ##不同平台数据，不区分来源
    sql = """
    insert  table  syb_bbs_source_data
    SELECT 
    %s,
    plat,
    'all',
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    daytime,
    case 
    when dm = 'BBS.G.QQ.COM' then 'PC' 
    when dm = 'AGAME.QQ.COM'  and instr(ua,'GAMEJOY') = 0 then 'H5'
    when instr(ua,'GAMEJOY') != 0 then 'APP' 
    else 'unknow_plat' 
    end as plat,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  and rt = 'forum'
    )t group by plat
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    ##PC数据区分来源
    sql = """
    insert  table  syb_bbs_source_data
    SELECT 
    %s,
    'PC',
    source,
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    daytime,
    CASE 
    WHEN  refer LIKE '%%baidu.com%%' or refer LIKE '%%google.com%%' or refer LIKE '%%sogou.com%%'  or refer LIKE '%%bing.com%%' and refer LIKE '%%360.com%%' then 'search_engine' 
    WHEN  refer != '--' and INSTR(refer,'sogou.com') = 0 and INSTR(refer,'baidu.com') = 0 and INSTR(refer,'google.com') = 0 and INSTR(refer,'360.com') = 0 and INSTR(refer,'bing.com') = 0  and INSTR(refer,'bbs.g.qq.com') = 0 then 'outer'
    ELSE 'enter_direct' 
    end as source,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  and rt = 'forum' and  dm = 'BBS.G.QQ.COM'
    )t 
    group by source
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    ##instr(ua,'Android') = 0 and  instr(ua,'iPhone') = 0 and instr(ua,'iPad') = 0 and  instr(ua,'BlackBerry') = 0 and instr(ua,'Nokia') = 0 and instr(ua,'SonyEricsson') = 0
    
    
    
    ##H5数据区分来源，但不区分安卓和IOS
    sql = """
    insert  table  syb_bbs_source_data
    SELECT 
    %s,
    'H5',
    source,
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    CASE 
    WHEN  instr(adt, 'game.app.') != 0  or instr(adt, 'game.web.') != 0 or instr(arg,'appid%%3D') != 0  then 'game_direct'
    WHEN  instr(arg ,'.xiaomi')!=0 then 'assist'
    WHEN  instr(arg,'gqq_from%%3D') != 0 then 'cooperation'  
    WHEN  instr(ua , 'MicroMessenger') != 0   then 'wx_enter'
    WHEN  instr(ua, 'QQ/') != 0  then 'cell_qq_enter'      
    else 'enter_direct'
    end as source,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  and rt = 'forum' and dm = 'AGAME.QQ.COM' and instr(ua,'GAMEJOY') = 0
    )t 
    group by source
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    ##( instr(ua,'Android') != 0 or  instr(ua,'iPhone') != 0  or  instr(ua,'iPod') != 0  or instr(ua,'iPad') != 0 or  instr(ua,'BlackBerry') != 0 or instr(ua,'Nokia') != 0   or instr(ua,'SonyEricsson') != 0 )
    
    
    
    ##H5数据区分来源，且区分安卓和IOS
    sql = """
    insert  table  syb_bbs_source_data
    SELECT 
    %s,
    plat,
    source,
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    CASE
    WHEN ua like '%%Android%%' THEN 'H5-android' 
    WHEN ua like '%%iPad%%' or ua like '%%iPhone%%' or ua like '%%iPod%%' THEN 'H5-ios' 
    else 'H5-other_os' 
    END AS plat,
    CASE 
    WHEN  instr(adt, 'game.app.') != 0  or instr(adt, 'game.web.') != 0 or instr(arg,'appid%%3D') != 0  then 'game_direct'
    WHEN  INSTR(arg ,'.xiaomi')!=0 then 'assist'
    WHEN  instr(arg,'gqq_from%%3D') != 0 then 'cooperation'  
    WHEN  instr(ua , 'MicroMessenger') != 0   then 'wx_enter'
    WHEN  instr(ua, 'QQ/') != 0  then 'cell_qq_enter'  
    else 'enter_direct'
    end as source,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  and rt = 'forum' and dm='AGAME.QQ.COM' and instr(ua,'GAMEJOY') = 0
    )t 
    group by plat,source
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##H5数据不区分来源，区分安卓和IOS
    sql = """
    insert  table  syb_bbs_source_data
    SELECT 
    %s,
    plat,
    'all',
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    CASE
    WHEN ua like '%%Android%%' THEN 'H5-android' 
    WHEN ua like '%%iPad%%' or ua like '%%iPhone%%' or ua like '%%iPod%%' THEN 'H5-ios' 
    else 'H5-other_os' 
    END AS plat,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  and rt = 'forum' and dm='AGAME.QQ.COM' and instr(ua,'GAMEJOY') = 0
    )t 
    group by plat
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    
    ##APP数据不区分来源，区分安卓和IOS
    sql = """
    insert  table  syb_bbs_source_data
    SELECT 
    %s,
    plat,
    'all',
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    CASE
    WHEN ua like '%%Android%%' THEN 'APP-andriod' 
    WHEN ua like '%%iPad%%' or ua like '%%iPhone%%' or ua like '%%iPod%%' THEN 'APP-ios' 
    else 'APP-other_os' 
    END AS plat,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s and rt = 'forum' and instr(ua,'GAMEJOY') != 0
    )t 
    group by plat
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    