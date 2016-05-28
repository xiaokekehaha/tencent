#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_bbs_static_import_user.py
# 功能描述:     手游宝门户论坛签到抽奖活动
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.syb_bbs_sign_lottery_click
# 数据源表:     teg_dw_tcss.tcss_agame_qq_com
# 创建人名:     llianli
# 创建日期:     2015-06-19
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
            CREATE TABLE IF NOT EXISTS syb_bbs_sign_lottery_click
            (
            fdate int,
            ei string,
            pv bigint,
            uv bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from syb_bbs_sign_lottery_click where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)
    

    ##各个场景下每个页面点击数据
    sql = """
    insert  table  syb_bbs_sign_lottery_click
    select
    %s,
    concat('click_',ei) as ei,
    count(*) as pv,
    count(distinct f_pvid) as uv
    from 
    (
    select 
    f_rdm_rurl as ei,
    f_pvid 
    from  teg_dw_tcss::tcss_agame_qq_com 
    where f_date = %s and f_dm = 'AGAME.QQ.COM' and f_url like '%%/act/dnw20150610%%' and instr(f_user_agent , 'GAMEJOY') = 0
    )t
    group by ei
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    ##各种场景下每个页面每个按钮点击数据
    sql = """
    insert  table  syb_bbs_sign_lottery_click
    select
    %s,
    ei,
    count(*) as pv,
    count(distinct f_pvid) as uv
    from 
    (
    select 
    f_hottag as ei,
    f_pvid 
    from  teg_dw_tcss::tcss_agame_qq_com  
    where f_date = %s and f_dm = 'AGAME.QQ.COM.HOT' and f_url like '%%/act/dnw20150610%%' and  f_hottag like '%%activitydnw%%' and instr(f_user_agent , 'GAMEJOY') = 0 
    )t
    group by ei
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##APP内打开
    sql = """
    insert  table  syb_bbs_sign_lottery_click
    select
    %s,
    'inner_app_open',
    count(*) as pv,
    count(distinct f_pvid) as uv
    from 
    (
    select 
    f_pvid 
    from  teg_dw_tcss::tcss_agame_qq_com  
    where f_date = %s and f_dm = 'AGAME.QQ.COM' and f_url like '%%/act/dnw20150610%%'  
    )t
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    tdw.WriteLog("== end OK ==")
    
    
    
    