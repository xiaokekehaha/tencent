#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_push_mail_data.py
# 功能描述:     手游宝推送日报数据表计算
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_push_mail_data
# 数据源表:     ieg_mg_oss_app::iplat_dm_syb_app_round_tbRegisterUser  ieg_mg_oss_app::iplat_dm_syb_app_round_tbUserActivity  iplat_dm_syb_app_round_tbstayscaledis 
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
            CREATE TABLE IF NOT EXISTS tb_syb_push_mail_data
            (
            fdate int,
            sgamecode  string,
            active_uin bigint,
            reg_uin bigint,
            preday_reg_num bigint,
            preday_reg_stay_num bigint,
            preday_act_num bigint,
            preday_act_stay_num bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_syb_push_mail_data where fdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
    insert table  tb_syb_push_mail_data
    select 
    t.dtstatdate as fdate,
    t.sgamecode as sgamecode,
    t.iactivitynum as iactivitynum,
    t1.iregnum as iregnum,
    t2.preday_reg_num as preday_reg_num,
    t2.preday_reg_stay_num as preday_reg_stay_num,
    t2.preday_act_num as preday_act_num,
    t2.preday_act_stay_num  as preday_act_stay_num
    from  
    (
    select 
    dtstatdate,
    sgamecode,
    iactivitynum 
    from  
    ieg_mg_oss_app::iplat_dm_syb_app_round_tbuseractivity 
    where dtstatdate = %s 
    and sgamecode in ('-100','android','ios') 
    and sAccountType = '-100' 
    and splattype = '-100'
    and splat = '-100' 
    and iperiod = 1
    )t
    join
    (
    select 
    dtstatdate,
    sgamecode,
    iregnum
    from  
    ieg_mg_oss_app::iplat_dm_syb_app_round_tbregisteruser 
    where dtstatdate = %s 
    and sgamecode in ('-100','android','ios') 
    and sAccountType = '-100' 
    and splattype = '-100'
    and splat = '-100' 
    and iperiod = 1
    )t1
    on (t.dtstatdate = t1.dtstatdate and t.sgamecode = t1.sgamecode)
    join
    (
    select 
    dtstatdate,
    sgamecode,
    sum(preday_reg_num) as preday_reg_num,
    sum(preday_reg_stay_num) as preday_reg_stay_num,
    sum(preday_act_num) as preday_act_num,
    sum(preday_act_stay_num) as preday_act_stay_num 
    from 
    ( 
    select 
    dtstatdate,
    sgamecode,
    case when ssourceuser = 'DayReg' then iallnum else 0  end preday_reg_num,
    case when ssourceuser = 'DayReg' then iactivitynum else 0  end preday_reg_stay_num,
    case when ssourceuser = 'DayActi' then iallnum else 0  end preday_act_num,
    case when ssourceuser = 'DayActi' then iactivitynum else 0  end preday_act_stay_num
    from 
    (
    select 
    dtstatdate,
    sgamecode,
    ssourceuser,
    iallnum,
    iactivitynum
    from  
    ieg_mg_oss_app::iplat_dm_syb_app_round_tbstayscaledis 
    where dtstatdate = %s 
    and sgamecode in ('-100','android','ios') 
    and sAccountType = '-100' 
    and splattype = '-100'
    and splat = '-100' 
    and ilookday = 2
    and ssourceuser in ('DayReg','DayActi')
    )tmp1
    )tmp2 
    group by dtstatdate,sgamecode
    )t2
    on  (t.dtstatdate = t2.dtstatdate and t.sgamecode = t2.sgamecode)

    """%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    