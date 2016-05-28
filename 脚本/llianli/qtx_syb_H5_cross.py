#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_h5_cross.py
# 功能描述:     手游宝H5数据和大盘用户交叉结果存储
# 输入参数:     yyyymmdd    例如：20150606
# 目标表名:     ieg_qt_community_app.tb_syb_h5_cross
# 数据源表:     ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount
# 创建人名:     llianli
# 创建日期:     2015-08-26
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
            CREATE TABLE IF NOT EXISTS tb_syb_h5_cross_ana
            (
            dtstatdate int COMMENT '统计时间',
            istatflag  int COMMENT '统计标识：1:交叉，2：并集',
            sdataflag string COMMENT '数据标识：act:活跃，reg:新增',
            iresult bigint COMMENT '交叉结果'
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_syb_h5_cross_ana where dtstatdate=%s  """ % (sDate)
    res = tdw.execute(sql)

##H5活跃数据和大盘用户交叉
    sql = ''' 
    insert table tb_syb_h5_cross_ana
    
    SELECT
    %s as dtstatdate,
    1 as istatflag,
    'act' as sdataflag,
    COUNT(DISTINCT t.suin)
    FROM 
    (
    SELECT 
    DISTINCT suin FROM  iplat_fat_syb_new_round_tbaccount 
    WHERE dtstatdate = %s AND ilastactdate = %s AND iappid = 0 AND  iplatform = 8 AND ilogintype = -100 AND sappver = '-100'
    )t
    JOIN
    (
    SELECT
    DISTINCT suin
    FROM  ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount 
    WHERE dtstatdate = %s AND ilastactdate = %s AND saccounttype = '-100' AND splattype = '-100' AND splat = '-100' 
    )t1
    on(t.suin = t1.suin)

    '''%(sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##H5新增数据和大盘用户交叉
    sql = ''' 
    insert table tb_syb_h5_cross_ana
    
    SELECT
    %s as dtstatdate,
    1 as istatflag,
    'reg' as sdataflag,
    COUNT(DISTINCT t.suin)
    FROM 
    (
    SELECT 
    DISTINCT suin FROM  iplat_fat_syb_new_round_tbaccount 
    WHERE dtstatdate = %s AND iregdate = %s AND iappid = 0 AND  iplatform = 8 AND ilogintype = -100 AND sappver = '-100'
    )t
    JOIN
    (
    SELECT
    DISTINCT suin
    FROM  ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount 
    WHERE dtstatdate = %s AND iregdate = %s AND saccounttype = '-100' AND splattype = '-100' AND splat = '-100' 
    )t1
    on(t.suin = t1.suin)

    '''%(sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##H5活跃数据和大盘用户并集
    sql = ''' 
    insert table tb_syb_h5_cross_ana
    
    SELECT
    %s as dtstatdate,
    2 as istatflag,
    'act' as sdataflag,
    COUNT(DISTINCT suin)
    FROM 
    (
    SELECT 
    DISTINCT suin FROM  iplat_fat_syb_new_round_tbaccount 
    WHERE dtstatdate = %s AND ilastactdate = %s AND iappid = 0 AND  iplatform = 8 AND ilogintype = -100 AND sappver = '-100'

    UNION ALL

    SELECT
    DISTINCT suin
    FROM  ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount 
    WHERE dtstatdate = %s AND ilastactdate = %s AND saccounttype = '-100' AND splattype = '-100' AND splat = '-100' 
    )t1

    '''%(sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
     ##H5新增数据和大盘用户并集
    sql = ''' 
    insert table tb_syb_h5_cross_ana
    
    SELECT
    %s as dtstatdate,
    2 as istatflag,
    'reg' as sdataflag,
    COUNT(DISTINCT suin)
    FROM 
    (
    SELECT 
    DISTINCT suin FROM  iplat_fat_syb_new_round_tbaccount 
    WHERE dtstatdate = %s AND iregdate = %s AND iappid = 0 AND  iplatform = 8 AND ilogintype = -100 AND sappver = '-100'

    UNION ALL

    SELECT
    DISTINCT suin
    FROM  ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount 
    WHERE dtstatdate = %s AND iregdate = %s AND saccounttype = '-100' AND splattype = '-100' AND splat = '-100' 
    )t1

    '''%(sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##论坛H5数据和APPH5版本的数据做合并求结果
    sql = '''
    insert table tb_syb_h5_cross_ana
    SELECT 
    %s AS dtstatdate,
    2 as istatflag,
    CASE WHEN GROUPING(ssource) = 1 THEN 'h5_-100' ELSE ssource END AS ssource,
    COUNT(DISTINCT suin)
    from 
    (
    SELECT
    DISTINCT ui as suin,
    'bbs_h5' as ssource
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  and rt = 'forum' AND 
    dm = 'AGAME.QQ.COM'  and instr(ua,'GAMEJOY') = 0 
    
    UNION ALL
    
    SELECT
    DISTINCT suin,
    'syb_h5' AS ssource 
    FROM iplat_fat_syb_new_round_tbaccount 
    WHERE dtstatdate = %s AND ilastactdate = %s 
    AND iappid = 0 AND iplatform = 8 AND ilogintype = -100 AND sappver = '-100'
    
    
    )t  GROUP BY cube(ssource) 
    ''' %(sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    