#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_round_tbaccount.py
# 功能描述:     手游宝直播用户活跃情况统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-10-20
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    today_str_2 = today_date.strftime("%Y-%m-%d")
    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，直播的用户数据
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_app_zhibo_round_tbAccount_v2
     (
     dtstatdate BIGINT,
     slogintype STRING,
     iclienttype BIGINT,
     isybid BIGINT,
     itimes BIGINT,
     iRegDate BIGINT,
     iLastActDate BIGINT
     )PARTITION BY LIST (dtstatdate)
            (
            partition p_20151016  VALUES IN (20151016),
            partition p_20151017  VALUES IN (20151017)
            ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_app_zhibo_round_tbAccount_v2 DROP PARTITION (p_%s)''' % (today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table iplat_fat_syb_app_zhibo_round_tbAccount_v2 ADD PARTITION p_%s VALUES in (%s) '''%(today_str,today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##写入直播用户的账户活跃数据
    sql = ''' 
    insert table  iplat_fat_syb_app_zhibo_round_tbAccount_v2
             
        select
        %s dtstatdate ,
        slogintype,
        iclienttype,
        isybid,
        sum(itimes) as itimes,
        min(iRegDate) as iRegDate,
        max(iLastActDate) as iLastActDate
        from
        (
        select
        slogintype,
        iclienttype,
        isybid,
        1 AS itimes,
        %s as iRegDate,
        %s as iLastActDate
        from iplat_fat_syb_app_zhibo_round_tbuseract_v2 PARTITION(p_%s) t 
        
        UNION ALL 
        
        select
        slogintype,
        iclienttype,
        isybid,
        itimes,
        iRegDate,
        iLastActDate
        from iplat_fat_syb_app_zhibo_round_tbAccount_v2 where dtstatdate=%s
        ) 
        group by
        slogintype,
        iclienttype,
        isybid
    '''%(sDate,sDate,sDate,sDate,pre_date_str)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    