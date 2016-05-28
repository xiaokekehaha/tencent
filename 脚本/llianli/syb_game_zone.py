#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_lol_penetrance.py
# 功能描述:     掌上英雄联盟资讯阅读量统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_lol_info_base
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2014-11-03
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
    ##sDate = '20141026'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS syb_game_ana
            (
            sdate int,
            game_id bigint,
            dnu_cross_uin bigint
            ) """
    print sql
    res = tdw.execute(sql)

    sql="""delete from syb_game_ana where sdate=%s""" % (sDate)
    res = tdw.execute(sql)


    sql = """
           insert  table syb_game_ana
           select 
           %s,
           t.game_id,
           count(distinct t.user_uuid) as cross_dun 
           from 
           (
            select 
            game_id,
            user_uuid from syb_user_uuid_svr_dnu_2 
            where logdate = %s 
            group by game_id,user_uuid
            )t 
            join 
            (
            select 
            distinct suin from ieg_mg_oss_app::iplat_fat_syb_app_round_tbaccount where iregdate = %s and saccounttype = '-100'  
            )t1 
            on (t.user_uuid = t1.suin) 
            group by t.game_id
                    """ % (sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    for i in res:
        print i


    tdw.WriteLog("== end OK ==")
    
    
   
    
    
    
    