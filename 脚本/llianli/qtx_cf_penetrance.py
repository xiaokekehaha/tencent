#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_cf_penetrance.py
# 功能描述:     掌上穿越火线功能渗透率统计
# 输入参数:     yyyymmdd    例如：20141230
# 目标表名:     ieg_qt_community_app.qtx_cf_penetrance
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2014-12-30
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
    ##sDate = '20141222'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS qtx_cf_penetrance
            (
            sdate int,
            id bigint,
            total_uin bigint,
            ei string,
            click_cnt bigint,
            click_uin bigint 
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_cf_penetrance where sdate=%s""" % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table qtx_cf_penetrance
            select
            cast(t1.sdate as int) as sdate,
            cast(t1.id as bigint) as id,
            cast(t2.total_uin  as bigint) as total_uin,
            t1.ei as ei ,
            cast(t1.click_cnt  as bigint) as click_cnt,
            cast(t1.click_uin  as bigint) as click_uin 
            from
            (
            select
            sdate,
            id,
            ei,
            count(*) as click_cnt,
            count(distinct uin_mac) as click_uin 
            from
            (
            select
            sdate,
            id,
            ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031)
            )t  group by sdate,id,ei
            )t1
            join
            (
            select
            sdate,
            id,
            count(distinct uin_info) as total_uin
            from
            (
            select
            sdate,
            id,
            concat(ui,mc) as  uin_info 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031)
            )j_1  group by sdate,id 
            )t2  on (t1.id=t2.id  and t1.sdate=t2.sdate ) 

                    """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    

    
    
    
    