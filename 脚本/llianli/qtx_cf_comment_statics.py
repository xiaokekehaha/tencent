#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_cf_comment_statics.py
# 功能描述:     掌上穿越火线评论数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_cf_comment_statics
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
            CREATE TABLE IF NOT EXISTS qtx_cf_comment_statics
            (
            sdate int,
            id bigint,
            ei string,
            pv bigint,
            uv bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_cf_comment_statics where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table qtx_cf_comment_statics
            
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
                when (id = 1100679031 and ei in ('写评论点击次数','查看评论点击次数','资讯回复评论','赞评论点击次数') )   or 
                     (id = 1200679031 and ei in ('发表评论','进入评论页面','发表回复','点击赞'))
                then '评论整体'
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
                when (id = 1100679031 and ei in ('写评论点击次数') )   or 
                     (id = 1200679031 and ei in ('发表评论') )
                then '写评论'
                
                when (id = 1100679031 and ei in ('查看评论点击次数') )   or 
                     (id = 1200679031 and ei in ('进入评论页面') )
                then '查看评论'
                
                when (id = 1100679031 and ei in ('资讯回复评论') )   or 
                     (id = 1200679031 and ei in ('发表回复') )
                then '回复评论'
                
                when (id = 1100679031 and ei in ('赞评论点击次数') )   or 
                     (id = 1200679031 and ei in ('点击赞') )
                then '评论点赞'
            
            
            end as ei,
            concat(ui,mc) as uin_mac 
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031)
            ) where ei is not null
            group by sdate, id, ei

            
            """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)





    tdw.WriteLog("== end OK ==")
    
    
    
    
    