#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cf_app_flottery_client_data.py
# 功能描述:     掌上穿越火线抽奖功能客户端相关事件统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_cf_app_flottery_client_click
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-02-01
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
            CREATE TABLE IF NOT EXISTS tb_cf_app_flottery_client_click
            (
            sdate int,
            id bigint,
            ei string,
            pv bigint,
            total_uin bigint,
            total_mac bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_cf_app_flottery_client_click where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table tb_cf_app_flottery_client_click
            select 
            %s as sdate,
            case when grouping(id) = 1 then -100 else id end as id,
            ei,
            count(*) as pv,
            count(distinct uin) as total_uin,
            count(distinct ui_mc) as total_mac 
            from
            (
            select 
            id,
            concat(ui,mc) as ui_mc,
            get_json_object(kv,'$.uin') as uin ,
            case 
                when ( id = 1100679031 and ei = '王者宝藏点击次数') or  
                     ( id = 1200679031 and ei = '抽奖_模块点击') 
                then '王者宝藏模块'
                
                when ( id = 1100679031 and ei = '抽奖页面点击量') or  
                     ( id = 1200679031 and ei = '抽奖_TAB展示次数' and get_json_object(kv,'$.type') = '宝藏')  
                then '抽奖页面'
                
                when ( id = 1100679031 and ei = '分享点击次数') or  
                     ( id = 1200679031 and ei = '抽奖_结果界面分享次数' )  
                then '分享点击次数'
                
                when ( id = 1100679031 and ei = '排行榜页面点击量') or  
                     ( id = 1200679031 and ei = '抽奖_TAB展示次数' and get_json_object(kv,'$.type') = '排行')  
                then '排行页面'
                
                
                when ( id = 1100679031 and ei = '兑换页面点击量') or  
                     ( id = 1200679031 and ei = '抽奖_TAB展示次数' and get_json_object(kv,'$.type') = '兑换')  
                then '兑换页面'
                
                when ( id = 1100679031 and ei = '记录页面点击量') or  
                     ( id = 1200679031 and ei = '抽奖_TAB展示次数' and get_json_object(kv,'$.type') = '记录')  
                then '记录页面'
                else 'other' 
            end as ei 
            from teg_mta_intf::ieg_lol where sdate = %s and id in (1100679031,1200679031)
            )t 
            where ei != 'other'
            group by cube(id),ei
            """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    tdw.WriteLog("== end OK ==")
    
    
    
    
    