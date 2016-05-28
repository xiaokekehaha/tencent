#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_pindao_statics.py
# 功能描述:     掌盟频道PVUV统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_lol_app_daishi_pv
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
            
            CREATE TABLE IF NOT EXISTS tb_lol_app_pindao_pv
            (
            sdate INT,
            itimerange INT,
            sid STRING,
            pv BIGINT,
            uv BIGINT
            ) """
    res = tdw.execute(sql)

    sql="""delete from tb_lol_app_pindao_pv where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
        INSERT TABLE tb_lol_app_pindao_pv
                    SELECT
        %s AS sdate,
        1 as itimerange,
        t2.id AS sid,
        SUM(pv) AS pv,
        COUNT(DISTINCT uin_info) AS uv 
        FROM
        (
        SELECT 
        url,
        uin_info,
        COUNT(*) AS pv
        FROM
        (
        SELECT 
        concat(ui,mc) AS uin_info,
        get_json_object(kv,'$.uin') AS uin,
        get_json_object(kv,'$.url') AS url
        FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id in (1100678382,1200678382)
        AND ei = '资讯详情'
        )t
        GROUP BY url,uin_info
        )t1 
        JOIN
        (
        select 
         ichannelid as id,
         surl as url
         from 
        ieg_tdbank::qqtalk_gpcd_dsl_tb_lol_app_channel_id_url_cfg_fdt0 where tdbank_imp_date = %s
        ) t2
        ON (t1.url = t2.url)
        group by t2.id
            """ % (sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    tdw.WriteLog("== end OK ==")
    
    
    
    
    