#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_gift_share.py
# 功能描述:     手游宝礼包数据分享统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_syb_gift_share
# 数据源表:     teg_dw_tcss::tcss_agame_qq_com
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


    ##H5来源数据统计
    sql = '''
    CREATE TABLE IF NOT EXISTS qtx_syb_gift_share
        (
        dtstatdate INT COMMENT '统计时间',
        ei STRING COMMENT '礼包分享对应事件',
        pv BIGINT COMMENT '点击PV',
        uv BIGINT COMMENT '点击UV'
        ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##H5整体来源统计
    sql="""delete from qtx_syb_gift_share where dtstatdate=%s  """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##点击流计算的PV UV
    sql = """
    INSERT TABLE qtx_syb_gift_share
       SELECT 
 f_date,
 ei,
 COUNT(*) AS pv,
 COUNT(DISTINCT f_pvid) AS uv
 FROM 
 (
 SELECT 
 f_date,
 f_pvid,
 CASE 
     WHEN f_dm = 'AGAME.QQ.COM' AND f_url LIKE '%%/gift/inapp/share.shtml%%' THEN 'total_pv'
     WHEN f_dm = 'AGAME.QQ.COM.HOT' AND f_url LIKE  '%%/gift/inapp/share.shtml%%' AND lower(f_hottag) = 'gift.share.j_goto_syb' THEN 'goto_app'
     WHEN f_dm = 'AGAME.QQ.COM.HOT' AND f_url LIKE  '%%/gift/inapp/share.shtml%%' AND lower(f_hottag) = 'gift.share.j_goto_syb_banner' THEN 'bottom_banner_free'
     ELSE 'other'
 END AS ei 
 
 FROM  
 
 teg_dw_tcss::tcss_agame_qq_com WHERE f_date = %s AND f_dm in ('AGAME.QQ.COM','AGAME.QQ.COM.HOT')
 )t 
 GROUP BY f_date,ei
    
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##标准行为账单上报的PVUV
    sql = """
    INSERT TABLE qtx_syb_gift_share
    SELECT 
 dtstatdate,
 ei,
 COUNT(*) AS pv,
 COUNT(DISTINCT isybid) as uv 
 FROM
 (
SELECT 
substr(tdbank_imp_date,1,8) as dtstatdate,
isybid,
CASE 
    WHEN iactiontype = 23 AND iactionid = 56 THEN 'activity_enter'
    WHEN iactiontype = 23 AND iactionid = 57 THEN 'packeg_share'
    ELSE 'other'
END AS ei
FROM 
 ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE  tdbank_imp_date >= %s00 AND tdbank_imp_date <= %s23 AND iactiontype = 23 AND iactionid in (56,57)
)t GROUP BY 
 dtstatdate,  ei
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    