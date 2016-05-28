#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_return_uin.py
# 功能描述:     掌盟掌火回流数据统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_return_uin
# 数据源表:     ieg_qt_community_app::tb_app_reg_account
# 创建人名:     llianli
# 创建日期:     2016-03-21
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ********************************q**********************************************


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
           CREATE TABLE IF NOT EXISTS tb_app_return_uin
            (
            dtstatdate INT COMMENT '统计日期',
            sfalg STRING COMMENT '数据标记：活跃回流 or 有效回流',
            idaysflag INT COMMENT '统计周期',
            iclienttype INT COMMENT '客户端类型',
            itotaluin BIGINT COMMENT '总活跃用户',
            ireturnuin BIGINT COMMENT '回流用户'
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_return_uin WHERE dtstatdate = %s """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    ##创建临时表存储中间结果
    sql = """
    CREATE TABLE IF NOT EXISTS tb_app_return_uin_original_temp_%s
        (
        dtstatdate INT COMMENT '统计日期',
        iclienttype INT COMMENT '客户端类型',
        iuin BIGINT COMMENT 'UIN',
        
        iweek1actdays INT COMMENT '周活跃第一周期',
        iweek2actdays INT COMMENT '周活跃第二周期',
        iweek3actdays INT COMMENT '周活跃第三周期',
        
        idweek1actdays INT COMMENT '双周活跃第一周期',
        idweek2actdays INT COMMENT '双周活跃第二周期',
        idweek3actdays INT COMMENT '双周活跃第三周期',
        
        imonth1actdays INT COMMENT '月活跃第一周期',
        imonth2actdays INT COMMENT '月活跃第二周期',
        imonth3actdays INT COMMENT '月活跃第三周期'
        
        ) 
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##数据写入临时表
    sql = """
    INSERT OVERWRITE TABLE tb_app_return_uin_original_temp_%(sDate)s
    SELECT
    %(sDate)s AS dtstatdate,
    iclienttype,
    iuin,
    iweek1actdays,
    iweek2actdays,
    iweek3actdays,
    
    idweek1actdays,
    idweek2actdays,
    idweek3actdays,
    
    imonth1actdays,
    imonth2actdays,
    imonth3actdays
    
    FROM 
    (
    SELECT
    iclienttype,
    iuin,
    length(regexp_replace(substr(cbitmap,15,7),'0','')) AS iweek1actdays,
    length(regexp_replace(substr(cbitmap,8,7),'0','')) AS iweek2actdays,
    length(regexp_replace(substr(cbitmap,1,7),'0','')) AS iweek3actdays,
    
    length(regexp_replace(substr(cbitmap,29,14),'0','')) AS idweek1actdays,
    length(regexp_replace(substr(cbitmap,15,14),'0','')) AS idweek2actdays,
    length(regexp_replace(substr(cbitmap,1,14),'0','')) AS idweek3actdays,
    
    
    length(regexp_replace(substr(cbitmap,61,30),'0','')) AS imonth1actdays,
    length(regexp_replace(substr(cbitmap,31,30),'0','')) AS imonth2actdays,
    length(regexp_replace(substr(cbitmap,1,30),'0','')) AS imonth3actdays
    
     
    FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s
    )t  
    """%(locals())
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    sql = """
            INSERT TABLE tb_app_return_uin

            SELECT
            %(sDate)s AS dtstatdate,
            sflag,
            idayflag,
            iclienttype,
            SUM(iactuin) AS iactuin,
            SUM(ireturnuin) AS ireturnuin
            
            FROM
            (
            SELECT
            'total' AS sflag,
            7 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN iweek1actdays !=0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN iweek1actdays != 0 AND iweek2actdays = 0 AND iweek3actdays != 0 THEN 1 ELSE 0 END AS ireturnuin
            FROM tb_app_return_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            'total' AS sflag,
            14 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN idweek1actdays != 0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN idweek1actdays != 0 AND idweek2actdays = 0 AND idweek3actdays != 0 THEN 1 ELSE 0 END AS ireturnuin
            FROM tb_app_return_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            'total' AS sflag,
            30 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN imonth1actdays != 0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN imonth1actdays != 0 AND imonth2actdays = 0 AND imonth3actdays != 0 THEN 1 ELSE 0 END AS ireturnuin
            FROM tb_app_return_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            'effect' AS sflag,
            30 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN imonth1actdays != 0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN imonth1actdays != 0 AND imonth2actdays = 0 AND imonth3actdays > 2 THEN 1 ELSE 0 END AS ireturnuin
            FROM tb_app_return_uin_original_temp_%(sDate)s
            )t
            GROUP BY sflag,idayflag,iclienttype


                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DROP TABLE tb_app_return_uin_original_temp_%s """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    