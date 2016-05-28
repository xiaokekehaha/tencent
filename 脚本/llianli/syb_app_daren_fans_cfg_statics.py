#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_daren_fans_cfg_statics.py
# 功能描述:     手游宝APP达人粉丝数据配置文件
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_login_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-11-27
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime
import time


def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
    timestamp = int(time.mktime(time.strptime(sDate + " 23:59:59", '%Y%m%d %H:%M:%S')))
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
   
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


     ##首先解决配置的问题，即读取的昵称、读取的手游宝账户，类型等问题
    sql = '''
      CREATE TABLE  IF NOT EXISTS tb_syb_daren_nick_name_cfg
      (
      dtstatdate INT COMMENT '统计日期',
      isybid BIGINT COMMENT '达人手游宝ID',
      stype STRING COMMENT '类型',
      suseralias STRING COMMENT '用户昵称',
      susername STRING COMMENT '用户手游宝用户名',
      report_timestamp BIGINT COMMENT '生成记录时间戳'
      )
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_syb_daren_nick_name_cfg WHERE  dtstatdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据配置写入表中
    sql = ''' 
                    INSERT TABLE tb_syb_daren_nick_name_cfg
        SELECT
        %s AS dtstatdate,
        isybid,
        stype,
        suseralias,
        susername,
        MAX(report_timestamp)
        FROM
        (
        SELECT
        t1.isybid AS isybid,
        t2.stype AS stype,
        t2.suseralias AS suseralias,
        t2.susername AS susername,
        t1.max_report_timestamp AS report_timestamp
        FROM
        (
        SELECT
        isybid,
        MAX(report_timestamp) AS max_report_timestamp
        FROM
        (
        SELECT
        isybid,
        unix_timestamp(dteventtime) AS report_timestamp
        FROM
         ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
        AND iactiontype = 2000016
        AND iactionid IN (
        2000039
        ,2000038
        ) 
        
        UNION ALL 
        
        SELECT 
        isybid,
        report_timestamp 
        FROM tb_syb_daren_nick_name_cfg WHERE dtstatdate = %s
        
        )t
        GROUP BY isybid
        )t1
        JOIN
        (
        SELECT
        isybid
        ,vv3 AS stype
        ,vv1 AS suseralias
        ,vv2 AS susername
        ,unix_timestamp(dteventtime) AS report_timestamp
        FROM  ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
        AND iactiontype = 2000016
        AND iactionid IN (
        2000039
        ,2000038
        ) 
        
        
        UNION ALL 
        
        SELECT 
        isybid,
        stype, 
        suseralias,
        susername,
        report_timestamp
        FROM tb_syb_daren_nick_name_cfg WHERE dtstatdate = %s
        )t2
        ON(t1.isybid = t2.isybid AND t1.max_report_timestamp = t2.report_timestamp)
        )t3
        GROUP BY isybid,stype,suseralias,susername

    '''%(sDate,sDate,sDate,pre_date_str,sDate,sDate,pre_date_str)
    
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    
    
    sql = '''
    INSERT TABLE  tb_syb_daren_nick_name_cfg
    SELECT 
    %s AS dtstatdate,
    t.isybid AS isybid,
    t.stype AS stype,
    t.snickname AS snickname,
    CASE WHEN t1.sybid IS NULL THEN '' ELSE t1.sybaccount END AS ssybaccount,
    %s AS reporttime
    FROM 
    syb_app_daren_nickname_stype_temp t 
    LEFT JOIN
    syb_app_daren_account_temp t1
    on(t.isybid = t1.sybid) 
    '''%(sDate,str(timestamp))
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    

    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    