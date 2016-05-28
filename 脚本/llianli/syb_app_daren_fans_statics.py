#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_daren_fans_statics.py
# 功能描述:     手游宝APP达人粉丝数据计算
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
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

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
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


     ##建立表，将每日的isybid对应的数字写入
    sql = '''
          CREATE TABLE IF NOT EXISTS tb_syb_app_daren_account
    (
    dtstatdate BIGINT COMMENT '统计日期',
    isybid BIGINT COMMENT '手游宝ID',
    ntotal BIGINT COMMENT '粉丝总数',
    nregnum BIGINT COMMENT '日新增粉丝数'
    )
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_syb_app_daren_account WHERE  dtstatdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据配置写入表中
    sql = ''' 
      INSERT TABLE tb_syb_app_daren_account
      SELECT
      %s AS dtstatdate,
      isybid,
      SUM(ntotal) AS ntotal,
      SUM(nregnum) AS nregnum
      FROM
      (
      SELECT 
      isybid,
      ntotal,
      0 AS nregnum
      FROM tb_syb_app_daren_account WHERE dtstatdate = %s
      
      UNION ALL
       
      SELECT
      isybid
      ,SUM(CASE
      WHEN iactionid = 2000039 THEN -1
      WHEN iactionid = 2000038 THEN 1
      END) AS ntotal,
      SUM(CASE
      WHEN iactionid = 2000039 THEN -1
      WHEN iactionid = 2000038 THEN 1
      END) AS nregnum
      FROM
       ieg_tdbank :: gqq_dsl_day_task_bill_fht0
      WHERE
      tdbank_imp_date BETWEEN %s00 AND %s23
      AND iactiontype = 2000016
      AND iactionid IN (
      2000039
      ,2000038
      )
      GROUP BY
      isybid
      )t
      GROUP BY isybid
    '''%(sDate,pre_date_str,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##将完整数据写入
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_syb_app_daren_account_total
      (
      dtstatdate BIGINT COMMENT '统计日期',
      isybid BIGINT COMMENT '手游宝ID',
      stype STRING COMMENT '类型',
      suseralias STRING COMMENT '用户昵称',
      susername STRING COMMENT '用户手游宝用户名',
      ntotal BIGINT COMMENT '粉丝总数',
      nregnum BIGINT COMMENT '日新增粉丝数'
      )
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_syb_app_daren_account_total WHERE  dtstatdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将包括账号信息的数据全部写入数据表中
    sql = ''' 
            INSERT TABLE tb_syb_app_daren_account_total
      
      SELECT
      %s AS dtstatdate,
      t.isybid,
      CASE WHEN t1.stype IS NULL THEN ' ' ELSE t1.stype END AS stype,
      CASE WHEN t1.suseralias IS NULL THEN ' ' ELSE t1.suseralias END AS suseralias,
      CASE WHEN t1.susername IS NULL THEN ' ' ELSE t1.susername END AS susername,
      t.ntotal,
      t.nregnum
      FROM
      (
      SELECT
      isybid,
      ntotal,
      nregnum 
      FROM tb_syb_app_daren_account WHERE dtstatdate = %s
      )t
      LEFT OUTER JOIN
      (
      SELECT
      isybid,
      stype,
      suseralias,
      susername 
      FROM tb_syb_daren_nick_name_cfg WHERE dtstatdate = %s
      )t1
      ON (t.isybid = t1.isybid)
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    


    tdw.WriteLog("== end OK ==")
    
    
    
    