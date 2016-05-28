#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_search_data.py
# 功能描述:     手游宝官网搜索数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-10-27
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

    pre_7_date = today_date - datetime.timedelta(days = 7)
    pre_7_date_str = pre_7_date.strftime("%Y%m%d")
    
    
    pre_30_date = today_date - datetime.timedelta(days = 30)
    pre_30_date_str = pre_30_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，直播免费礼物表
    sql = '''
            CREATE TABLE IF NOT EXISTS tb_syb_keyword_search_data
(
dtstatdate INT,
skeyword STRING,
ssearchsource STRING,
searchresult INT,
search_cnt BIGINT
)
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_keyword_search_data WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##各种来源的关键词检索数据
    sql = ''' 
    INSERT TABLE tb_syb_keyword_search_data
        SELECT
        %s AS dtstatdate,
        skeyword,
        searchsource,
        searchresult,
        COUNT(*) AS search_cnt
        FROM 
        (
        SELECT 
        vv7 AS suin,
        lower(vv8) AS skeyword,
        vv9 AS searchsource,
        vv10 AS searchresult
        FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0
        WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23' 
        AND ireportid = 800 
        AND iactiontype = 24 
        AND iactionid = 58 
        )t 
        GROUP BY
        skeyword,
        searchsource,
        searchresult
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##创建结果出库的表
    sql = '''
   CREATE TABLE IF NOT EXISTS tb_syb_keyword_search_data_db
(
dtstatdate INT,
skeyword STRING,
ssearchsource STRING,
searchresult INT,
search_cnt BIGINT,
data_flag INT
) 
   '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = ''' delete from tb_syb_keyword_search_data_db where dtstatdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
   
   
   
    ##日结果数据写入
    sql = '''
          INSERT TABLE tb_syb_keyword_search_data_db
      SELECT 
      dtstatdate ,
      skeyword ,
      ssearchsource ,
      searchresult ,
      search_cnt ,
      0 AS data_flag
      FROM tb_syb_keyword_search_data WHERE dtstatdate = %s AND skeyword != '' AND skeyword IS NOT NULL
      ORDER BY search_cnt DESC LIMIT 150 
    '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##周输入写入
    sql = '''         INSERT TABLE tb_syb_keyword_search_data_db
        SELECT 
        %s AS dtstatdate,
        skeyword ,
        ssearchsource ,
        searchresult ,
        search_cnt,
        1 AS data_flag
        FROM
        (
        SELECT 
        skeyword ,
        ssearchsource ,
        searchresult ,
        SUM(search_cnt) AS search_cnt 
        FROM
        (
        SELECT 
        skeyword ,
        ssearchsource ,
        searchresult ,
        search_cnt 
        FROM tb_syb_keyword_search_data WHERE dtstatdate <= %s AND dtstatdate>%s AND skeyword != '' AND skeyword IS NOT NULL
        )t
        GROUP BY skeyword ,
        ssearchsource ,
        searchresult 
        )t1
        ORDER BY search_cnt DESC LIMIT 150  '''%(sDate,sDate,pre_7_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##月数据写入
    sql = '''
        INSERT TABLE tb_syb_keyword_search_data_db
        SELECT 
        %s AS dtstatdate,
        skeyword ,
        ssearchsource ,
        searchresult ,
        search_cnt,
        2 AS data_flag
        FROM
        (
        SELECT 
        skeyword ,
        ssearchsource ,
        searchresult ,
        SUM(search_cnt) AS search_cnt 
        FROM
        (
        SELECT 
        skeyword ,
        ssearchsource ,
        searchresult ,
        search_cnt 
        FROM tb_syb_keyword_search_data WHERE dtstatdate <= %s AND dtstatdate>%s AND skeyword != '' AND skeyword IS NOT NULL
        )t
        GROUP BY skeyword ,
        ssearchsource ,
        searchresult 
        )t1
        ORDER BY search_cnt DESC LIMIT 150
    '''%(sDate,sDate,pre_30_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    