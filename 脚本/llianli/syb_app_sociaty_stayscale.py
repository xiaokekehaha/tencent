#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_sociaty_stayscale.py
# 功能描述:     手游宝炫斗公会留存率数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-11-03
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
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，计算留存用户的 原始表
    sql = '''
           CREATE TABLE IF NOT EXISTS tb_syb_app_ttxd_sociaty_useract
        (
        dtstatdate INT COMMENT '时间分区',
        id INT COMMENT 'APPID -100 为全体，11开头为IOS，12开头为安卓',
        sybid BIGINT COMMENT '用手游宝ID来计算留存情况'
        )PARTITION BY LIST (dtstatdate)
                    (
                    partition p_20151021  VALUES IN (20151021),
                    partition p_20151022  VALUES IN (20151022)
                    ) '''
    
    tdw.WriteLog(sql)        
    res = tdw.execute(sql)


    sql = ''' ALTER TABLE tb_syb_app_ttxd_sociaty_useract DROP PARTITION (p_%s)'''%(today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql=''' ALTER TABLE tb_syb_app_ttxd_sociaty_useract ADD PARTITION p_%s VALUES IN (%s)''' % (today_str,today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##原始数据写入
    sql = ''' 
    INSERT TABLE tb_syb_app_ttxd_sociaty_useract
        SELECT
        %s AS dtstatdate,
        CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
        sybid
        FROM 
        ( 
        SELECT 
        id,
        CASE WHEN id = 1100679302 THEN get_json_object(kv,'$.uin') ELSE get_json_object(kv,'$.syb_id') END AS sybid
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s AND 
         (
          ( ei = 'GENERAL_AD_CLICK' AND get_json_object(kv,'$.url') LIKE '%%native://sociatyhome?%%' AND get_json_object(kv,'$.game_id') = '102')  
                 OR ( ei = 'MGC_SOCIATY_ENTRANCE_CLICK' AND get_json_object(kv,'$.gameid') = '102' )
         )
        )t1
        GROUP BY sybid,cube(id)
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##留存率表建立
    
    sql = ''' 
          CREATE TABLE IF NOT EXISTS tb_syb_app_ttxd_sociaty_stayscale
           (
           dtstatdate INT COMMENT '统计日期',
           deltadays INT COMMENT '相隔日期',
           sdate INT COMMENT '回溯的日期',
           id INT COMMENT '应用id',
           act_uin INT COMMENT '活跃用户数',
           stay_uin_num INT COMMENT '留存用户数'
           )
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ delete from tb_syb_app_ttxd_sociaty_stayscale where dtstatdate = %s """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT TABLE tb_syb_app_ttxd_sociaty_stayscale

      SELECT
      %s AS dtstatdate,
      datediff(%s,t.dtstatdate) AS deladay,
      t.dtstatdate AS sdate,
      t.id AS id,
      t.act_uin AS act_uin,
      t2.istayuin AS istayuin
      FROM 
      (
      SELECT 
      dtstatdate,
      id,
      COUNT(DISTINCT sybid) AS act_uin
      FROM tb_syb_app_ttxd_sociaty_useract WHERE dtstatdate >= %s AND dtstatdate < %s
      GROUP BY dtstatdate,id
      )t
      JOIN
      
      (
      SELECT 
      t1.dtstatdate AS sdate,
      t.id AS id,
      COUNT(DISTINCT t.sybid) AS istayuin
      FROM 
      (
      SELECT
      id,
      sybid 
      FROM tb_syb_app_ttxd_sociaty_useract WHERE dtstatdate = %s 
      )t
      JOIN
      (
      SELECT 
      dtstatdate,
      id,
      sybid
      FROM tb_syb_app_ttxd_sociaty_useract WHERE dtstatdate >= %s AND dtstatdate < %s
      )t1
      ON (t1.id = t1.id AND t.sybid = t1.sybid)
      GROUP BY t1.dtstatdate,t.id
      
      )t2
      on (t.dtstatdate = t2.sdate AND t.id = t2.id ) 
    ''' %(sDate,sDate,pre_7_date_str,sDate,sDate,pre_7_date_str,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    