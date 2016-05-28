#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_bbs_url_srouce.py
# 功能描述:     手游宝论坛具体URL来源数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     dw_ta::ta_log_bbs_g_qq_com 
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
    
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，论坛详细ULR的来源
    sql = '''
               CREATE TABLE IF NOT EXISTS tb_syb_app_bbs_url_source
               (
               dtstatdate INT COMMENT '统计日期',
               surl STRING COMMENT 'URL',
               ssource STRING COMMENT '来源信息',
               pv BIGINT COMMENT '对应来源的PV',
               uv BIGINT COMMENT '对应来源的UV'
               ) 
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_app_bbs_url_source WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##数据写入
    sql = ''' 
    INSERT TABLE tb_syb_app_bbs_url_source
  SELECT
  %s AS dtstatdate,
  url,
  CASE WHEN GROUPING(adt) = 1 THEN '-100' ELSE adt END AS adt,
  COUNT(*) AS pv,
  COUNT(DISTINCT pvi) AS uv 
  FROM 
 ( 
 SELECT
 url, 
 CASE 
     WHEN adt = 'cf.gw.rk' THEN '官网'
     WHEN adt = 'cf.wx.rk' THEN '微信公众号'
     WHEN adt = 'cf.dylt.rk' THEN '端游论坛'
     WHEN adt = 'cf.zh.rk' THEN '掌火'
     WHEN adt = 'cf.zq.rk' THEN '专区'
     WHEN adt = '-' THEN '直接进入'
     ELSE adt
  END AS adt,
  pvi 
  FROM dw_ta::ta_log_bbs_g_qq_com WHERE  daytime = %s AND dm = 'BBS.G.QQ.COM' AND url = '/forum-56951-1-1-1.html'
  )t 
  GROUP BY url,cube(adt)
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
   

    tdw.WriteLog("== end OK ==")
    
    
    
    