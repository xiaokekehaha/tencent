#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_hongren_xiaowo_click.py
# 功能描述:     手游宝APP红人小窝数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     teg_mta_intf :: ieg_shouyoubao 
# 创建人名:     llianli
# 创建日期:     2015-12-01
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
          CREATE TABLE IF NOT EXISTS tb_syb_app_hongren_xiaowo_click
    (
    dtstatdate BIGINT COMMENT '统计日期',
    id BIGINT COMMENT 'appid',
    ei STRING COMMENT '事件ID',
    pv BIGINT COMMENT '点击PV',
    uv BIGINT COMMENT '点击UV'
    )
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_syb_app_hongren_xiaowo_click WHERE  dtstatdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##写入小窝数据
    sql = ''' 
    INSERT TABLE tb_syb_app_hongren_xiaowo_click
      SELECT
      %s AS dtstatdate,
      id,
      ei,
      COUNT(*) AS pv,
      COUNT(DISTINCT uin_info) AS uv
      FROM
      (
      SELECT
      id,
      concat(ui,mc) AS uin_info,
      CASE 
          WHEN (id =1200679337 AND ei = 'MGC_CELEBRITY_DETAIL_VC_LIKE_CLICKED' ) OR (id =1100679302 AND ei = 'STAR_PRIVATE_LIKE') THEN 'zan' 
          WHEN (id =1200679337 AND ei = 'MGC_CELEBRITY_DETAIL_VC_REPLY_CLICKED' ) OR (id =1100679302 AND ei = 'STAR_PRIVATE_COMMENT') THEN 'comment'
          ELSE NULL
      END AS ei
      FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s
      )t
      WHERE ei IS NOT NULL 
      GROUP BY id,ei
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    


    tdw.WriteLog("== end OK ==")
    
    
    
    