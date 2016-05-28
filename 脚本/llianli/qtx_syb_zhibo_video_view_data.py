#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_video_view_data.py
# 功能描述:     手游宝直播各个房间用户观看总数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-10-21
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
    today_str_2 = today_date.strftime("%Y-%m-%d")
    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    
    
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


     ##创建表，手游宝直播各个房间用户观看总数据
    sql = '''
            CREATE TABLE IF NOT EXISTS tb_syb_room_pv_uv_day
   (
   dtstatdate INT,
   imainroomid INT,
   isubroomid INT,
   pv BIGINT,
   uv BIGINT 
   ) 
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM tb_syb_room_pv_uv_day WHERE dtstatdate = %s ''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##统计手游宝直播各个房间用户观看数据
    sql = ''' 
    INSERT TABLE tb_syb_room_pv_uv_day
    
   SELECT 
   %s AS dtstatdate,
   imainroomid,
   isubroomid,
   COUNT(*) AS pv,
   COUNT(DISTINCT userid) AS total_pv
   FROM 
   (
   SELECT
   rootid AS imainroomid,
   roomid AS isubroomid,
   userid,
   dteventtime 
   FROM 
   ieg_tdbank::qtalk_dsl_videoroomevent_fht0 
   WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
   AND room_mode = 19 
   AND event_type = 1 
   AND unix_timestamp(enter_time) >= unix_timestamp('%s 00:00:00')
   AND unix_timestamp(enter_time) <= unix_timestamp('%s 23:59:59')
   )t1 
   GROUP BY imainroomid,  isubroomid
    '''%(sDate,sDate,sDate,today_str_2,today_str_2)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    