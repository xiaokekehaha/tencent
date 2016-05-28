#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_anchor_video_stat.py
# 功能描述:     手游宝直播主播开播状态
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


     ##创建表，主播每段直播时长详情
    sql = '''
            CREATE TABLE IF NOT EXISTS tb_syb_anchor_video_stat
     (
     dtstatdate BIGINT,
     iuin BIGINT,
     imainroomid BIGINT,
     isubroomid BIGINT,
     ivideoid BIGINT,
     sstarttmie STRING,
     sendtime STRING,
     itotaltime BIGINT
     )
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_anchor_video_stat WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##统计主播每段开播时长数据
    sql = ''' 
    INSERT TABLE tb_syb_anchor_video_stat
    select 
    %s AS dtstatdate,
    uin,
    mainroomid,
    subroomid,
    videoid,
    from_unixtime(videoStartTime) AS sstarttmie,
    from_unixtime(videoEndTime) AS sendtime,
    videoEndTime - videoStartTime AS itotaltime
    from 
    (
    select 
    uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime,
    min(videoEndTime ) as videoEndTime
    from  ieg_tdbank::qtalk_dsl_anchoropenvideostat_fht0 
    where tdbank_imp_date BETWEEN '%s00' AND   '%s23' 
    AND videoEndTime != 0  AND videotype = 4 AND gameid = 10017 
    group by uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime    
    )t 
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = ''' 
    CREATE TABLE IF NOT EXISTS tb_syb_anchor_video_time_day
   (
   dtstatdate INT,
   iuin BIGINT,
   imainroomid BIGINT,
   isubroomid BIGINT,
   itotaltime BIGINT
   )
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' delete from tb_syb_anchor_video_time_day where dtstatdate = %s''' %(sDate)
    
    
    
    ##统计每个主播 每天开播的总时长
    sql = ''' 
     INSERT TABLE tb_syb_anchor_video_time_day
   SELECT 
   %s AS dtstatdate,
   uin,
   mainroomid,
   subroomid,
   SUM(ionlinetime) AS total_time
   FROM 
   (
   select 
    uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime,
    max_report_time,
    min_report_time,
    CASE 
        WHEN max_report_time = 0 AND min_report_time = 0 THEN unix_timestamp('%s 23:59:59') - videoStartTime 
        WHEN max_report_time != 0 AND min_report_time != 0 THEN min_report_time - unix_timestamp('%s 00:00:00')
        WHEN min_report_time = 0 AND max_report_time != 0 THEN max_report_time - videoStartTime
    END AS ionlinetime
    FROM 
        
   (
   SELECT 
    uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime,
    min(videoEndTime ) as min_report_time,
    max(videoEndTime ) as max_report_time
    from  
   (
   select 
    uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime,
    min(videoEndTime ) as videoEndTime
    from  ieg_tdbank::qtalk_dsl_anchoropenvideostat_fht0 
    where tdbank_imp_date BETWEEN '%s00' AND   '%s23' 
    AND  videotype = 4 AND gameid = 10017 
    AND videoEndTime != 0 
    AND unix_timestamp(dteventtime) >= unix_timestamp('%s 00:00:00') 
    AND unix_timestamp(dteventtime) <= unix_timestamp('%s 23:59:59')
    group by uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime  
    
    
    
    UNION ALL 
    
    
    select 
    uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime,
    videoEndTime
    from  ieg_tdbank::qtalk_dsl_anchoropenvideostat_fht0 
    where tdbank_imp_date BETWEEN '%s00' AND   '%s23' 
    AND  videotype = 4 AND gameid = 10017 
    AND videoEndTime = 0 
    AND unix_timestamp(dteventtime) >= unix_timestamp('%s 00:00:00') 
    AND unix_timestamp(dteventtime) <= unix_timestamp('%s 23:59:59')

   )tmp 
   group by uin ,
    mainroomid,
    subroomid,
    videoid,
    videoStartTime  
    
    )t
    )t1 
    GROUP BY uin,
   mainroomid,
   subroomid
    '''%(sDate,today_str_2,today_str_2,sDate,sDate,today_str_2,today_str_2,sDate,sDate,today_str_2,today_str_2)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    