#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_cal.py
# 功能描述:     手游宝直播数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-08-13
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
    partition_date = today_date + datetime.timedelta(days = 1)
    partition_date_str = partition_date.strftime("%Y%m%d")
    
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


     ##创建表，直播的用户数据
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_app_zhibo_round_tbuseract
     (
     dtstatdate BIGINT,
     ilogintype BIGINT,
     iplatform BIGINT,
     liveroomtype STRING,
     roomid BIGINT,
     isybid BIGINT,
     itimes BIGINT,
     ionlinetime BIGINT,
     sdataflat STRING
     )PARTITION BY RANGE (dtstatdate)
            (
            partition p_20150801  VALUES LESS THAN (20150802),
            partition p_20150802  VALUES LESS THAN (20150803),
            partition p_20150803  VALUES LESS THAN (20150804),
            partition p_20150804  VALUES LESS THAN (20150805),
            partition p_20150805  VALUES LESS THAN (20150806)
            ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_app_zhibo_round_tbuseract DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table iplat_fat_syb_app_zhibo_round_tbuseract ADD PARTITION p_%s VALUES LESS THAN (%s) '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##从原始日志中写入直播的用户数据
    sql = ''' 
    INSERT TABLE iplat_fat_syb_app_zhibo_round_tbuseract
     SELECT
     %s AS dtstatdate,
     ilogintype,
     iplatform,
     liveroomtype,
     roomid,
     isybid ,
     times,
     ionlinetime,
     sdataflag
     FROM 
     (
     SELECT 
     CAST(-100 AS BIGINT)   AS ilogintype,
     CAST(-100 AS BIGINT)   AS iplatform,
     liveroomtype,
     roomid,
     isybid ,
     SUM(times) as times,
     SUM(ionlinetime) as ionlinetime,
     'dau' AS sdataflag 
     FROM 
     (
     SELECT 
     isybid,
     ilogintype,
     iplatform,
     vv3  AS liveroomtype ,
     0  AS ionlinetime,
     1  AS times ,
     vv2 as roomid 
     FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE  tdbank_imp_date >= %s00 AND tdbank_imp_date <= %s23 AND iactiontype = 10 AND iactionid in (49)
     )t
     GROUP BY isybid,roomid,liveroomtype

     
    UNION ALL
    

     SELECT 
     CASE WHEN GROUPING(ilogintype) = 1 THEN CAST(-100 as BIGINT) ELSE ilogintype END AS ilogintype,
     CASE WHEN GROUPING(iplatform) = 1 THEN CAST(-100 as BIGINT) ELSE iplatform END AS iplatform,
     CASE WHEN GROUPING(liveroomtype) = 1 THEN '-100' ELSE liveroomtype END AS liveroomtype,
     '-100' AS roomid,
     isybid ,
     SUM(times) as times,
     SUM(ionlinetime) as ionlinetime, 
     'dau' AS sdataflag
     FROM 
     (
     SELECT 
     isybid,
     ilogintype,
     iplatform,
     vv3   AS liveroomtype ,
     0  AS ionlinetime,
     1 AS times 
     FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE  tdbank_imp_date >= %s00 AND tdbank_imp_date <= %s23 AND iactiontype = 10 AND iactionid in (49)
     )t
     GROUP BY isybid,CUBE(ilogintype,iplatform,liveroomtype)
     )t1
    '''%(sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##从原始日志中写入直播的用户使用时长数据
    sql = ''' 
     INSERT TABLE iplat_fat_syb_app_zhibo_round_tbuseract
     SELECT
     %s AS dtstatdate,
     ilogintype,
     iplatform,
     liveroomtype,
     roomid,
     isybid ,
     times,
     ionlinetime,
     sdataflag
     FROM 
     (
     SELECT 
     CAST(-100 AS BIGINT)   AS ilogintype,
     CAST(-100 AS BIGINT)   AS iplatform,
     liveroomtype,
     roomid,
     isybid ,
     SUM(times) as times,
     SUM(ionlinetime) as ionlinetime,
     'time' AS sdataflag 
     FROM 
     (
     SELECT 
     isybid,
     ilogintype,
     iplatform,
     '-100'  AS liveroomtype ,
     CAST(unix_timestamp(vv4) AS BIGINT) - CAST(unix_timestamp(vv3) AS BIGINT)  AS ionlinetime,
     1  AS times ,
     vv2 as roomid 
     FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE  tdbank_imp_date >= %s00 AND tdbank_imp_date <= %s23 AND iactiontype = 10 AND iactionid in (48)
     )t
     GROUP BY isybid,roomid,liveroomtype

     
    UNION ALL
    

     SELECT 
     CASE WHEN GROUPING(ilogintype) = 1 THEN CAST(-100 as BIGINT) ELSE ilogintype END AS ilogintype,
     CASE WHEN GROUPING(iplatform) = 1 THEN CAST(-100 as BIGINT) ELSE iplatform END AS iplatform,
     CASE WHEN GROUPING(liveroomtype) = 1 THEN '-100' ELSE liveroomtype END AS liveroomtype,
     '-100' AS roomid,
     isybid ,
     SUM(times) as times,
     SUM(ionlinetime) as ionlinetime, 
     'time' AS sdataflag
     FROM 
     (
     SELECT 
     isybid,
     ilogintype,
     iplatform,
     '-100'   AS liveroomtype ,
     CAST(unix_timestamp(vv4) AS BIGINT) - CAST(unix_timestamp(vv3) AS BIGINT)   AS ionlinetime,
     1 AS times 
     FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE  tdbank_imp_date >= %s00 AND tdbank_imp_date <= %s23 AND iactiontype = 10 AND iactionid in (48)
     )t
     GROUP BY isybid,CUBE(ilogintype,iplatform,liveroomtype)
     )t1
    '''%(sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##创建用户的活跃账户表，每天存储全量数据
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_app_zhibo_round_tbAccount
     (
     dtstatdate BIGINT,
     ilogintype BIGINT,
     iplatform BIGINT,
     liveroomtype STRING,
     roomid STRING,
     isybid BIGINT,
     itimes BIGINT,
     ionlinetime BIGINT,
     iRegDate BIGINT,
     iLastActDate BIGINT,
     sdataflag STRING
     )PARTITION BY RANGE (dtstatdate)
            (
            partition p_20150801  VALUES LESS THAN (20150802),
            partition p_20150802  VALUES LESS THAN (20150803),
            partition p_20150803  VALUES LESS THAN (20150804),
            partition p_20150804  VALUES LESS THAN (20150805),
            partition p_20150805  VALUES LESS THAN (20150806)
            ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_app_zhibo_round_tbAccount DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table iplat_fat_syb_app_zhibo_round_tbAccount ADD PARTITION p_%s VALUES LESS THAN (%s) '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##这里用要创建临时表，主要目的是保证注册日期的正确性
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_app_zhibo_round_tbAccount_tmp_%s
     (
     dtstatdate BIGINT,
     ilogintype BIGINT,
     iplatform BIGINT,
     liveroomtype STRING,
     roomid STRING,
     isybid BIGINT,
     itimes BIGINT,
     ionlinetime BIGINT,
     iRegDate BIGINT,
     iLastActDate BIGINT,
     sdataflag STRING
     ) '''%(sDate)
    tdw.WriteLog(sql)       
    res = tdw.execute(sql)
    
    
    sql = ''' DELETE FROM iplat_fat_syb_app_zhibo_round_tbAccount_tmp_%s'''%(sDate)
    tdw.WriteLog(sql)       
    res = tdw.execute(sql)
    
    
    ##现将数据写入这个临时表中
    sql = """
    insert table  iplat_fat_syb_app_zhibo_round_tbAccount_tmp_%s
             
        select
        %s dtstatdate ,
        ilogintype,
        iplatform,
        liveroomtype,
        roomid,
        isybid,
        sum(iTimes) as iTimes,
        sum(iOnlineTime) as iOnlineTime ,
        min(iRegDate) as iRegDate,
        max(iLastActDate) as iLastActDate,
        sdataflag
        from
        (
        select
        ilogintype,
        iplatform,
        liveroomtype,
        roomid,
        isybid,
        iTimes,
        iOnlineTime,
        CAST(%s AS BIGINT) as iRegDate,
        CAST(%s AS BIGINT) as iLastActDate,
        sdataflat as sdataflag
        from iplat_fat_syb_app_zhibo_round_tbUserAct PARTITION(p_%s) t 
        
        union all
        
        select
        ilogintype,
        iplatform,
        liveroomtype,
        roomid,
        isybid,
        iTimes,
        iOnlineTime,
        iRegDate,
        iLastActDate,
        sdataflag
        from iplat_fat_syb_app_zhibo_round_tbAccount where dtstatdate=%s
        ) group by
        ilogintype,
        iplatform,
        liveroomtype,
        roomid,
        isybid,sdataflag
    """%(sDate,sDate,sDate,sDate,sDate,pre_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##讲临时数据表里面的结果写入如下表格中
    sql = """
    insert table  iplat_fat_syb_app_zhibo_round_tbAccount
             
        select
        %s dtstatdate ,
        t.ilogintype,
        t.iplatform,
        t.liveroomtype,
        t.roomid,
        t.isybid,
        t.iTimes,
        t.iOnlineTime ,
        CASE WHEN t1.iRegDate is NOT NULL AND t.iRegDate > t1.iRegDate THEN 0 ELSE t.iRegDate END AS iRegDate,
        t.iLastActDate,
        t.sdataflag
        from
        (      
        select
        ilogintype,
        iplatform ,
        liveroomtype ,
        roomid,
        isybid,
        itimes ,
        ionlinetime ,
        iRegDate ,
        iLastActDate,
        sdataflag
        from iplat_fat_syb_app_zhibo_round_tbAccount_tmp_%s 
        )t
        left outer join 
        (
        select 
        isybid,
        min(iRegDate)  as iRegDate
        from iplat_fat_syb_app_zhibo_round_tbAccount_tmp_%s
        where  ilogintype = -100 
        and iplatform = -100 
        and liveroomtype = '-100' 
        and roomid = '-100'
        group by isybid
        )t1
        on (t.isybid = t1.isybid)
         
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    #@运算完成之后将临时表删除
    sql = ''' drop table iplat_fat_syb_app_zhibo_round_tbAccount_tmp_%s'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
   

    
    ##创建活跃新增表
    sql = """
    CREATE TABLE  IF NOT EXISTS iplat_fat_syb_app_zhibo_tbuseracti
        (
         dtstatedate INT,
         ilogintype INT,
         iplatform INT,
         sliveroomtype STRING,
         sroomgid STRING,
         reg_uin BIGINT ,
         act_uin BIGINT,
         itimes BIGINT,
         ionlinetime BIGINT,
         sdataflag STRING
        )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    DELETE FROM  iplat_fat_syb_app_zhibo_tbuseracti  WHERE dtstatedate=%s
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
     INSERT TABLE iplat_fat_syb_app_zhibo_tbuseracti
       SELECT 
       %s AS dtstatdate,
       t1.ilogintype AS ilogintype,
       t1.iplatform AS iplatform,
       t1.liveroomtype AS liveroomtype,
       t1.roomid AS roomid,
       t1.reg_uin AS reg_uin,
       t1.act_uin AS act_uin,
       t1.iTimes - t2.iTimes as iusetimes,
       CAST(0 AS BIGINT) AS ionlinetime,
       'dau' AS sdataflag
       FROM
        (
         SELECT 
         ilogintype,
         iplatform,
         liveroomtype,
         roomid,
         COUNT(DISTINCT reg_uin) as reg_uin,
         COUNT(DISTINCT act_uin) as act_uin,
         SUM(iTimes) as iTimes
         FROM 
         (
         SELECT 
         ilogintype,
         iplatform,
         liveroomtype ,
         roomid,
         CASE WHEN iRegDate = %s THEN isybid ELSE NULL END AS reg_uin,
         CASE WHEN iLastActDate = %s THEN isybid ELSE NULL END AS act_uin,
         iTimes
         FROM iplat_fat_syb_app_zhibo_round_tbAccount WHERE dtstatdate=%s AND sdataflag = 'dau'
         )t GROUP BY ilogintype,iplatform, liveroomtype,roomid
        )t1 
         
         LEFT OUTER JOIN 
        ( 
         
         SELECT 
         ilogintype,
         iplatform,
         liveroomtype ,
         roomid,
         SUM(iTimes)  AS iTimes
         FROM iplat_fat_syb_app_zhibo_round_tbAccount WHERE dtstatdate=%s AND iLastActDate = %s AND sdataflag = 'dau'
         GROUP BY ilogintype,iplatform, liveroomtype,roomid
        )t2
        ON(t1.ilogintype = t2.ilogintype AND t1.iplatform = t2.iplatform AND t1.liveroomtype = t2.liveroomtype AND t1.roomid = t2.roomid)

    """%(sDate,sDate,sDate,sDate,sDate,pre_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##使用时长数据统计
    sql = """
     INSERT TABLE iplat_fat_syb_app_zhibo_tbuseracti
        SELECT 
        %s AS dtstatdate,
        ilogintype,
        iplatform,
        liveroomtype,
        roomid,
        CAST(0 as BIGINT) AS reg_uin,
        COUNT(DISTINCT isybid) AS act_uin,
        CAST(0 as BIGINT) as itimes,
        SUM(ionlinetime) as ionlinetime,
        sdataflat
        FROM iplat_fat_syb_app_zhibo_round_tbuseract WHERE dtstatdate = %s AND sdataflat = 'time' GROUP BY ilogintype,
        iplatform,
        liveroomtype,
        roomid,sdataflat
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##日注册用户留存跟踪数据
    sql = """
            CREATE TABLE  IF NOT EXISTS iplat_fat_syb_app_zhibo_tb_stayscale
        (
         dtstatedate INT,
         dtactday INT,
         daydelta INT,
         sflag STRING,
         ilogintype INT,
         iplatform INT,
         liveroomtype INT,
         istayuin BIGINT
        ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
            DELETE FROM  iplat_fat_syb_app_zhibo_tb_stayscale WHERE dtstatedate = %s 
            """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##将注册留存的数据写入
    week_ago_date = today_date - datetime.timedelta(days = 7)
    week_ago_date_str = week_ago_date.strftime("%Y%m%d")
    
    sql = '''
    INSERT TABLE iplat_fat_syb_app_zhibo_tb_stayscale
      
      SELECT 
      %s as dtstatdate,
      t.iRegDate as iRegDate,
      datediff(%s,t.iRegDate) as daydelta,
      'reg',
      t.ilogintype,
      t.iplatform,
      t.liveroomtype ,
      COUNT(DISTINCT t.isybid) 
      FROM 
      (
      SELECT 
      ilogintype,
      iplatform,
      liveroomtype ,
      isybid ,
      iRegDate
      FROM iplat_fat_syb_app_zhibo_round_tbAccount WHERE  dtstatdate = %s AND iRegDate >= %s AND iRegDate <= %s AND roomid = '-100' AND sdataflag = 'dau'
      )t 
      JOIN
      (
      SELECT 
      ilogintype,
      iplatform,
      liveroomtype, 
      isybid 
      FROM iplat_fat_syb_app_zhibo_round_tbAccount WHERE  dtstatdate = %s AND  iLastActDate = %s  AND roomid = '-100' AND sdataflag = 'dau'
      )t1 
      on (t.ilogintype = t1.ilogintype AND t.iplatform = t1.iplatform AND t.liveroomtype = t1.liveroomtype AND t.isybid = t1.isybid  ) 
      GROUP BY t.iRegDate,t.ilogintype,t.iplatform,t.liveroomtype
    '''%(sDate,sDate,sDate,week_ago_date_str,pre_date_str,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
            
    
    ##将活跃留存的数据写入    
    sql = '''
     INSERT TABLE iplat_fat_syb_app_zhibo_tb_stayscale
        SELECT 
        %s as dtstatdate,
        t.actday as actday,
        datediff(%s,t.actday) as daydelta,
        'act',
        t.ilogintype,
        t.iplatform,
        t.liveroomtype ,
        COUNT(DISTINCT t.isybid) 
        FROM 
        (
        SELECT 
        ilogintype,
        iplatform,
        liveroomtype ,
        isybid ,
        dtstatdate as actday
        FROM iplat_fat_syb_app_zhibo_round_tbUserAct WHERE dtstatdate >= %s AND dtstatdate <= %s AND roomid = '-100' AND sdataflat = 'dau'
        )t 
        JOIN
        (
        SELECT 
        ilogintype,
        iplatform,
        liveroomtype ,
        isybid 
        FROM iplat_fat_syb_app_zhibo_round_tbAccount WHERE  dtstatdate = %s AND  iLastActDate = %s AND roomid = '-100' AND sdataflag = 'dau'
        )t1 
        on (t.ilogintype = t1.ilogintype AND t.iplatform = t1.iplatform AND t.liveroomtype = t1.liveroomtype AND t.isybid = t1.isybid  ) 
        GROUP BY t.actday,t.ilogintype,t.iplatform,t.liveroomtype
    '''%(sDate,sDate,week_ago_date_str,pre_date_str,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    tdw.WriteLog("== end OK ==")
    
    
    
    