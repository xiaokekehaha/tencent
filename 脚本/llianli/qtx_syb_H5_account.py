#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_H5_account.py
# 功能描述:     手游宝H5数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_login_bill_fht0 
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
    partition_date = today_date + datetime.timedelta(days = 0)
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


     ##创建表，H5
    sql = '''
          CREATE TABLE IF NOT EXISTS  iplat_fat_syb_new_round_tbuseract
         (
         dtstatdate BIGINT COMMENT '统计时间',
         suin STRING COMMENT '账户信息',
         iappid BIGINT COMMENT 'APP形态，1：APP，0：手游宝id',
         iplatform BIGINT COMMENT '平台类型，8：H5',
         ilogintype BIGINT COMMENT '登录账号类型',
         sappver STRING COMMENT '版本号',
         sreguin STRING COMMENT '是否是大盘新进用户,如果是大盘新增用户，该字段为对应UIN，否则为空',
         itimes BIGINT COMMENT '登录次数',
         ionlinetime BIGINT COMMENT '使用时长'
         )PARTITION BY LIST (dtstatdate)
                     (
                     PARTITION p_20150817  VALUES IN (20150817),
                     PARTITION p_20150818  VALUES IN (20150818),
                     PARTITION p_20150819  VALUES IN (20150819),
                     PARTITION p_20150820  VALUES IN (20150820)
                     )
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_new_round_tbuseract DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = '''  ALTER  TABLE iplat_fat_syb_new_round_tbuseract ADD  PARTITION p_%s VALUES IN (%s) '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##从原始日志中写入H5的用户数据
    sql = ''' 
    INSERT TABLE iplat_fat_syb_new_round_tbuseract
        
        SELECT
        %s as dtstatdate,
        suin,
        iappid,
        case when grouping(iplatform)=1 then -100 else iplatform end as iplatform,
        case when grouping(ilogintype)=1 then -100 else ilogintype end as ilogintype,
        case when grouping(sappver)=1 then '-100' else sappver end as sappver,
        sreguin,
        0 as itimes,
        0 as ionlinetime
        from
        (
        SELECT
        CAST(isybid as STRING) AS suin,
        iappid AS iappid,
        iplatform AS iplatform,
        ilogintype  AS ilogintype,
        vappver AS sappver,
        CASE WHEN ik1 = 1 THEN CAST(isybid AS STRING) ELSE NULL END AS sreguin
        FROM  ieg_tdbank::gqq_dsl_day_login_bill_fht0
        where tdbank_imp_date between %s00 and %s23 and iappid = 0 
        )
        group by suin,sreguin,iappid,cube(iplatform,ilogintype,sappver)
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    
    ##创建用户的活跃账户表，每天存储全量数据
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_new_round_tbaccount
        (
        dtstatdate BIGINT COMMENT '统计时间',
        suin STRING COMMENT '账户信息',
        iappid BIGINT COMMENT 'APP形态，1：APP，0：手游宝id',
        iplatform BIGINT COMMENT '平台类型，8：H5',
        ilogintype BIGINT COMMENT '登录账号类型',
        sappver STRING COMMENT '版本号',
        sreguin STRING COMMENT '是否是大盘新进用户,如果是大盘新增用户，该字段为对应UIN，否则为空',
        iregdate BIGINT COMMENT '注册日期',
        ilastactdate BIGINT COMMENT '最后活跃日期',
        sdayacti STRING COMMENT '最近60天每天活跃情况',
        sweekacti STRING COMMENT '最近60天每周活跃情况',
        smonthacti STRING COMMENT '最近60天每月活跃情况',
        itimes BIGINT COMMENT '活跃总次数',
        ionlinetime BIGINT COMMENT '使用时长'
        )PARTITION BY LIST (dtstatdate)
                    (
                    PARTITION p_20150817  VALUES IN (20150817),
                    PARTITION p_20150818  VALUES IN (20150818),
                    PARTITION p_20150819  VALUES IN (20150819),
                    PARTITION p_20150820  VALUES IN (20150820)
                    ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_new_round_tbaccount DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table iplat_fat_syb_new_round_tbaccount ADD PARTITION p_%s VALUES IN  (%s) '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_new_round_tbaccount_tmp_%s
        (
        dtstatdate BIGINT COMMENT '统计时间',
        suin STRING COMMENT '账户信息',
        iappid BIGINT COMMENT 'APP形态，1：APP，0：手游宝id',
        iplatform BIGINT COMMENT '平台类型，8：H5',
        ilogintype BIGINT COMMENT '登录账号类型',
        sappver STRING COMMENT '版本号',
        sreguin STRING COMMENT '是否是大盘新进用户,如果是大盘新增用户，该字段为对应UIN，否则为空',
        iregdate BIGINT COMMENT '注册日期',
        ilastactdate BIGINT COMMENT '最后活跃日期',
        sdayacti STRING COMMENT '最近60天每天活跃情况',
        sweekacti STRING COMMENT '最近60天每周活跃情况',
        smonthacti STRING COMMENT '最近60天每月活跃情况',
        itimes BIGINT COMMENT '活跃总次数',
        ionlinetime BIGINT COMMENT '使用时长'
        ) '''%(sDate)
    tdw.WriteLog(sql)      
    res = tdw.execute(sql)
    
    
    sql = ''' DELETE FROM iplat_fat_syb_new_round_tbaccount_tmp_%s 
    ''' %(sDate)
    tdw.WriteLog(sql)      
    res = tdw.execute(sql)
    
    ##写入临时表
    sql = """
    INSERT TABLE  iplat_fat_syb_new_round_tbaccount_tmp_%s
        
        select
        %s dtstatdate,
        suin,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        sreguin,
        min(iRegDate) as iRegDate,
        max(iLastActDate) as iLastActDate,
        case 
           when ( sum(todayact)=1 or sum(todayact)=3 ) 
           then substr(
                      max(sDayActi),
                      case when 
                           length(max(sDayActi))>=90 then 2 else 0 end 
                       ) || '1' 
           else substr(
                       max(sDayActi),
                       case when 
                              length(max(sDayActi))>=90 then 2 else 0 end
                              ) || '0'
        end as sDayActi,
        max(sWeekActi) as sWeekActi,
        max(sMonthActi) as sMonthActi,
        sum(iTimes) as iTimes,
        sum(iOnlineTime) as iOnlineTime 
        from
        (
        SELECT 
        suin,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        sreguin,
        %s AS iregdate,
        %s AS ilastactdate,
        '' as sDayActi,
        '' as sWeekActi,
        '' as sMonthActi,
        sum(iTimes) as iTimes,
        sum(iOnlineTime) as iOnlineTime,
        1 as todayact
        from iplat_fat_syb_new_round_tbUserAct PARTITION(p_%s) t
        group by
        suin,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        suin,
        sreguin
        
        UNION ALL
        
        SELECT
        suin,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        sreguin,
        iRegDate,
        iLastActDate,
        sDayActi,
        sWeekActi,
        sMonthActi,
        iTimes,
        iOnlineTime,
        2 as todayact
        from iplat_fat_syb_new_round_tbAccount where dtstatdate=%s
        ) group by
        suin,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        sreguin
    """%(sDate,sDate,sDate,sDate,sDate,pre_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
   

    ##将数据写入结果表中
    sql = """ 
    INSERT TABLE iplat_fat_syb_new_round_tbaccount 
        SELECT 
        %s as dtstatdate,
        t.suin,
        t.iappid,
        t.iplatform,
        t.ilogintype,
        t.sappver,
        t.sreguin,
        CASE WHEN t1.iRegDate is not null and t.iRegDate > t1.iRegDate THEN 0 ELSE t.iRegDate END as iRegDate,
        t.ilastactdate,
        t.sdayacti,
        t.sweekacti,
        t.smonthacti,
        t.itimes,
        t.ionlinetime
        FROM 
        (
        SELECT 
        suin,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        sreguin,
        iregdate,
        ilastactdate,
        sdayacti,
        sweekacti,
        smonthacti,
        itimes,
        ionlinetime
        FROM iplat_fat_syb_new_round_tbaccount_tmp_%s 
        )t 
        LEFT OUTER JOIN
        (
        SELECT 
        suin,
        iappid,
        MIN(iregdate) as iregdate 
        FROM iplat_fat_syb_new_round_tbaccount_tmp_%s WHERE iplatform = -100 AND ilogintype = -100 AND sappver = '-100'
        GROUP BY suin,iappid
        )t1
        ON (t.suin = t1.suin AND t.iappid = t1.iappid)
        
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##将临时表删除
    sql = """ DROP TABLE iplat_fat_syb_new_round_tbaccount_tmp_%s""" %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##创建活跃新增表
    sql = """
    CREATE TABLE IF NOT EXISTS iplat_fat_syb_new_tbuseracti
        (
        dtstatdate BIGINT COMMENT '统计时间',
        iappid BIGINT COMMENT 'APP形态，1：APP，0：手游宝id',
        iplatform BIGINT COMMENT '平台类型，8：H5',
        ilogintype BIGINT COMMENT '登录账号类型',
        sappver STRING COMMENT '版本号',
        iactuin BIGINT COMMENT '总活跃用户',
        ireguin BIGINT COMMENT '总新增用户',
        iappreguin BIGINT COMMENT 'app纯新增用户',
        itimes BIGINT COMMENT '活跃总次数',
        ionlinetime BIGINT COMMENT '使用时长'
        )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    DELETE FROM  iplat_fat_syb_new_tbuseracti  WHERE dtstatdate=%s
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##写入新增活跃数据
    sql = """
      INSERT TABLE iplat_fat_syb_new_tbuseracti
        SELECT 
        %s AS dtstatdate,
        iappid,
        iplatform,
        ilogintype,
        sappver,
        COUNT(DISTINCT act_uin) AS iactuin,
        COUNT(DISTINCT reg_uin) AS ireguin,
        COUNT(DISTINCT sappreguin) AS iappreguin,
        SUM(itimes) AS itimes,
        SUM(ionlinetime) as ionlinetime 
        FROM 
        (
        SELECT 
        iappid,
        iplatform,
        ilogintype,
        sappver,
        CASE WHEN ilastactdate = %s THEN suin ELSE NULL END AS act_uin,
        CASE WHEN iregdate = %s THEN suin ELSE NULL END AS reg_uin,
        CASE WHEN ilastactdate = %s THEN sreguin ELSE  NULL END AS sappreguin,
        CASE WHEN ilastactdate = %s THEN itimes ELSE 0 END AS itimes,
        CASE WHEN ilastactdate = %s THEN ionlinetime ELSE 0 END AS ionlinetime
        FROM iplat_fat_syb_new_round_tbaccount PARTITION (p_%s) t
        )t1 GROUP BY iappid,iplatform,ilogintype,sappver
    """%(sDate,sDate,sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    
    
    
    ##留存跟踪数据
    sql = """
            CREATE TABLE  IF NOT EXISTS iplat_fat_syb_new_tbstayscale
                (
                 dtstatedate INT COMMENT '计算日期',
                 dtactday INT COMMENT '活跃日期',
                 daydelta INT COMMENT '时间范围',
                 iappid BIGINT COMMENT 'APP形态，1：APP，0：手游宝id',
                 iplatform BIGINT COMMENT '平台类型，8：H5',
                 ilogintype BIGINT COMMENT '登录账号类型',
                 sappver STRING COMMENT '版本号',
                 sflag STRING COMMENT '活跃或者新增的标志',
                 istayuin BIGINT COMMENT '留存用户'
                ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
            DELETE FROM  iplat_fat_syb_new_tbstayscale WHERE dtstatedate = %s 
            """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    week_ago_date = today_date - datetime.timedelta(days = 15)
    week_ago_date_str = week_ago_date.strftime("%Y%m%d")
    
    
    ##将活跃留存的数据写入
    sql = '''
    INSERT TABLE iplat_fat_syb_new_tbstayscale
        SELECT 
        %s AS dtstatdate,
        t1.actday AS actday,
        datediff(%s,t1.actday) as daydelta,
        t1.iappid as iappid,
        t1.iplatform as iplatform,
        t1.ilogintype as ilogintype,
        t1.sappver as sappver,
        'act' AS sflag,
        COUNT(DISTINCT t1.suin) as istayuin
        FROM
        (
        SELECT 
        iappid,
        iplatform,
        ilogintype,
        sappver,
        suin
        FROM iplat_fat_syb_new_round_tbaccount WHERE dtstatdate = %s AND ilastactdate = %s
        )t 
        JOIN 
        (
        SELECT
        iappid,
        iplatform,
        ilogintype,
        sappver,
        suin,
        dtstatdate as actday
        FROM iplat_fat_syb_new_round_tbuseract WHERE dtstatdate >= %s AND dtstatdate  < %s
        )t1
        on (t.iappid = t1.iappid AND t.iplatform = t1.iplatform AND t.ilogintype = t1.ilogintype AND t.sappver = t1.sappver AND t.suin = t1.suin)
        GROUP BY t1.actday,t1.iappid,
        t1.iplatform,
        t1.ilogintype,
        t1.sappver
    '''%(sDate,sDate,sDate,sDate,week_ago_date_str,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
            
    
    ##将注册留存的数据写入    
    sql = '''
     INSERT TABLE iplat_fat_syb_new_tbstayscale
        SELECT 
        %s AS dtstatdate,
        t1.actday AS actday,
        datediff(%s,t1.actday) as daydelta,
        t1.iappid as iappid,
        t1.iplatform as iplatform,
        t1.ilogintype as ilogintype,
        t1.sappver as sappver,
        'reg' AS sflag,
        COUNT(DISTINCT t1.suin) as istayuin
        FROM
        (
        SELECT 
        iappid,
        iplatform,
        ilogintype,
        sappver,
        suin
        FROM iplat_fat_syb_new_round_tbaccount WHERE dtstatdate = %s AND ilastactdate = %s
        )t 
        JOIN 
        (
        SELECT
        iappid,
        iplatform,
        ilogintype,
        sappver,
        suin,
        iregdate as actday
        FROM iplat_fat_syb_new_round_tbaccount WHERE dtstatdate = %s AND iregdate >= %s AND iregdate  < %s
        )t1
        on (t.iappid = t1.iappid AND t.iplatform = t1.iplatform AND t.ilogintype = t1.ilogintype AND t.sappver = t1.sappver AND t.suin = t1.suin)
        GROUP BY t1.actday,t1.iappid,
        t1.iplatform,
        t1.ilogintype,
        t1.sappver
    '''%(sDate,sDate,sDate,sDate,sDate,week_ago_date_str,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    tdw.WriteLog("== end OK ==")
    
    
    
    