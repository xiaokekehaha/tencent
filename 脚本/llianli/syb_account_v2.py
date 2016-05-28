#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_account_v2.py
# 功能描述:     手游宝大盘数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_login_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-09-10
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


     ##创建表
    sql = '''
          CREATE TABLE IF NOT EXISTS  iplat_fat_syb_app_round_tbuseract_v2
         (
         dtstatdate BIGINT COMMENT '统计时间',
         saccounttype BIGINT COMMENT '账户类型微信或者qq',
         sappver STRING COMMENT '版本信息',
         splatform STRING COMMENT '平台类型，app或者h5',
         sgamecode STRING COMMENT '安卓还是ios',
         iloginflag BIGINT COMMENT '是否登陆',
         istartsource BIGINT COMMENT '启动来源', 
         istartsourceii BIGINT COMMENT '第三方启动来源',
         
         suin STRING COMMENT '账户信息',
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


    sql='''  alter table iplat_fat_syb_app_round_tbuseract_v2 drop partition (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = '''  alter table iplat_fat_syb_app_round_tbuseract_v2 add partition p_%s values in ('%s') '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##从原始日志中写入用户数据
    sql = ''' 
    INSERT TABLE iplat_fat_syb_app_round_tbuseract_v2
        
        SELECT
        
        %s as dtstatdate,
        saccounttype,
        case when grouping(sappver)=1 then '-100' else sappver end as sappver,
        case when grouping(splatform)=1 then '-100' else splatform end as splatform,
        case when grouping(sgamecode)=1 then '-100' else sgamecode end as sgamecode,
        iloginflag,
        case when grouping(istartsource)=1 then -100 else istartsource end as istartsource,
        case when grouping(istartsourceii)=1 then -100 else istartsourceii end as istartsourceii,
        suin,
        0 as itimes,
        0 as ionlinetime
        from
        
        (
        
        SELECT
        isybid as suin,
        '-100' as saccounttype,
        lower(vostype) as sgamecode,
        vappver as sappver,
        CASE WHEN iappid = 1 THEN 'app'  WHEN iappid = 0 AND iplatform = 8 THEN 'h5' ELSE 'unknow' END AS splatform,
        CASE WHEN isybid != 0 THEN 1 ELSE 0 END AS iloginflag,
        ik2 AS istartsource,
        ik3 AS istartsourceii 
        FROM  ieg_tdbank::gqq_dsl_day_login_bill_fht0
        where tdbank_imp_date between %s00 and %s23 and  vdevicetype not like '%%generic%%' AND (iappid = 1 OR (iappid = 0 and iplatform = 8))
        
        UNION ALL
        
        SELECT
        isybid as suin,
        case when ilogintype=1 then 'qq' when ilogintype=2 then 'wx' else 'unknow' end as saccounttype,
        lower(vostype) as sgamecode,
        vappver as sappver,
        CASE WHEN iappid = 1 THEN 'app'  WHEN iappid = 0 AND iplatform = 8 THEN 'h5' ELSE 'unknow' END AS splatform,
        CASE WHEN isybid != 0 THEN 1 ELSE 0 END AS iloginflag,
        ik2 AS istartsource,
        ik3 AS istartsourceii 
        FROM ieg_tdbank::gqq_dsl_day_login_bill_fht0
        where tdbank_imp_date between %s00 and %s23 and  vdevicetype not like '%%generic%%' AND (iappid = 1 OR (iappid = 0 and iplatform = 8))
        
        )
        group by saccounttype,suin,iloginflag,cube(sgamecode,sappver,splatform,istartsource,istartsourceii)
    '''%(sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    
    ##创建用户的活跃账户表，每天存储全量数据
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_app_round_tbaccount_v2
        (
         dtstatdate BIGINT COMMENT '统计时间',
         saccounttype BIGINT COMMENT '账户类型微信或者qq',
         sappver STRING COMMENT '版本信息',
         splatform STRING COMMENT '平台类型，app或者h5',
         sgamecode STRING COMMENT '安卓还是ios',
         iloginflag BIGINT COMMENT '是否登陆',
         istartsource BIGINT COMMENT '启动来源', 
         istartsourceii BIGINT COMMENT '第三方启动来源', 
         suin STRING COMMENT '账户信息',
         iregdate BIGINT COMMENT '注册日期',
             ilastactdate BIGINT COMMENT '最后活跃日期',
         
             sdayacti STRING COMMENT '最近90天每天活跃情况',
            sweekacti STRING COMMENT '最近90天周活跃情况',
            smonthacti STRING COMMENT '最近90月活跃情况',
         
         itimes BIGINT COMMENT '登录次数',
         ionlinetime BIGINT COMMENT '使用时长'
         )PARTITION BY LIST (dtstatdate)
                     (
                     PARTITION p_20150817  VALUES IN (20150817),
                     PARTITION p_20150818  VALUES IN (20150818),
                     PARTITION p_20150819  VALUES IN (20150819),
                     PARTITION p_20150820  VALUES IN (20150820)
                     ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_app_round_tbaccount_v2 DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table iplat_fat_syb_app_round_tbaccount_v2 ADD PARTITION p_%s VALUES IN  (%s) '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
            create external table if not exists iplat_fat_tmp_syb_app_tbAccount_v2_%s
          (
          dtstatdate BIGINT COMMENT '统计时间',
         saccounttype BIGINT COMMENT '账户类型微信或者qq',
         sappver STRING COMMENT '版本信息',
         splatform STRING COMMENT '平台类型，app或者h5',
         sgamecode STRING COMMENT '安卓还是ios',
         iloginflag BIGINT COMMENT '是否登陆',
         istartsource BIGINT COMMENT '启动来源', 
         istartsourceii BIGINT COMMENT '第三方启动来源', 
         suin STRING COMMENT '账户信息',
         
         iregdate BIGINT COMMENT '注册日期',
         ilastactdate BIGINT COMMENT '最后活跃日期',
         
         sdayacti STRING COMMENT '最近90天每天活跃情况',
        sweekacti STRING COMMENT '最近90天周活跃情况',
        smonthacti STRING COMMENT '最近90月活跃情况',
         
         itimes BIGINT COMMENT '登录次数',
         ionlinetime BIGINT COMMENT '使用时长')"""%(sDate)
    tdw.WriteLog(sql)      
    res = tdw.execute(sql)
    
    
    sql = ''' DELETE FROM iplat_fat_tmp_syb_app_tbAccount_v2_%s 
    ''' %(sDate)
    tdw.WriteLog(sql)      
    res = tdw.execute(sql)
    
    ##写入临时表
    sql = """
    insert table iplat_fat_tmp_syb_app_tbAccount_v2_%s
        select
        %s dtstatdate,
        sAccountType,
        sappver,
        splatform,
        sgamecode,
        iloginflag,
        istartsource,
        istartsourceii,
        suin,
        
        min(iRegDate) as iRegDate,
        
        max(iLastActDate) as iLastActDate,
        
        case when (sum(todayact)=1 or sum(todayact)=3) then substr(max(sDayActi),case when length(max(sDayActi))>=90 then 2 else 0 end) || '1' else substr(max(sDayActi),case when length(max(sDayActi))>=90 then 2 else 0 end) || '0' end as sDayActi,
        
        max(sWeekActi)
        
        as sWeekActi,
        
        max(sMonthActi)
        
        as sMonthActi,
        
        sum(iTimes) as iTimes,
        
        sum(iOnlineTime) as iOnlineTime from
        
        (
        
        select
        sAccountType,
        sappver,
        splatform,
        sgamecode,
        iloginflag,
        istartsource,
        istartsourceii,
        sUin,
        
        %s as iRegDate,
        %s as iLastActDate,
        '' as sDayActi,
        '' as sWeekActi,
        '' as sMonthActi,
        sum(iTimes) as iTimes,
        sum(iOnlineTime) as iOnlineTime,
        1 as todayact
        from iplat_fat_syb_app_round_tbUserAct_v2 PARTITION(p_%s) t
        
        group by
        sAccountType,
        sappver,
        splatform,
        sgamecode,
        iloginflag,
        istartsource,
        istartsourceii,
        suin
        
        union all
        
        select
        
        sAccountType,
        sappver,
        splatform,
        sgamecode,
        iloginflag,
        istartsource,
        istartsourceii,
        suin,
        
        iRegDate,
        iLastActDate,
        sDayActi,
        sWeekActi,
        sMonthActi,
        iTimes,
        iOnlineTime,
        2 as todayact
        from iplat_fat_syb_app_round_tbaccount_v2  where dtstatdate=%s
        ) group by
        sAccountType,
        sappver,
        splatform,
        sgamecode,
        iloginflag,
        istartsource,
        istartsourceii,
        suin
    """%(sDate,sDate,sDate,sDate,sDate,pre_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
   

    ##将数据写入结果表中
    sql = """ 
            INSERT TABLE iplat_fat_syb_app_round_tbaccount_v2
        SELECT
        %s as dtstatdate,
        t1.sAccountType,
        t1.sappver,
        t1.splatform,
        t1.sgamecode,
        t1.iloginflag,
        t1.istartsource,
        t1.istartsourceii,
        t1.suin,
        
        CASE WHEN t2.iRegDate is not null and t1.iRegDate > t2.iRegDate THEN 0 ELSE t1.iRegDate END as iRegDate,
        
        t1.iLastActDate,
        
        t1.sDayActi,
        
        t1.sWeekActi,
        
        t1.sMonthActi,
        
        t1.iTimes,
        
        t1.iOnlineTime
        
        FROM
        
        (
        
        select
        
        sAccountType,
        sappver,
        splatform,
        sgamecode,
        iloginflag,
        istartsource,
        istartsourceii,
        suin,
        
        iRegDate,
        iLastActDate,
        sDayActi,
        sWeekActi,
        sMonthActi,
        iTimes,
        iOnlineTime
        FROM iplat_fat_tmp_syb_app_tbAccount_v2_%s
        )t1
        LEFT JOIN
        (
        SELECT
        sUin,
        MIN(iRegDate) as iRegDate
        FROM iplat_fat_tmp_syb_app_tbAccount_v2_%s
        where sAccountType='-100' and sgamecode='-100' and sappver='-100' and splatform='-100' AND iloginflag = 1 AND istartsource = -100 AND istartsourceii = -100 
        GROUP BY sUin
        )t2
        ON t1.sUin=t2.sUin
        
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##将临时表删除
    sql = """ DROP TABLE iplat_fat_tmp_syb_app_tbAccount_v2_%s""" %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##创建活跃新增表
    sql = """
    CREATE TABLE IF NOT EXISTS iplat_fat_syb_tbuseracti_v2
        (
        dtstatdate BIGINT COMMENT '统计时间',
        saccounttype BIGINT COMMENT '账户类型微信或者qq',
         sappver STRING COMMENT '版本信息',
         splatform STRING COMMENT '平台类型，app或者h5',
         sgamecode STRING COMMENT '安卓还是ios',
         iloginflag BIGINT COMMENT '是否登陆',
         istartsource BIGINT COMMENT '启动来源', 
         istartsourceii BIGINT COMMENT '第三方启动来源',
        iactuin BIGINT COMMENT '总活跃用户',
        ireguin BIGINT COMMENT '总新增用户',
        itimes BIGINT COMMENT '活跃总次数',
        ionlinetime BIGINT COMMENT '使用时长'
        )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    DELETE FROM  iplat_fat_syb_tbuseracti_v2  WHERE dtstatdate=%s
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##写入新增活跃数据
    sql = """
      INSERT TABLE iplat_fat_syb_tbuseracti_v2
        SELECT 
        %s AS dtstatdate,
        saccounttype,
         sappver,
         splatform,
         sgamecode,
         iloginflag,
         istartsource, 
         istartsourceii,
        COUNT(DISTINCT act_uin) AS iactuin,
        COUNT(DISTINCT reg_uin) AS ireguin,
        SUM(itimes) AS itimes,
        SUM(ionlinetime) as ionlinetime 
        FROM 
        (
        SELECT 
        saccounttype,
         sappver,
         splatform,
         sgamecode,
         iloginflag,
         istartsource, 
         istartsourceii,
        CASE WHEN ilastactdate = %s THEN suin ELSE NULL END AS act_uin,
        CASE WHEN iregdate = %s THEN suin ELSE NULL END AS reg_uin,
        CASE WHEN ilastactdate = %s THEN itimes ELSE 0 END AS itimes,
        CASE WHEN ilastactdate = %s THEN ionlinetime ELSE 0 END AS ionlinetime
        FROM iplat_fat_syb_app_round_tbaccount_v2 PARTITION (p_%s) t
        )t1 GROUP BY saccounttype,
         sappver,
         splatform,
         sgamecode,
         iloginflag,
         istartsource, 
         istartsourceii
    """%(sDate,sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    