#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_reg_account.py
# 功能描述:     掌盟掌火注册表维护
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_reg_account
# 数据源表:     ieg_qt_community_app.tb_app_original_data
# 创建人名:     llianli
# 创建日期:     2016-03-21
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ********************************q**********************************************


#import system module


# main entry
import datetime
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    
   
    pre_3_date = today_date - datetime.timedelta(days = 3)
    pre_3_date_str = pre_3_date.strftime("%Y%m%d")
    
    pre_7_date = today_date - datetime.timedelta(days = 7)
    pre_7_date_str = pre_7_date.strftime("%Y%m%d")
    
    pre_14_date = today_date - datetime.timedelta(days = 14)
    pre_14_date_str = pre_14_date.strftime("%Y%m%d")
    
    
    pre_30_date = today_date - datetime.timedelta(days = 30)
    pre_30_date_str = pre_30_date.strftime("%Y%m%d")
    
    pre_60_date = today_date - datetime.timedelta(days = 60)
    pre_60_date_str = pre_60_date.strftime("%Y%m%d")

    pre_90_date = today_date - datetime.timedelta(days = 90)
    pre_90_date_str = pre_90_date.strftime("%Y%m%d")


    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS tb_app_reg_account
            (
            iuin BIGINT COMMENT '用户UIN',
            iclienttype INT COMMENT '客户端类型，0 掌盟整体，1 掌火整体，9：掌盟安卓，10：掌盟IOS,15：掌火安卓，16：掌火IOS',
            iregdaate INT COMMENT '注册日期',
            ilastactdate INT COMMENT '最后一天活跃日期',
            cbitmap STRING COMMENT '用户活跃的位图情况',
            dtstatdate INT COMMENT '统计日期'
            )PARTITION BY LIST (dtstatdate)
                (
                 partition p_20160314  VALUES IN (20160314),
                 partition p_20160315  VALUES IN (20160315)
                ) """
    res = tdw.execute(sql)

    sql=""" ALTER TABLE tb_app_reg_account DROP PARTITION (p_%s)""" % (sDate)
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_app_reg_account ADD  PARTITION p_%s VALUES IN (%s) """ % (sDate,sDate)
    res = tdw.execute(sql)
    
    ##-----写入数据之前，先将每日数据写入一张临时表里面，求出掌盟，掌火各自的总数
    sql = """
            CREATE TABLE IF NOT EXISTS tb_app_reg_account_temp_%s
            (
            dtstatdate INT COMMENT '统计日期',
            iuin BIGINT COMMENT '用户UIN',
            iclienttype INT COMMENT '客户端类型，0 掌盟整体，1 掌火整体，9：掌盟安卓，10：掌盟IOS,15：掌火安卓，16：掌火IOS'
            )""" %(sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    ##-----15号的数据合并之后写入临时表
    sql = """
    INSERT OVERWRITE TABLE tb_app_reg_account_temp_%s

        SELECT 
        dtstatdate,
        iuin,
        iclienttype 
        FROM tb_app_original_data PARTITION (p_%s) a  WHERE dtstatdate = %s AND iclienttype IN (9,10,15,16)
        
        UNION ALL 
        
        SELECT
        DISTINCT
        dtstatdate,
        iuin,
        iclienttype
        FROM
        (
        SELECT 
        dtstatdate,
        iuin,
        CASE WHEN iclienttype IN (9,10) THEN 0 ELSE 1 END AS iclienttype 
        FROM tb_app_original_data PARTITION (p_%s) a WHERE dtstatdate = %s AND  iclienttype IN (9,10,15,16)
        )tmp 
    """%(sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##-----15号和14号的数据合并，写入总表中
    sql = """
    INSERT TABLE tb_app_reg_account
    SELECT 
    nvl(a.iuin,b.iuin) AS iuin,
    nvl(a.iclienttype,b.iclienttype) AS iclienttype,
    IF(b.iuin IS NOT NULL ,b.iregdate,%s) AS iregdate,
    IF(a.iuin IS NOT NULL ,%s,b.ilastactdate) AS ilastactdate,
    CASE 
        WHEN a.iuin IS NOT NULL AND b.iuin IS NULL      THEN RPAD('1',100,'0')
        WHEN a.iuin IS NULL     AND b.iuin IS NOT NULL  THEN CONCAT('0',substr(b.cbitmap,1,99))
        WHEN a.iuin IS NOT NULL AND b.iuin IS NOT NULL  THEN CONCAT('1',substr(b.cbitmap,1,99))
    END AS cbitmap,
    %s AS dtstatdate
    FROM 
    tb_app_reg_account_temp_%s a 
    FULL OUTER JOIN
    (
    SELECT
    dtstatdate,
    iuin,
    iclienttype,
    iregdate ,
    ilastactdate,
    cbitmap
    FROM
    tb_app_reg_account PARTITION (p_%s) a_1 WHERE  dtstatdate = %s
    )b
    ON(a.iuin = b.iuin AND a.iclienttype = b.iclienttype) 
    """%(sDate,sDate,sDate,sDate,pre_date_str,pre_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##将无用表删除
    sql = """ DROP TABLE tb_app_reg_account_temp_%s """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    tdw.WriteLog("== end OK ==")
    
    
    
    