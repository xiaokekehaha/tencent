#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 2014-12-24

@author: jakegong
'''

#import system module

time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")    
    
    pre_date = today_date - datetime.timedelta(days = 6)
    pre_6_date_str = pre_date.strftime("%Y%m%d")       

    tdw.WriteLog("== sDate = " + sDate + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qqtalk_mtgp_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)

    ##-----创建活跃表
    sql = """
            CREATE TABLE IF NOT EXISTS tb_gpcd_active_user
            (
            iuin STRING COMMENT '用户UIN',
            business STRING COMMENT '业务类型',
            dtstatdate INT COMMENT '统计日期'
            )PARTITION BY LIST (dtstatdate)
            (
                partition p_20160314  VALUES IN (20160314),
                partition p_20160315  VALUES IN (20160315)
            ) """
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)

    ##----创建注册表
    sql = """
            CREATE TABLE IF NOT EXISTS tb_gpcd_reg_account
            (
            iuin STRING COMMENT '用户UIN',
            business STRING COMMENT '业务类型',
            iregdate INT COMMENT '注册日期',
            ilastactdate INT COMMENT '最后一天活跃日期',
            cbitmap STRING COMMENT '用户活跃的位图情况',
            dtstatdate INT COMMENT '统计日期'
            )PARTITION BY LIST (dtstatdate)
            (
                partition p_20160314  VALUES IN (20160314),
                partition p_20160315  VALUES IN (20160315)
            )"""
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)

    sql=""" ALTER TABLE tb_gpcd_reg_account DROP PARTITION (p_%s)""" % (sDate)
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_gpcd_reg_account ADD PARTITION p_%s VALUES IN (%s) """ % (sDate,sDate)
    res = tdw.execute(sql)   


    ##-----将活跃和注册合并
    sql = """
    INSERT TABLE tb_gpcd_reg_account
    SELECT 
    nvl(a.iuin,b.iuin) AS iuin,
    nvl(a.business,b.business) AS business,
    IF(b.iuin IS NOT NULL ,b.iregdate,%s) AS iregdate,
    IF(a.iuin IS NOT NULL ,%s,b.ilastactdate) AS ilastactdate,
    CASE 
        WHEN a.iuin IS NOT NULL AND b.iuin IS NULL      THEN RPAD('1',366,'0')
        WHEN a.iuin IS NULL     AND b.iuin IS NOT NULL  THEN CONCAT('0',substr(b.cbitmap,1,365))
        WHEN a.iuin IS NOT NULL AND b.iuin IS NOT NULL  THEN CONCAT('1',substr(b.cbitmap,1,365))
    END AS cbitmap,
    %s AS dtstatdate
    FROM 
    (
    select distinct
    dtstatdate, 
    iuin,
    business
    from
    tb_gpcd_active_user PARTITION (p_%s) a_1
    where dtstatdate = %s
    ) a 
    FULL OUTER JOIN
    (
    SELECT
    dtstatdate,
    iuin,
    business,
    iregdate ,
    ilastactdate,
    cbitmap
    FROM
    tb_gpcd_reg_account WHERE  dtstatdate = %s
    )b
    ON(a.iuin = b.iuin AND a.business = b.business) 
    """%(sDate,sDate,sDate,sDate,sDate,pre_date_str)
    
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)  
    
 
    sql=""" ALTER TABLE tb_gpcd_active_user DROP PARTITION (p_%s)""" % (pre_6_date_str)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)   

    sql=""" ALTER TABLE tb_gpcd_reg_account DROP PARTITION (p_%s)""" % (pre_6_date_str)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("==end OK ==")
    
    