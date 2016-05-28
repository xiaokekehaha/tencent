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
    res = tdw.execute(sql)
    
    sql=""" ALTER TABLE tb_gpcd_active_user DROP PARTITION (p_%s)""" % (sDate)
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_gpcd_active_user ADD PARTITION p_%s VALUES IN (%s) """ % (sDate,sDate)
    res = tdw.execute(sql)        
   

    ##--导入活跃表全体数据
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT 
        distinct 
        iuin, 
        'MTGP_ALL',
        %s
        FROM ieg_tdbank::mtgp_dsl_mtgpreqstat_fht0 
        where tdbank_imp_date>=%d and tdbank_imp_date<=%d and iclienttype in (601, 602)       
        """ % (sDate,int(sDate)*100,int(sDate)*100 + 23)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== end OK ==")
    
    