#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 2016-04-06

@author: llianli
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

    ##-----先将表中游点赞的数据删掉，然后继续
    sql = """
            DELETE FROM  tb_gpcd_active_user PARTITION  (p_%s) a WHERE dtstatdate = %s AND business  LIKE 'YDZ%%'""" %(sDate,sDate)
    res = tdw.execute(sql)
    



    ##--导入活跃表全体数据
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT  
        DISTINCT isybid, 
        'YDZ-ALL',
        %s
        FROM ieg_tdbank::gqq_dsl_gamebible_day_task_bill_fht0 
        where tdbank_imp_date between '%s00' and '%s23' and iactiontype in (1,2)
        """ % (sDate,sDate,sDate)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)  
    
    
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT  
        DISTINCT isybid, 
        'YDZ-android',
        %s
        FROM ieg_tdbank::gqq_dsl_gamebible_day_task_bill_fht0 
        where tdbank_imp_date between '%s00' and '%s23' and iactiontype in (1,2)  AND lower(vostype) = 'android'    
        """ % (sDate,sDate,sDate)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)   
    
    
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT  
        DISTINCT isybid, 
        'YDZ-ios',
        %s
        FROM ieg_tdbank::gqq_dsl_gamebible_day_task_bill_fht0 
        where tdbank_imp_date between '%s00' and '%s23' and iactiontype in (1,2)  AND lower(vostype) = 'ios'    
        """ % (sDate,sDate,sDate)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)   


  

    tdw.WriteLog("== end OK ==")
    
    