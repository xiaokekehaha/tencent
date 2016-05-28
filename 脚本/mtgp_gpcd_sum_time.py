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
            CREATE TABLE IF NOT EXISTS tb_gpcd_sum_time
            (
            sdate BIGINT COMMENT '日期',
            prefecture_name STRING COMMENT '专区名字',
            sum_time INT COMMENT '访问时长'
            ) """
    res = tdw.execute(sql)
    
    sql=""" DELETE FROM tb_gpcd_sum_time where sdate=%s """ %(sDate)
    res = tdw.execute(sql)

    ##--整体时长
    sql="""
        INSERT TABLE tb_gpcd_sum_time
        SELECT 
        m.sdate as sdate,
        'mtgp_prefecture_time_all',
        sum(m.du) as total_time 
        FROM 
        (SELECT
        sdate,
        du,
        pi
        FROM 
        ieg_qqtalk_mtgp_app::mtgp_pi_usernum_times_time 
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND period_type= 'day')m 
        JOIN 
        (SELECT /*+ MAPJOIN(tgp_pi_list) */   
        pi, title                       
        FROM ieg_qqtalk_mtgp_app::tgp_pi_list )n 
        ON m.pi = n.pi 
        GROUP BY m.sdate     
        """ %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== mtgp_prefecture_time_all end OK ==")
    
    ##--LOL专区整体时长
    sql="""
        INSERT TABLE tb_gpcd_sum_time
        SELECT 
        m.sdate as sdate,
        'mtgp_prefecture_time_lol',
        sum(m.du) as total_time 
        FROM 
        (SELECT
        sdate,
        du,
        pi
        FROM 
        ieg_qqtalk_mtgp_app::mtgp_pi_usernum_times_time 
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND period_type= 'day')m 
        JOIN 
        (SELECT /*+ MAPJOIN(tgp_pi_list) */   
        pi, title                       
        FROM ieg_qqtalk_mtgp_app::tgp_pi_list 
        WHERE (title like '666_%%') 
        OR ( title like '战绩_%%')
        OR ( title like '一起玩_%%'))n 
        ON m.pi = n.pi 
        GROUP BY m.sdate     
        """ %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== mtgp_prefecture_time_lol end OK ==")
    
    
    ##--DNF专区整体时长
    sql="""
        INSERT TABLE tb_gpcd_sum_time
        SELECT 
        m.sdate as sdate,
        'mtgp_prefecture_time_dnf',
        sum(m.du) as total_time 
        FROM 
        (SELECT
        sdate,
        du,
        pi
        FROM 
        ieg_qqtalk_mtgp_app::mtgp_pi_usernum_times_time
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND period_type= 'day')m 
        JOIN 
        (SELECT /*+ MAPJOIN(tgp_pi_list) */   
        pi, title                       
        FROM ieg_qqtalk_mtgp_app::tgp_pi_list 
        WHERE (title like 'DNF_%%'))n 
        ON m.pi = n.pi 
        GROUP BY m.sdate     
        """ %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== mtgp_prefecture_time_dnf end OK ==")
    
    
    ##--CF专区整体时长
    sql="""
        INSERT TABLE tb_gpcd_sum_time
        SELECT 
        m.sdate as sdate,
        'mtgp_prefecture_time_cf',
        sum(m.du) as total_time 
        FROM 
        (SELECT
        sdate,
        du,
        pi
        FROM 
        ieg_qqtalk_mtgp_app::mtgp_pi_usernum_times_time
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND period_type= 'day')m 
        JOIN 
        (SELECT /*+ MAPJOIN(tgp_pi_list) */   
        pi, title                       
        FROM ieg_qqtalk_mtgp_app::tgp_pi_list 
        WHERE (title like 'CF%%'))n 
        ON m.pi = n.pi 
        GROUP BY m.sdate     
        """ %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== mtgp_prefecture_time_cf end OK ==")