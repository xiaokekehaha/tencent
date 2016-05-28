#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     mtgp_pi_usernum_times_time.py
# 功能描述:      
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:       
# 数据源表:     mtgp_pi_record_perday  
# 创建人名:     yaoyaopeng
# 创建日期:     2016-03-26
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module
from datetime import date, timedelta    
import config_tgp_ei_pi
            
# main entry
def TDW_PL(tdw, argv=[]):
     
    tdw.WriteLog("=====  begin  =====")
    sDate = argv[0];
#    sDate = '20160217' 

    year = int(sDate[0:4])
    mon = int(sDate[4:6])
    day = int(sDate[6:8])
#    d = date(year, mon, day) - timedelta(1)
    #sLastDate = d.strftime('%Y%m%d')
    
    d = date(year, mon, day) - timedelta(6)
    sLastWeek = d.strftime('%Y%m%d')
    
    d = date(year, mon, day) - timedelta(13)
    sLastDWeek = d.strftime('%Y%m%d')
    
    d = date(year, mon, day) - timedelta(29)
    sLastMonth = d.strftime('%Y%m%d')
    
    d = date(year, mon, day) - timedelta(59)
#    sLastTwoMonth = d.strftime('%Y%m%d')
        
    sql = """use ieg_qqtalk_mtgp_app"""
    tdw.execute(sql)
    sql = '''
      CREATE TABLE IF NOT EXISTS mtgp_pi_usernum_times_time
        (
        sdate INT,
        id BIGINT,
        pi STRING,
        title STRING,
        usernum BIGINT comment '人数',
        times bigint comment '次数',
        du BIGINT comment '时长',
        period_type string comment 'day日  week周   double_week 双周  month月 '
        )
                      '''
    tdw.WriteLog(sql)
    tdw.execute(sql)
     
    sql = '''  
       DELETE FROM mtgp_pi_usernum_times_time WHERE  sdate = %s 
    '''%(sDate)
    
    tdw.WriteLog(sql)
    tdw.execute(sql)
    
 
    #天，人数
    tdw.WriteLog("====day======")
    sql = ''' 
        INSERT TABLE mtgp_pi_usernum_times_time
        SELECT 
            %s AS sdate,
            t2.id, 
            t2.pi, 
            t1.title, 
            t2.user_num, 
            t2.times,
            t2.du, 
            'day' as period_type
        FROM 
             ( SELECT /*+ MAPJOIN(tgp_pi_list) */   
                    pi, title                       
             FROM tgp_pi_list )t1
             JOIN 
            (SELECT
            id,
            pi,
            count(distinct(uin_mac)) AS user_num,
            sum(pv) as times,
            sum(du) AS du
            FROM  mtgp_pi_record_perday 
            WHERE sdate = %s  AND id in (%s) 
            GROUP BY id,pi)t2 
            ON t1.pi=t2.pi 
    '''%(sDate, sDate, config_tgp_ei_pi.mta_id)
        
    tdw.WriteLog(sql)
    tdw.execute(sql)
    tdw.WriteLog("====day OK ======")
   
   #周，人数
    tdw.WriteLog("====week======")
    sql = ''' 
        INSERT TABLE mtgp_pi_usernum_times_time
        SELECT 
            %s AS sdate,
            t2.id, 
            t2.pi, 
            t1.title, 
            t2.user_num, 
            t2.times,
            t2.du, 
            'week' as period_type
        FROM 
             ( SELECT /*+ MAPJOIN(tgp_pi_list) */   
                    pi, title                       
             FROM tgp_pi_list )t1
             JOIN 
            (SELECT
            id,
            pi,
            count(distinct(uin_mac)) AS user_num,
            sum(pv) as times,
            sum(du) AS du
            FROM  mtgp_pi_record_perday 
            WHERE sdate >= %s  AND sdate <= %s AND id in (%s) 
            GROUP BY id,pi)t2 
            ON t1.pi=t2.pi 
    '''%(sDate, sLastWeek, sDate,config_tgp_ei_pi.mta_id)
        
    tdw.WriteLog(sql)
    tdw.execute(sql)
    tdw.WriteLog("====week OK ======")
    
     #双周，人数
    tdw.WriteLog("====week======")
    sql = ''' 
        INSERT TABLE mtgp_pi_usernum_times_time
        SELECT 
            %s AS sdate,
            t2.id, 
            t2.pi, 
            t1.title, 
            t2.user_num, 
            t2.times,
            t2.du, 
            'dweek' as period_type
        FROM 
             ( SELECT /*+ MAPJOIN(tgp_pi_list) */   
                    pi, title                       
             FROM tgp_pi_list )t1
             JOIN 
            (SELECT
            id,
            pi,
            count(distinct(uin_mac)) AS user_num,
            sum(pv) as times,
            sum(du) AS du
            FROM  mtgp_pi_record_perday 
            WHERE sdate >= %s  AND sdate <= %s AND id in (%s) 
            GROUP BY id,pi)t2 
            ON t1.pi=t2.pi 
    '''%(sDate, sLastDWeek, sDate,config_tgp_ei_pi.mta_id)
        
    tdw.WriteLog(sql)
    tdw.execute(sql)
    tdw.WriteLog("====dweek OK ======")
    
     #月，人数
    tdw.WriteLog("====week======")
    sql = ''' 
        INSERT TABLE mtgp_pi_usernum_times_time
        SELECT 
            %s AS sdate,
            t2.id, 
            t2.pi, 
            t1.title, 
            t2.user_num, 
            t2.times,
            t2.du, 
            'month' as period_type
        FROM 
             ( SELECT /*+ MAPJOIN(tgp_pi_list) */   
                    pi, title                       
             FROM tgp_pi_list )t1
             JOIN 
            (SELECT
            id,
            pi,
            count(distinct(uin_mac)) AS user_num,
            sum(pv) as times,
            sum(du) AS du
            FROM  mtgp_pi_record_perday 
            WHERE sdate >= %s  AND sdate <= %s AND id in (%s) 
            GROUP BY id,pi)t2 
            ON t1.pi=t2.pi 
    '''%(sDate, sLastMonth, sDate,config_tgp_ei_pi.mta_id)
        
    tdw.WriteLog(sql)
    tdw.execute(sql)
    tdw.WriteLog("====month OK ======")
    
         
    tdw.WriteLog("end OK ==")
    

    
    
    