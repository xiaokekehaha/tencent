#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     mtgp_pi_record_perday.py
# 功能描述:      
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:       
# 数据源表:     teg_mta_intf::ieg_lol  
# 创建人名:     yaoyaopeng
# 创建日期:     2016-03-26
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module
  
import config_tgp_ei_pi
            
# main entry
def TDW_PL(tdw, argv=[]):
     
    
    sDate = argv[0];
    #sDate = '20160105'
    #mta_id = config.mta_id
    #game_dict = config.game_dict
    #sql = """drop table mtgp_pi_record_perday"""
    #tdw.execute(sql)
    
    sql = """use ieg_qqtalk_mtgp_app"""
    tdw.execute(sql)
    sql = '''
      CREATE TABLE IF NOT EXISTS mtgp_pi_record_perday
        (
        sdate INT,
        id BIGINT,
        pi STRING,
        uin_mac STRING,
        pv BIGINT,
        du BIGINT
        )
        partition by list(sdate)
        (partition default);
      '''
    tdw.WriteLog(sql)
    tdw.execute(sql)
    
    sql = '''ALTER TABLE mtgp_pi_record_perday DROP PARTITION (p_%s)'''%(sDate)
    tdw.WriteLog(sql)
    tdw.execute(sql)
     
    sql = """ALTER TABLE mtgp_pi_record_perday ADD PARTITION p_%s VALUES IN (%s)"""%(sDate,sDate)
    tdw.WriteLog(sql)
    tdw.execute(sql) 
    

     
    sql = ''' 
        INSERT TABLE mtgp_pi_record_perday
            SELECT 
            %s as sdate,
            t2.id,
            t2.pi,
            t2.uin_mac,
            t2.pv,
            t2.du
            FROM 
            (SELECT
            id,
            pi,
            uin_mac,
            COUNT(*) AS pv,
            SUM(du) AS du
            FROM
            (
            SELECT 
            id,
            pi,
            concat(ui,mc) AS uin_mac,
            du
            FROM  teg_mta_intf::ieg_tgp 
            WHERE sdate = %s  AND id in (%s) AND pi != '-' 
            )t
            GROUP BY id,pi,uin_mac)t2 
            WHERE t2.du <= 21600
    '''%(sDate, sDate, config_tgp_ei_pi.mta_id)
        
    tdw.WriteLog(sql)
    tdw.execute(sql)
   
    tdw.WriteLog("end OK ==")
    

    
    
    