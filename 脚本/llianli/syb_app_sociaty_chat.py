#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_sociaty_chat.py
# 功能描述:     手游宝天天炫斗公会聊天数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-11-03
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
    today_str = today_date.strftime("%Y%m%d")

    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，聊天数据表
    sql = '''
     CREATE TABLE IF NOT EXISTS tb_syb_app_ttxd_sociaty_chat_msg_data
     (
     dtstatdate INT COMMENT '统计日期',
     client_type INT COMMENT '客户端类型，-100：全部',
     chat_room_cnt BIGINT COMMENT '有聊天信息的聊天室数目',
     msg_cnt BIGINT COMMENT '聊天消息数',
     uin_cnt BIGINT COMMENT '聊天UIN数'
     )
         '''
            
    res = tdw.execute(sql)


    sql='''  DELETE FROM tb_syb_app_ttxd_sociaty_chat_msg_data WHERE dtstatdate = %s  ''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##参与聊天总用户数统计
    sql = ''' 
    INSERT TABLE tb_syb_app_ttxd_sociaty_chat_msg_data
    
    SELECT 
    %s AS dtstatdate,
    CASE WHEN GROUPING(client_type) = 1 THEN -100 ELSE client_type END AS client_type ,
    COUNT(DISTINCT session_id ) AS chat_room_cnt,
    COUNT(*) AS msg_cnt,
    COUNT(DISTINCT uuid) AS uin_cnt
     FROM ieg_tdbank::qtalk_dsl_syb_sendchatmsg_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
     GROUP BY cube(client_type) 
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
   

    tdw.WriteLog("== end OK ==")
    
    
    
    