#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cfapp_growth_system_sign_data.py
# 功能描述:     掌火成长体系签到数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_cfapp_grouwth_system_sign_data
# 数据源表:     ieg_tdbank::qtalk_dsl_CModifyUserScore_fht0   ieg_tdbank::qtalk_dsl_CSetReceiveSigninAlertFlag_fht0 
# 创建人名:     llianli
# 创建日期:     2016-05-17
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS tb_cfapp_grouwth_system_sign_data
            (
            dtstatdate INT COMMENT '统计日期',
            sign_uin_cnt BIGINT COMMENT '签到用户数',
            accumlate_sign_uin_cnt BIGINT COMMENT '累计签到用户数',
            cancle_sign_remind_uin_cnt BIGINT COMMENT '取消签到提醒用户数'
            )

"""
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""delete from tb_cfapp_grouwth_system_sign_data where dtstatdate=%s  """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    sql = """
            INSERT TABLE tb_cfapp_grouwth_system_sign_data
            SELECT
            %s AS dtstatdate,
            t2.sign_uin_cnt AS sign_uin_cnt,
            t2.accumlate_sign_uin_cnt AS accumlate_sign_uin_cnt,
            t1.cancle_sign_remind_uin_cnt AS cancle_sign_remind_uin_cnt
            FROM 
            (
            SELECT
            COUNT(DISTINCT sign_uin) AS sign_uin_cnt,
            COUNT(DISTINCT accumlate_sign_uin) AS accumlate_sign_uin_cnt
            FROM 
            (
            SELECT
            CASE WHEN task_id = 2 THEN iuin ELSE NULL END AS sign_uin,
            CASE WHEN task_id = 7 THEN iuin ELSE NULL END AS accumlate_sign_uin
            FROM ieg_tdbank::qtalk_dsl_CModifyUserScore_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
            )t
            )t2
            JOIN
            (
            SELECT
            COUNT(DISTINCT iuin) AS cancle_sign_remind_uin_cnt
            FROM  ieg_tdbank::qtalk_dsl_CSetReceiveSigninAlertFlag_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23' AND signin_flag = 1 
            )t1         
            """ % (sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
