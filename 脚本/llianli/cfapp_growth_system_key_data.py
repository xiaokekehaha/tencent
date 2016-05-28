#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cfapp_growth_system_key_data.py
# 功能描述:     掌火成长体系关键数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_cfapp_grouwth_system_key_data
# 数据源表:     ieg_tdbank::qtalk_dsl_CModifyUserScore_fht0   ieg_tdbank::qtalk_dsl_CDispatchGift2User_fht0
# 创建人名:     llianli
# 创建日期:     2016-05-13
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
            CREATE TABLE IF NOT EXISTS tb_cfapp_grouwth_system_key_data
        (
        dtstatdate INT COMMENT '统计日期',
        sign_uin_num BIGINT COMMENT '总签到用户数',
        gain_score_uin_num BIGINT COMMENT '每日获取积分用户数',
        gain_score BIGINT COMMENT '每日获取积分总数',
        update_times BIGINT COMMENT '每日升级次数',
        update_uin_num BIGINT COMMENT '每日升级用户数'
        )

"""
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""delete from tb_cfapp_grouwth_system_key_data where dtstatdate=%s  """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    sql = """
            INSERT  TABLE tb_cfapp_grouwth_system_key_data
            SELECT
            %s AS dtstatdate,
            t1.sign_uin_num AS sign_uin_num,
            t1.gain_score_uin_num AS gain_score_uin_num,
            t1.gain_score AS gain_score,
            t2.update_times AS update_times,
            t2.update_uin_num AS update_uin_num
            FROM
            (
            SELECT
            COUNT(DISTINCT sign_uin) AS sign_uin_num,
            COUNT(DISTINCT gain_score_uin) AS gain_score_uin_num,
            SUM(gain_score) AS gain_score
            FROM
            (
            SELECT 
            CASE WHEN task_id = 2 THEN iuin ELSE NULL END AS sign_uin,
            iuin AS gain_score_uin,
            score AS gain_score
            FROM ieg_tdbank::qtalk_dsl_CModifyUserScore_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
            )t  
            )t1
            JOIN
            (
            SELECT
            SUM(gift_id_num) AS update_times,
            COUNT(DISTINCT iuin) AS update_uin_num
            FROM ieg_tdbank::qtalk_dsl_CDispatchGift2User_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
            )t2 
             

            
            """ % (sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    