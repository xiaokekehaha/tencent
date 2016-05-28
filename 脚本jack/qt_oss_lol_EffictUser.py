#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qt_oss_lol_EffictUser.py
# 功能描述:     QT-LOL直播有效用户统计（在线大于等于十分钟）
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     qt_tb_lol_EffictUserAccount qt_tb_lol_EffictUserDay qt_tb_lol_EffictUserReg  
# 数据源表:     ieg_tdbank::qtalk_dsl_leaveroom_v2_fht0 
# 创建人名:     leavesqin
# 创建日期:     2014-08-26
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module
time = __import__('time')
datetime= __import__('datetime')
string=__import__('string')


from ieg_oss_lib import *


# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")
    

    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")
    
    sDate_from = argv[0];
    sDate_to   = from_date(add_days(to_date(argv[0]), 1), 1)
    
    tdw.WriteLog("== sDate_from = " + sDate_from + " ==")
    tdw.WriteLog("== sDate_to = " + sDate_to + " ==")

    
    tdw.WriteLog("== connect tdw ==")
    sql = """use hy_dc_oss"""
    res = tdw.execute(sql)
    
    
    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)
    
    sql="""delete from qt_tb_lol_EffictUserAccount where dtstatdate=%s""" % (sDate_from)
    res = tdw.execute(sql)
    
    sql = """
            insert table qt_tb_lol_EffictUserAccount     
            select 
            %s as dtStatDate,
             b.iuin 
            from 
            (
            select 
            distinct iuin  
            FROM 
            ieg_tdbank::qtalk_dsl_leaveroom_v2_fht0 
            where 
            tdbank_imp_date between %s00 and %s23 
            and ionlinetime>=600 and iroomid1=94961178 
            )b 
            left join 
            (
            select 
            distinct iuin 
            from 
            qt_tb_lol_EffictUserAccount   
            where 
            dtStatDate<%s
            ) c 
            on b.iuin=c.iuin 
            where c.iuin is null 
            group by b.iuin 
                    """ % (sDate_from, sDate_from,sDate_from, sDate_from)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql="""delete from qt_tb_lol_EffictUserDay where dtstatdate=%s""" % (sDate_from)
    res = tdw.execute(sql)
    
    sql = """
            insert table qt_tb_lol_EffictUserDay   
            select 
            %s as dtStatDate, 
            count(distinct iuin) as iEffictUserDayNum  
            FROM 
            ieg_tdbank::qtalk_dsl_leaveroom_v2_fht0 
            where 
            tdbank_imp_date between %s00 and %s23 
            and ionlinetime>=600 and iroomid1=94961178 
                    """ % (sDate_from, sDate_from,sDate_from)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql="""delete from qt_tb_lol_EffictUserReg where dtstatdate=%s""" % (sDate_from)
    res = tdw.execute(sql)
    
    sql = """
            insert table qt_tb_lol_EffictUserReg    
            select 
            %s as dtStatDate, 
            count(distinct iuin) as iEffictUserRegNum   
            FROM 
            qt_tb_lol_EffictUserAccount  
            where dtstatdate=%s
                    """ % (sDate_from, sDate_from)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
                    

    sql = """set hive.inputfiles.splitbylinenum=false"""
    res = tdw.execute(sql)
    
    
    tdw.WriteLog("== end OK ==")