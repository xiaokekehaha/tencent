#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qt_oss_lol_DAU.py
# 功能描述:     QT-LOL独立端统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     tbQt_lol_Account  
# 数据源表:     ieg_tdbank::qtalk_dsl_enterroom_v2_fht0 ieg_tdbank::qtalk_dsl_leaveroom_v2_fht0 
# 创建人名:     leavesqin
# 创建日期:     2014-08-22
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
    
    sql="""delete from tbQt_lol_Account where dtstatdate=%s""" % (sDate_from)
    res = tdw.execute(sql)
    
    sql = """
            insert table tbQt_lol_Account  
            select 
            %s as dtStatDate,
             b.iuin 
            from 
            (
            select 
            distinct iuin 
            from 
            (
            select distinct iuin  
            from ieg_tdbank::qtalk_dsl_enterroom_v2_fht0 
            where tdbank_imp_date>=%s00 and tdbank_imp_date<=%s23 and isourcetype=536023592  
            union all 
            select distinct iuin  
            from ieg_tdbank::qtalk_dsl_leaveroom_v2_fht0 
            where tdbank_imp_date>=%s00 and tdbank_imp_date<=%s23 and isourcetype=536023592   
            )a
            )b 
            left join 
            (
            select 
            distinct iuin 
            from 
            tbQt_lol_Account 
            where 
            dtStatDate<%s
            ) c 
            on b.iuin=c.iuin 
            where c.iuin is null 
            group by b.iuin 
                    """ % (sDate_from, sDate_from,sDate_from, sDate_from, sDate_from,sDate_from)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
                    
    sql="""delete from tbQt_oss_lol_DauAndReg where dtstatdate=%s""" % (sDate_from)
    res = tdw.execute(sql)      
    
    sql="""
        insert table tbQt_oss_lol_DauAndReg   
        select 
        %s dtstatdate,
        iLoLDauNum,
        iLoLRegNum 
        from 
        (
        select 
        %s dtstatdate,
        count(distinct iuin) iLoLDauNum 
        from 
        (
        select distinct iuin  
        from ieg_tdbank::qtalk_dsl_enterroom_v2_fht0 
        where tdbank_imp_date>=%s00 and tdbank_imp_date<=%s23 and isourcetype=536023592  
        union all 
        select distinct iuin  
        from ieg_tdbank::qtalk_dsl_leaveroom_v2_fht0 
        where tdbank_imp_date>=%s00 and tdbank_imp_date<=%s23 and isourcetype=536023592   
        )a 
        ) b 
        left join 
        (
        select 
        %s dtstatdate,
        count(distinct iuin) iLoLRegNum 
        from tbQt_lol_Account 
        where 
        dtstatdate=%s 
        ) c 
        on b.dtstatdate=c.dtstatdate 
        """ % (sDate_from,sDate_from,sDate_from,sDate_from,sDate_from,sDate_from,sDate_from,sDate_from)
    res = tdw.execute(sql)             

    sql = """set hive.inputfiles.splitbylinenum=false"""
    res = tdw.execute(sql)
    
    
    tdw.WriteLog("== end OK ==")