'''
Created on 2014-12-24

@author: jakegong
'''



# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20141222'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use hy_dc_oss"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS qt_bbs_pvi
            (
            sdate int,
            cf_dau bigint,
            cf_dnu bigint,
            lol_dau bigint,
            lol_dnu bigint
            ) """
    res = tdw.execute(sql)

    sql="""delete from qt_bbs_pvi where sdate=%s""" % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert   table qt_bbs_pvi 
            select t1.dtstatdate, t1.cf_dau, t2.cf_dnu, t3.lol_dau, t4.lol_dnu
            from
            (
                select dtstatdate, icfactnum as cf_dau
                from qt_oss_game_bbs_act_cf
                where dtstatdate = %s
            ) t1
            join
            ( 
                select count(distinct iuin) as cf_dnu
                from qt_oss_game_bbs_reg_cf
                where dtstatdate = %s
            ) t2 
            join
            (
                select 
                ilolactnum as lol_dau
                from qt_oss_game_bbs_act_lol
                where dtstatdate = %s
            ) t3
            join
            ( 
                select count(distinct iuin) as lol_dnu
                from qt_oss_game_bbs_reg_lol
                where dtstatdate = %s
            ) t4  """ % (sDate, sDate, sDate, sDate)
            
    print "sql=%s"%(sql)
    tdw.WriteLog(sql)
    res = tdw.execute(sql) 
    
    tdw.WriteLog("== end OK ==")
     
    