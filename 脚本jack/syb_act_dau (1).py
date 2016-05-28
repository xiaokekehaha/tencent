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
    sql = """use  ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """
            CREATE TABLE IF NOT EXISTS qt_syb_act_pvi
            (
            sDate string,
            business int,
            pvi bigint
            )
            PARTITION BY LIST(sDate)
            (
                PARTITION default
            )
            """
    print sql
    res = tdw.execute(sql)
    
    sql = """
            alter table  qt_syb_act_pvi drop partition (p_%s) 
          """ %(sDate)
    print sql
    res = tdw.execute(sql)
  
    sql = """
            alter table  qt_syb_act_pvi add partition p_%s values in ("%s")
          """ %(sDate, sDate)
    print sql
    res = tdw.execute(sql)
    
    sql = """
            insert table qt_syb_act_pvi
            select sDate,  business, f_pvid
            from
            (
            select distinct %s as sDate, 1 as business, f_pvid from teg_dw_tcss::tcss_agame_qq_com  where f_url  like  '%%/act/dnw20150610/%%' and f_date=%s
            union all 
            select distinct %s as sDate, 2 as business, f_pvid from teg_dw_tcss::tcss_agame_qq_com  where f_url  like  '%%/act/qjnn/20150624/%%' and f_date=%s
            ) t 
            """ %(sDate, sDate, sDate, sDate)
    
    print sql 
    res = tdw.execute(sql)    

     
    
