'''
Created on 2014-12-24

@author: jakegong
'''

# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];


    tdw.WriteLog("== sDate = " + sDate + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    
    #create result table
    sql = """
            CREATE TABLE IF NOT EXISTS qt_syb_act_pvi_dnu
            (
            sDate string,
            business int, 
            dnu bigint
            )
            """
    print sql
    res = tdw.execute(sql)    
 
    sql = """
            delete from qt_syb_act_pvi_dnu where sDate=%s
            """%(sDate)
    print sql
    res = tdw.execute(sql)   
 
    sql = """
            insert table qt_syb_act_pvi_dnu
            select "%s", business, result
            from
            (
                select t.business, count(distinct pvi) as result
                from
                (
                    select  t1.business,  t1.pvi
                    from
                    (
                    select  distinct  business, pvi
                    from qt_syb_act_pvi
                    where sDate = "%s"
                    ) t1
                    left outer join
                    (
                    select distinct business, pvi
                    from qt_syb_act_pvi
                    where sDate < "%s"                        
                    ) t2
                    on (t1.business = t2.business and t1.pvi = t2.pvi)
                    where t2.pvi is null
                ) t  
                group by business   
            ) t
            """ %(sDate, sDate, sDate)
    print sql
    res = tdw.execute(sql)       
    

    
    