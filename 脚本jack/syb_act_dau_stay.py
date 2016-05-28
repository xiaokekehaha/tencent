'''
Created on 2014-12-24

@author: jakegong
'''

time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

def dateDelta(baseTime, delta):
    d1 = datetime.datetime(string.atoi(baseTime[0:4]), string.atoi(baseTime[4:6]), string.atoi(baseTime[6:8]))
    d2 = d1 + datetime.timedelta(days=delta)
    deltaDate = d2.strftime("%Y%m%d")
    return deltaDate

# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];

    tdw.WriteLog("== sDate = " + sDate + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    
    day1 = dateDelta(sDate, -1)
    day3 = dateDelta(sDate, -3)
    day7 = dateDelta(sDate, -7)
    
    print day1, day3, day7

    #create result table
    sql = """
            CREATE TABLE IF NOT EXISTS qt_syb_act_pvi_dau_stay
            (
            sDate string,
            business int, 
            day int,
            result bigint
            )
            """
    print sql
    res = tdw.execute(sql)    
 
    sql = """
            delete from qt_syb_act_pvi_dau_stay where sDate=%s
            """%(sDate)
    print sql
    res = tdw.execute(sql)   
 
    sql = """
            insert table qt_syb_act_pvi_dau_stay
            select "%s", business, day, result
            from
            (
                select t.business, t.day, count(distinct pvi) as result
                from
                (
                    select  t1.business, (t1.sDate - t2.sDate) as day, t1.pvi
                    from
                    (
                    select  distinct sDate, business, pvi
                    from qt_syb_act_pvi
                    where sDate = "%s"
                    ) t1
                    join
                    (
                    select distinct sDate, business, pvi
                    from qt_syb_act_pvi
                    where sDate in ("%s", "%s", "%s")
                    ) t2
                    on(t1.business = t2.business and t1.pvi = t2.pvi)
                ) t  
                group by business, day       
            ) t
            """ %(sDate, sDate, day1, day3, day7)
    print sql
    res = tdw.execute(sql)       
    

    
    