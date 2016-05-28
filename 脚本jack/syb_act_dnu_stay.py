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
    #sDate = '20150709'

    tdw.WriteLog("== sDate = " + sDate + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    
    day1 = dateDelta(sDate, -1)
    
    print day1

    #create result table
    sql = """
            CREATE TABLE IF NOT EXISTS qt_syb_act_pvi_dnu_stay
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
            delete from qt_syb_act_pvi_dnu_stay where sDate=%s
            """%(sDate)
    print sql
    res = tdw.execute(sql)   
 
    sql = """
            insert table qt_syb_act_pvi_dnu_stay
            select "%s", business, 1 , result
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
                    join
                    (
                    select distinct business, pvi
                    from qt_syb_act_pvi
                    where sDate = "%s"
                    ) t2
                    on(t1.business = t2.business and t1.pvi = t2.pvi)
                    left outer join
                    (
                    select distinct business, pvi
                    from qt_syb_act_pvi
                    where sDate < "%s"                        
                    ) t3
                    on (t1.business = t3.business and t1.pvi = t3.pvi)
                    where t3.pvi is null
                ) t  
                group by business   
            ) t
            """ %(sDate, sDate, day1, day1)
    print sql
    res = tdw.execute(sql)       
    

    
    