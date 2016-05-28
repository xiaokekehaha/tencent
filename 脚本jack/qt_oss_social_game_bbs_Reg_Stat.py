#!/usr/bin/env python
#-*- coding: utf-8 -*-
#  Descripe: 
#  Auther: leavesqin
#  CopyRight: Tencent Company
#  编写python SQL 规范:
#  0.
#    sql语句最后不要加分号
#    sql="alter table  T_FAT_QQGAME_GAME_BILL  drop partition (p_%s)" %(bgDt_tmp)
#  1.
#    execute SQL有异常，如果你在代码里捕捉了，调度是不会重新调度的。
#    你可以在捕捉代码里打印“FAILED”字符串，调度如果发现log里有FAILED，就会重新调度。
#    try:
#       res = tdw.execute(sql)
#       WriteLog("info","exec sql 1 ok")
#    except Exception, hsx:
#       WriteLog("error","sql 1 FAILED"+hsx.message) #hsx.message异常打印信息
#  2.
#    分区查询都用隐式查询  where  STATIS_DATE=20100908
#  3.
#    case when then else 语句要注意类型匹配，比如CHGHAPPYENERGY是bigint 因此 0 也要cast成bigint
#    case when CHGHAPPYENERGY<0 then CHGHAPPYENERGY else cast ( 0 as BIGINT) end
#    else 是bigint，then 也要是bigint
#    then cast (gametype+68 as bigint) else gametype 
#  4. 
#    建议使用drop partition 和 add partition ，而不使用truncate partition
# ------------------------------------------------------------------------------------------------

# need __import__ three python modules to support the datetime conversion
time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

# some global variables related to the shell script
bgDt = "00000000"
edDt = "00000000"
SepDay = 1
res = ""

# ------------------------------------------------------------------------------------------------
# new function to support `date -d "$bgDt_tmp 1 days" +%Y%m%d` in shell script
# 
def getYM(baseTime):   
    return str(baseTime)[0:6]   

def getYMFirstDay(baseTime):   
    return str(baseTime)[0:6] + "01"   
                    
def getYear():   
    return str(datetime.date.today())[0:4]
  
def getMonth():   
    return str(datetime.date.today())[5:7]
 
def getDay():   
    return str(datetime.date.today())[8:10]      

def getToday():   
    return str(getYear() + getMonth() + getDay())    
    
def dateDelta(baseTime, delta):
    # baseTime： is a string like "19700101"
    # delta：    1代表后一天 -1代表前一天
    d1 = datetime.datetime(string.atoi(baseTime[0:4]), string.atoi(baseTime[4:6]), string.atoi(baseTime[6:8]))
    d2 = d1 + datetime.timedelta(days=delta)
    deltaDate = d2.strftime("%Y%m%d")
    return deltaDate

# 返回指定日期周一和周日
# return the date(monday:weektype=1 or sunday weektype=7) from a giving date 
# baseDate： is a string like "19700101"
def getMondayOfWeek(baseDate, weektype):
    #convert the number into a DateType
    tRealDate = time.strptime(str(baseDate), '%Y%m%d')
    #get the day of a week
    nWeekDay = int(time.strftime('%w', tRealDate))
    if nWeekDay == 0:
        nWeekDay = 7
    nAddNumber = (int(weektype) - nWeekDay) * 1440
    dRealDate = datetime.datetime(tRealDate[0], tRealDate[1], tRealDate[2])
    dRealDate2 = dRealDate + datetime.timedelta(minutes=nAddNumber)
    ndate = int(str(dRealDate2)[0:4] + str(dRealDate2)[5:7] + str(dRealDate2)[8:10])
    return ndate

# ------------------------------------------------------------------------------------------------    

NOW = lambda: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
script_name = __file__
    
def Usage():
    print "Usage:%s <YYYYMMDD> <para>" % script_name

def write_log(msg):
    '''打日志'''
    tdw_obj.WriteLog("|%s|msg ----> %s" % (script_name, msg))

def is_mth_end_day(day):
    '''判断所给日期(格式yyyymmdd)是否月末，是在返回True，否则返回False'''
    original_mth = day[4:6]
    after_day_mth = dateDelta(day, 1)[4:6]
    # 如果前后的月份不同，说明是月末
    return original_mth != after_day_mth

def date2Str(dateNum):
    dateNum = str(dateNum)
    if len(dateNum) != 8:
        write_log("dateNum_to_string func ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return None
    return '%s-%s-%s' % (dateNum[:4], dateNum[4:6], dateNum[6:])

def _exec_sql(sql='', raise_exception=False):
    '''执行 SQL 语句'''
    write_log("running:\n" + sql)
    try:
        rs = tdw_obj.execute(sql)
        write_log("query result:\n" + str(rs))
        return rs
    except Exception, e:
        write_log("ERROR execute sql failed: " + e.message)
        if raise_exception: raise e

def parse_argv(argv):
    '''准备全局变量，获取参数'''
    global iDate
    iDate = argv[0]
    
def create_rs_table():
    #检查并创建目标表
    sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS qt_oss_game_bbs_stay (
        dtStatDate BIGINT,
        iGameCode bigint comment 'cf=306733、lol=797535',
        iPeriod bigint comment '统计周期，7=周，30=月',
        iPrevNum bigint comment '上周期活跃数',
        iStayNum bigint comment '本周期留存数'
    ) STORED AS PGDATA"""
    _exec_sql(sql)
    
def create_mid_table():
    #检查并创建中间表
    sql = """
    CREATE TABLE IF NOT EXISTS qt_tb_RoomModel_Valid_Act_Reg_mid (
        statis_date BIGINT,
        iRoomModel bigint,
        iRoomId bigint,
        iUin bigint
    ) 
    PARTITION BY LIST( statis_date )
    (    
        PARTITION default
    )
    STORED AS FORMATFILE COMPRESS
    """
    _exec_sql(sql)
    
def add_partition(table):
    _exec_sql("alter table %s drop partition (p_%s) " % (table, iDate))
    _exec_sql("alter table %s add partition p_%s values in ('%s') " % (table, iDate, iDate))

def build_mid_data():
    pass

def Stat():
	 #统计cf日新增
    sql = """
	insert table qt_oss_game_bbs_Reg_cf 
	select 
		%s as dtStatDate,
		 a.pvi as iuin  
	from  
		(
			select 
				distinct pvi 
			from 
				dw_ta::ta_log_cf_gamebbs_qq_com 
			where
				daytime='%s'
	    ) a 
	left outer join 
		(
		select 
		distinct iuin 
		from 
		qt_oss_game_bbs_Reg_cf 
		where 
		dtStatDate<%s 
		) b 
	on a.pvi=b.iuin 
	where b.iuin  is null 
	group by a.pvi
		""" % (iDate, iDate,iDate)
    _exec_sql(sql="delete from qt_oss_game_bbs_Reg_cf where dtStatDate=%s  " % iDate, raise_exception=True)
    _exec_sql(sql, raise_exception=True)
    
	#统计cf日活跃   
    sql = """
	insert table qt_oss_game_bbs_act_cf  
	select 
		%s as dtStatDate,
		 iCfActNum as iCfActNum   
	from  
		(
			select 
				count(distinct pvi) as iCfActNum 
			from 
				dw_ta::ta_log_cf_gamebbs_qq_com 
			where
				daytime='%s'
	    ) a 
	
		""" % (iDate, iDate)	
    _exec_sql(sql="delete from qt_oss_game_bbs_act_cf where dtStatDate=%s  " % iDate, raise_exception=True)  
    _exec_sql(sql, raise_exception=True)
       
    #统计LOl日新增
    sql = """ 
	insert table qt_oss_game_bbs_Reg_lol  
	select 
		%s as dtStatDate,
		 a.pvi as iuin  
	from  
		(
			select 
				distinct pvi 
			from 
				dw_ta::ta_log_bbs_lol_qq_com  
			where
				daytime='%s'
	    ) a 
	left outer join 
		(
		select 
		distinct iuin 
		from 
		qt_oss_game_bbs_Reg_lol 
		where 
		dtStatDate<%s  
		) b 
	on a.pvi=b.iuin 
	where b.iuin  is null 
	group by a.pvi
		""" % (iDate, iDate, iDate)
    _exec_sql(sql="delete from qt_oss_game_bbs_Reg_lol where dtStatDate=%s  " % iDate, raise_exception=True)
    _exec_sql(sql, raise_exception=True)  
    
	#统计lol日活跃
    
    sql = """
	insert table qt_oss_game_bbs_act_lol  
	select 
		%s as dtStatDate,
		 iLolActNum as iLolActNum   
	from  
		(
			select 
				count(distinct pvi) as iLolActNum 
			from 
				dw_ta::ta_log_bbs_lol_qq_com  
			where
				daytime='%s'
	    ) a 
	
		""" % (iDate, iDate)	
    _exec_sql(sql="delete from qt_oss_game_bbs_act_lol where dtStatDate=%s  " % iDate, raise_exception=True)  
    _exec_sql(sql, raise_exception=True)  
 
 
    
# 主函数入口
def TDW_PL(tdw, argv=[]):
    global tdw_obj      # 声明为全局变量
    tdw_obj = tdw
    write_log('argv: ' + str(argv))
    if len(argv) != 1:
        Usage()
        return 1
    write_log("start: Program %s start" % script_name)
    
    tdw_obj.execute("use hy_dc_oss")                    # 连接主库
    tdw_obj.execute("set fetch.execinfo.mode=no")       # 关闭返回SQL执行信息到结果集
    tdw_obj.execute("set hive.exec.parallel=true")      # 并行优化开关
    tdw_obj.execute("set hive.inputfiles.splitbylinenum=true")
    
    # 2014-08-11 leavesqin add：CF、LOL论坛日新增
    parse_argv(argv)
    Stat()
    
    write_log("end: Program %s end" % script_name)
    
