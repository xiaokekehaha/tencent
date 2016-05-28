#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_december.py
# 功能描述:     lolapp12月用户对比数据
# 输入参数:     yyyymmdd    例如：20151208
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-08
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime
import time


def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    #tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    #sDate = argv[0]
    
    

    #tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    


    


 
    
    ##写入事件结果
    sql = ''' 
       INSERT  overwrite TABLE tb_lol_app_user_ei_december
SELECT 
cast(uin as bigint),
ei,
COUNT(*) AS pv,
-1 
FROM 
(
SELECT
get_json_object(kv,'$.uin') AS uin,
CASE 
 WHEN  ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯视频' THEN '资讯视频'
 WHEN  ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯攻略' THEN '资讯攻略'
 WHEN  ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯官方' THEN '资讯官方'



 WHEN ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '视频' THEN '资讯视频'
 WHEN ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '攻略' THEN '资讯攻略'
 WHEN ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '官方' THEN '资讯官方'


 when ei = '发现模块' and get_json_object(kv,'$.title') = '英雄时刻'
               then '英雄时刻观看'
               
               
 when ei = '发现模块' and get_json_object(kv,'$.title') = '对战助手'
               then '对战助手-发现木块'
ELSE 'other' END AS ei

FROM teg_mta_intf::ieg_lol WHERE sdate BETWEEN 20151201 AND 20151231 AND id in (1200678382,1100678382) 
AND ei in ('资讯分类','资讯TAB','发现模块')
)t 
WHERE ei != 'other' and uin is not null
GROUP BY uin,ei
    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    

    
    ##写入page结果
    sql = ''' 
    INSERT TABLE tb_lol_app_user_ei_december
SELECT
t.iuin,
t1.pi AS pi,
SUM(t1.pv) AS pv,
SUM(t1.du) AS du
FROM 
(
SELECT
iuin,
uin_mac,
id
FROM  tb_use_lol_app_tgp_lol_cross_mac_effect WHERE iflag = 0
)t
JOIN
(
SELECT
id,
uin_mac,
pi,
SUM(pv) AS pv,
SUM(du) AS du
FROM
(
SELECT
id,
uin_mac,
pv,
du, 
CASE when pi like '%%com.tencent.qt.qtl.activity.info.InfoBaseActivity%%'  or pi like '%%NewsTabViewController%%'
then '资讯列表'
            
when pi like '%%com.tencent.qt.qtl.activity.find.FindActivity%%'  or pi like '%%LocateViewController%%'
then '发现'

when pi like '%%com.tencent.qt.qtl.activity.sns.MyInfoActivity%%'  or pi like '%%我%%'
then '我'
            
when pi like '%%com.tencent.qt.qtl.activity.friend.battle.BattleDetailActivity%%'  or pi like '%%BattleDetailViewController%%'
then  '战绩详情'

when pi like '%%PeoplenearbyMainActivity%%' or pi like '%%NearbyViewController%%'
then  '附近的人'
                                       
                     
WHEN pi = 'com.tencent.qt.qtl.activity.friend.playerinfo.FriendInfoActivity' 
OR pi = '个人中心客态' THEN '个人中心客态'


WHEN pi = 'com.tencent.qt.qtl.activity.info.HeroMainActivity' OR pi = 'com.tencent.qt.qtl.activity.hero.HeroDetailActivity' 
OR pi = 'HeroMainViewController' OR pi = 'HeroInfoDetailViewController' THEN '英雄资料&英雄详情'

WHEN pi = 'com.tencent.qt.qtl.activity.club.ClubSquareActivity' 
OR pi = 'ClubSquareViewController' THEN '俱乐部广场'


WHEN pi = 'com.tencent.qt.qtl.activity.club.ClubMainPageActivity' 
OR pi = 'ClubMainViewController' THEN '俱乐部主页'


WHEN pi = 'com.tencent.qt.qtl.activity.friend.FriendConversationActivity' 
OR pi = 'CommunityViewController' THEN '好友页卡'


when pi like '%%CurrentMatchViewController%%' or pi like '%%com.tencent.qt.qtl.activity.battle.RealTimeBattleActivity%%' 
then '对战助手-整体'

ELSE 'other' 

END AS pi
FROM tb_lol_app_page_view_new WHERE fdate BETWEEN 20151201 AND 20151231
)t WHERE pi != 'other'
GROUP BY id,uin_mac,pi
)t1
ON(t.id = t1.id AND t.uin_mac = t1.uin_mac)
GROUP BY t.iuin,t1.pi
              

    '''
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' 
    INSERT  OVERWRITE TABLE  tb_lol_app_user_range_active
SELECT
 use_days,
 CASE WHEN GROUPING(ei) = 1 THEN '-100' ELSE ei END AS ei,
 COUNT(DISTINCT iuin) AS uv,
 SUM(pv) AS pv,
 SUM(time) AS time
 FROM
(
SELECT
t.iuin AS iuin,
CASE WHEN t.ilogindays = 1 THEN '1'
WHEN t.ilogindays = 2 THEN '2'
WHEN t.ilogindays >= 3 AND t.ilogindays <= 10  THEN '3-10'
WHEN t.ilogindays >= 11 AND t.ilogindays <= 20  THEN '11-20'
WHEN t.ilogindays >= 21 AND t.ilogindays <= 31  THEN '21-31'
END AS use_days,
t1.ei AS ei,
t1.pv AS pv,
t1.time AS time
FROM
tb_lol_app_uin_december t
JOIN
 tb_lol_app_user_ei_december t1
ON(t.iuin = t1.iuin)
)t2
GROUP BY use_days,
 cube(ei)
 '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    