# 数据超低异常
# 早7点到晚7点  所有低的值仅记作一条异常         


# 1.从数据库中读出不同的站点名字 设备编号，保存在一个列表中
# 2.根据站点名字读出早7点到晚7点的数据 是按时间升序排列的
# 4.重复2,3，直到所有的站点分析完毕

# 5.写入数据库
import pandas as pd
from sqlalchemy import create_engine

import sys
sys.path.append('../fugitive_dust')
print(sys.path)
import common_function.read_from_mysql.index as index

# 一天的站点数据
def judge_ultra_low(site_data_7_to_19:list)->list:
    """找出该站点一天的数据异常区间

    Args:
        site_data_7_to_19 (list): _description_

    Returns:
        list: _description_
    """
    if len(site_data_7_to_19)==0:
        return
    temp = []
    day_exception = []
    for item in site_data_7_to_19:
        if item[1] <= 0.01 :
            temp.append(item)
    if len(temp)==0:
        return []
    else :
        day_exception.append(temp[0])
        day_exception.append(temp[-1])
        return day_exception



def site_all_day(site_day:list)->list:
    """将时间段数据按日期分组，并判断每天的异常

    Args:
        site_day (list): _description_

    Returns:
        list: 返回默认规则的异常数据
    """
    every_day_data = index.group_by_date(site_day)
    exception_data = []
    for item in every_day_data:
        a = judge_ultra_low(item)
        if len(a)!=0:
            exception_data.append(a)
    return exception_data


def main():
    # 连接数据库
    engine = create_engine("mysql+pymysql://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    con = engine.connect()
    exception_data = []
    # 获取所有站点对应的设备编号
    all_site_code = index.get_site_mncode(con)
    for site in all_site_code:
        # 获取单个站点的早7到晚7的数据
        site_data = index.get_data_by_mncode(site,con)
        # 得到该站点的异常数据
        exception_data =exception_data + site_all_day(site_data)
    print('所有异常为：',exception_data)
    # 写入所有站点的数据
    index.write_to_exception_table(exception_data,'1',con)
    con.close()

if __name__ == '__main__':

    time = [['AQHJ0JS0150481',0.01	,'2023-07-18 10:30:00'],
             ['AQHJ0JS0150481'	,0.041	,'2023-07-18 10:45:00'], 
             ['AQHJ0JS0150481',	0.043,	'2023-07-18 11:00:00'], 
             ['AQHJ0JS0150481',	0.045	,'2023-07-18 11:15:00'], 
             ['AQHJ0JS0150481'	,0.001,	'2023-07-18 11:30:00'], 
             ['AQHJ0JS0150481',	0.041	,'2023-07-18 11:45:00'], 
             ['AQHJ0JS0150481',	0.005	,'2023-07-18 12:00:00'], 
             ['AQHJ0JS0150481'	,0.043,	'2023-07-18 12:15:00'], 
             ['AQHJ0JS0150481'	,0.008	,'2023-07-18 12:30:00'], 

             ['AQHJ0JS0150481'	,0.018	,'2023-07-20 12:30:00'], 
             ['AQHJ0JS0150481'	,0.009	,'2023-07-20 14:30:00'], 
             ['AQHJ0JS0150481'	,0.008	,'2023-07-20 15:30:00'], 
            ]
    
    a = site_all_day(time)
    
    # index.write_to_exception_table(a,'1')
    # main()