# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.dirname(__file__))
import time
from utils.log_utils import LogUtils
from login.auto_login_fugitive_dust import web_login
from get_page.fetch_data import FetchData
from utils.date_utils import DateUtils
from datebase.repository import Repository

rep = Repository()
fet  = FetchData()

def login():
    """登陆
    """
    login_success = False
    count = 0

    while count < 5 and login_success == False:
        login_success = web_login()
        time.sleep(5)
        count = count + 1
    return login_success


def get_basis_info(time):
    # 判断是否是零点
    if  DateUtils.get_hour(time)== 0:
        # 回调函数 写入数据库
        return fet.fetch_basis_data(rep.dust_site_basis_info_store_to_mysql)
    else:
        return True




# 获取监测数据
def get_site_data():
    return fet.fetch_dust_data(rep.write_site_data_table)




def main():
    # 登陆网站 
    while True:
        sucess = login()
        count = 0
        is_appear_exception = False
        while count < 4 and sucess:
            now = DateUtils.now_time()

            # 获取监测点基本信息
            if get_basis_info(now) == False:
                count = count + 1
            
            # 获取监测数据
            try :
                status = get_site_data()
            except SyntaxError:
                is_appear_exception  =-True
                break

            if status == False:
                count  = count + 1
            LogUtils.info('本次获取数据完成,程序会在1h后再次获取!')
            time.sleep(3600)
        # 当出现异常
        if is_appear_exception == True :
            # 更新数据表最新时间
            # 读取不同的设备编号
            mn_codes = rep.read_diffierent_mncode()
            latest = []
            # 根据设备编号查找最新时间
            for item in mn_codes:
                r = rep.read_site_latest_time_by_mncode(item[0])   
                latest.append(r)
            # 根据最新时间表
            rep.update_latest_time(latest)
            time.sleep(360)
        else:
            LogUtils.info('登陆失败,程序会在1h后再次尝试登陆!')
            time.sleep(3600)

if __name__ == '__main__':
    main()


# app.run(debug=False,host='0.0.0.0',port=8089)    

# 登陆失败之后 打印值对不对  （登陆失败后，跳过数据获取的部分，打印’登陆失败‘）
# 失败后成功 一直失败 时间对不对  （时间是按设置睡眠时间后执行）
# 一直失败后间隔该小一点  （5s后再次尝试）

# 登陆成功  获取成功后一直成功 打印的间隔       (获取成功间隔5秒再下一次获取)
# 失败N次 n小于限定的次数4，可错范围内先失败后成功  （成功后睡眠5秒，打印的时间是正确的）
# 一直失败  （获取数据失败超过5次后，在1h后重新登陆）