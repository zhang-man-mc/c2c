
import sys
import os
sys.path.append('../fugitive_dust')
sys.path.append(os.path.dirname(__file__))
import time

import datebase.repository as rep
# import test_login
# import test_getData
# import utils.date_utils as date_utils
# import utils.log_utils as log_utils

# import time 


# mode_login = 2
# mode_getdata = 1
# def main():
#     # 登陆网站 
#     while True:
#         sucess = test_login.login(mode_login)
#         count = 0
#         while count < 4 and sucess:
#             now = date_utils.now_time()

#             # 获取监测数据
#             if test_getData.get_site_data(mode_getdata,count) == False:
#                 count  = count + 1
            
#             time.sleep(5)
#         log_utils.LogUtils.info('登陆失败')
#         time.sleep(5)


# main()



# 读取站点信息表的不同设备编号

# 遍历所有设备编号查找最新时间

# 写入设备表

rep1 = rep.Repository()

mn_codes = rep1.read_diffierent_mncode()

latest = []

for item in mn_codes:
    r = rep1.read_site_latest_time_by_mncode(item[0])   
    latest.append(r[0])
# rep1.delete_latest_time_data()
# time.sleep(5)
rep1.update_latest_time(latest)




# def divide(a, b):
#     if b == 0:
#         raise ZeroDivisionError("除数不能为0")
#     print('你好')


# def test():
#     return divide(10,0)





# count = 1
# while count < 4 :

#     if count ==2:
#         break
#     try:
#         test()
#     except ZeroDivisionError as e:
#         print("除法运算出错:", e)
#         break
#     count = count + 1 
# print(count)