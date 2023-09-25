# 区域对比分析  写入Excel
import requests

import sys
sys.path.append('../fugitive_dust')
import   request.dust_url as web_url 
import common_function.common_function as common
import json
from decimal import Decimal
import math
# 计算指定列开始的同列数字的累加结果.从第几列开始累加
def sum_value(data:list,start_column_index:int)->list:
    a = []
    for item in data:
        for i in item :
            temp = []
            temp.append(Decimal(i))
        a.append()
    for item in data:
        print(item[start_column_index:])
    result = [sum(column[start_column_index:]) for column in zip(*a)]
    return result


requests.packages.urllib3.disable_warns()
session= requests.session()
session.headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Origin": "https://10.90.1.226:9402",
        "Referer":"https://10.90.1.226:9402/DustManger/web/static/SubPages/SiteDataCompare.html",
        "Sec-Fetch-Site":"same-origin",
        "X-Requested-With": "XMLHttpRequest"


}
print('cookie:',session.cookies.get_dict())

with open('ck.txt','r') as f:
    ck = eval(f.read())
a = requests.utils.cookiejar_from_dict(ck,cookiejar=session.cookies,overwrite = True)
print('改变后的：',session.cookies.get_dict())



# 请求参数
playload={'sTime':'2023-01','eTime':'2023-08','type':6}
r = session.post(web_url.get_url(4),data=playload,verify=False)
print(r.text)

# avg_data = json.loads(r.text.replace('null', 'None'))

avg_data =eval(r.text.replace('null', 'None')) 
print('原始数据为：',avg_data,len(avg_data))

# 将字典的列表转为列表的列表
r1 = common.dict_to_list(avg_data)
print('原始:',r1)
# [区，5月，4，7，6，1，3，2]
for item in r1:
   del item[1:3]
print('删除后1',r1)
for item in r1:
    del item[2:3]

# 计算从第二类开始的累加结果
print('删除后2',r1)
# print('类型：',r1[0][1],type(r1[0][1]))

# sum_result = sum_value(r1,2)

# market_avg =  [format(item/16, '.3f') for item in sum_result]
# print('合计，市均值',sum_result,market_avg,)

# 将列表调整为月份升序
r2 = []
for i in r1:
    temp = []
    temp.append(i[0])
    temp.append(i[6])
    temp.append(i[8])
    temp.append(i[7])

    temp.append(i[3])
    temp.append(i[2])

    temp.append(i[5])
    temp.append(i[4])
    temp.append(i[1])
    r2.append(temp)
print('顺序为：',r2)
cloum_names = ['区名-工地(mg/m³)','2023-1月','2月','3月','4月','5月','6月','7月']
common.list_to_excel(r2,cloum_names,'区域对比分析',True)




# 写入数据库 直接覆盖原始数据  因为这是统计1~7  下个月爬取的是1~8

# 从数据库直接赋值粘贴即可


