# 平均数据写入成功
# 张总需要的是  数据类型：日  一整月
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import re

import sys
# sys.path.append('D:\\z\\workplace\\VsCode\\show\\fugitive_dust')
import request.dust_url as web_url

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



# 点位浓度
playload={'dataType':'15min','sTime':'2023-08-24 00:00','eTime':'2023-08-24 23:59',"regionID":16,'projectTypeID':'3,5'}
r = session.post(web_url.get_url(2),data=playload,verify=False)
print(r.text,type(r.text))

# 转为列表
avg_data =eval(r.text) 
print(avg_data[0],type(avg_data[0]))

for item in avg_data:
    item['type_name'] = '码头堆场'
    item['type_time'] = '15分钟数据'
    item['month'] = '7月'
print(avg_data,len(avg_data))


# 创建DataFrame对象
df = pd.DataFrame(avg_data)
# 帕斯卡命名改为下划线连接
df.columns = ['Valid_Count','Project_Name','Max_Value','Avg_Value','Project_Id','Min_Value','type_name','type_time','month']
print('df是：',df)

engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/fugitive_dust?charset=utf8")
con = engine.connect()
df.to_sql(name="dust_concentration", con=con, if_exists="append",index=False,index_label=False)
con.close()
print("平均浓度数据写入完成!")

