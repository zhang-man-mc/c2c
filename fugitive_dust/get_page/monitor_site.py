# 检测点 写入成功！

import requests
import urllib3
import pandas as pd
from sqlalchemy import create_engine


import sys
import os
# sys.path.append('D:\\z\\workplace\\VsCode\\show\\fugitive_dust')
# from request.dust_url im
sys.path.append('../fugitive_dust')
import request.dust_url as web_url


# 获取监测点某段时间对应的浓度
def monitor_site_data(typeID,begin_time,end_time):
    urllib3.disable_warnings()
    session= requests.session()
    session.headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    }

    # 从文件中获取cookie
    with open('ck.txt','r') as f:
        ck = eval(f.read())
    # 覆盖requests的cookie
    requests.utils.cookiejar_from_dict(ck,cookiejar=session.cookies,overwrite = True)
    print('改变后的：',session.cookies.get_dict())

   
    # 查询-监测点 
    playload =  {
        'time':'Quarter',
        'sTime':begin_time,
        'eTime':end_time,
        'div':16,
        'typeID':typeID
    }
  

    r = session.post(web_url.UrlConfig.get_url(1),data=playload,verify=False)
    print('状态码：',r.status_code)
    if r.status_code != 200:
        print('连接失败')
        return 
    # 转为列表
    # site_data = eval(r.text)
    site_data = r.json()
    # print(site_data)
    print('获取数据条数为：',len(site_data))
    # 正常的接口字段，保存所有字段
    have_noise_value= []
    # 保存缺失NoiseValue字段
    not_have_noise_value= []
    # 码头如果存在无nois字段的情况
    for item in site_data:
        if 'NoiseValue'  not in item:
            item['NoiseValue'] = 0
            not_have_noise_value.append(item)
        else:
            have_noise_value.append(item)
    # print('无noise字段',len(not_have_noise_value))
    # print('有noise字段',len(have_noise_value))
    
    # 创建DataFrame对象
    # df_have_noise_value = pd.DataFrame(have_noise_value)
    # df_not_have_noise_value = pd.DataFrame(not_have_noise_value)



    # df_have_noise_value.columns = ['longitude','latitude','Address','Dust_Value','Grade','Group_ID','Group_Name','Lst','LST1','Name','MN_Code','Noise_Value','Project_ID','Quality','S_Name','Type_Name','flag']
    # engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/fugitive_dust?charset=utf8")
    # con = engine.connect()
    # 
    # df_have_noise_value.to_sql(name="monitor_site", con=con, if_exists="append",index=False,index_label=False)
    # print("监测站点写入完成!")
    # # 码头如果存在无nois字段的情况
    # if len(not_have_noise_value) !=0 :
    #     df_not_have_noise_value.columns = ['longitude','latitude','Address','Dust_Value','Grade','Group_ID','Group_Name','Lst','LST1','Name','MN_Code','Project_ID','Quality','S_Name','Type_Name','flag','Noise_Value']
    #     df_not_have_noise_value.to_sql(name="monitor_site", con=con, if_exists="append",index=False,index_label=False)
    #     print('无noise字段数据存在，且写入成功')
   
    # con.close()


if __name__ == '__main__':
    monitor_site_data('3,5','2023-09-22  00:00:00','2023-09-22 23:59:59')


