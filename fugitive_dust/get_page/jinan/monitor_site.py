# 检测点 写入成功！

import requests

import pandas as pd
from sqlalchemy import create_engine


import sys
# sys.path.append('D:\\z\\workplace\\VsCode\\show\\fugitive_dust')
import request.dust_url as web_url


# 获取监测点某段时间对应的浓度
def monitor_site_data(typeID,begin_time,end_time):
    requests.packages.urllib3.disable_warns()
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
        'div':3,
        'typeID':typeID
    }
  

    r = session.post(web_url.get_url(1),data=playload,verify=False)
    # 转为列表
    site_data = eval(r.text)
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
    
    print('数据为：',site_data,len(site_data))





if __name__ == '__main__':
    monitor_site_data('1','2023-09-04 00:00:00','2023-09-04 23:59:59')


