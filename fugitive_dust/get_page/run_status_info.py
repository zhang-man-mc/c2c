import requests
import pandas as pd
from sqlalchemy import create_engine

import sys
# sys.path.append('D:\\z\\workplace\\VsCode\\show\\fugitive_dust')

requests.packages.urllib3.disable_warns()
null=None
# --------------------------------------------session
with open('ck.txt','r') as f:
    ck = eval(f.read())
print(ck,type(ck))
session= requests.session()
session.headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        # 'Origin': 'https://10.90.1.226:9402',
        # 'Referer':'https://10.90.1.226:9402/DustManger/web/static/SubPages/SiteDataCompare.html',
        # 'X-Requested-With': 'XMLHttpRequest',
        # 'Accept':'application/json, text/javascript, */*; q=0.01',
        # 'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8',
        # 'Accept-Encoding': 'gzip, deflate, br',
        # 'Accept-Language':'zh-CN,zh;q=0.9',
        # 'Connection':'keep-alive',
        # 'Content-Length':'97',
        # 'Host':'10.90.1.226:9402',
        # # 'Sec-Ch-Ua':'"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        # 'Sec-Ch-Ua-Mobile':'?0',
        # 'Sec-Fetch-Dest':'empty',
        # 'Sec-Fetch-Mode':'cors',
        # 'Sec-Fetch-Site':'same-origin',

}
print('cookie:',session.cookies.get_dict())

with open('ck.txt','r') as f:
    ck = eval(f.read())

a = requests.utils.cookiejar_from_dict(ck,cookiejar=session.cookies,overwrite = True)
print('改变后的：',session.cookies.get_dict())



# session.cookies.update(cookies)
# print('改变后的：',session.cookies.get_dict())


# 管理-运行状况统计
url_target = 'https://10.90.1.226:9402/DustManger/ReportData/RunstatusCount'
playload =  {
        'type':'1,4,3,5',
        'div':16,
        'online':1
        
}
r = session.post(url_target,data=playload,verify=False)
print('请求后',session.cookies.get_dict())
print(r.text)



# ----------------------------------------requests
# ck ='JSESSIONID=01184703A0A954FEB9F2EB7FC124AFEA; JSESSIONID=7F651238B220091AFCA524B1F7C3A849; GOVSSOSSOTOKEN=2c9483d1837d06230189b071d1c50371'
# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
#     'Cookie':ck
# }
# playload =  {
#         'type':'1,4,3,5',
#         'div':16,
#         'online':1
        
# }
# r = requests.post(url =web_url.get_url(0),headers=headers,data=playload, verify=False )
# # print(r.text,type(r.text))
# # 转为列表
# site_info = eval(r.text)
# print('获取数据条数为：',len(site_info))
