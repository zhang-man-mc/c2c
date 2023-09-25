# 不间断执行 上午8点抓取前一天的数据

import requests
import pandas as pd
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta
import copy
import schedule

import sys
sys.path.append('\\VsCode\\show\\fugitive_dust')
import request.dust_url as web_url
import login.auto_login_fugitive_dust as auto_login
import logic_processing.judge_network_is_disconnect as net_disc
import logic_processing.judge_dust_value_ultra_low as value_low
import logic_processing.judge_dust_value_exceeing as value_exceed
def now_time()->str:
    """返回当前日期时间

    Returns:
        str: 当前日期时间
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def preday_time()->str:
    """返回前一天的日期时间

    Returns:
        str: 前一天的日期时间
    """
    now_time = datetime.now()
    previous_time = now_time - timedelta(days=1)
    return previous_time.strftime('%Y-%m-%d %H:%M:%S')



# 获取监测点某段时间对应的浓度
def monitor_site_data(typeID:str,begin_time:str=preday_time(),end_time:str=now_time())->list:
    """获取监测点场景为typeID的数据

    Args:
        typeID (str): 场景编号
        begin_time (str, optional): 当前时间. Defaults to now_time().
        end_time (str, optional): 前一天时间. Defaults to preday_time().

    Returns:
        list: 获取到的站点数据
    """
    requests.packages.urllib3.disable_warns()

    print('爬取的时间为：',begin_time,end_time)

    session= requests.session()
    session.headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',

    }
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
  

    r = session.post(web_url.get_url(1),data=playload,verify=False)
    # print(r.text)
    # 转为列表
    site_data = eval(r.text)
    print(f'typeID为{typeID},获取数据条数为：',len(site_data))
    
    return site_data
    



# 读取数据
def read_from_site_data_table(begin_time:str=preday_time(),end_time:str=now_time())->list:
    """从站点数据表中读取某时间段的数据

    Args:
        begin_time (str, optional): 当前时间. Defaults to now_time().
        end_time (str, optional): 前一天时间. Defaults to preday_time().

    Returns:
        list: 站点历史数据
    """
    engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    con_read = engine.connect()
    df = pd.read_sql(f'select mn_code,dust_value,noise_value,lst from ja_t_dust_site_data_info where lst between "{begin_time}" and "{end_time}"',con=con_read)
    con_read.close()  
    # #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
    res = df.values.tolist() 
    return res


# 判断数据库是否有该站点的对应时间的数据
# 循环比较
def compare_old_and_new_site_data(old_site_data:list,new_site_data:list)->list:
    """比较数据库中站点数据与新获取的数据，当发现重复时，删去新的数据中重复的

    Args:
        old_site_data (list): 数据库中的站点历史数据
        new_site_data (list): 从网页爬取的数据

    Returns:
        list: 去重后的列表
    """
    new_site_data_temp =copy.deepcopy(new_site_data)
    for index,item in enumerate(new_site_data):
        temp =[]
        temp.append(item['MNCode'])
        temp.append(item['DustValue'])
        temp.append(item['LST'])
        for item_old in old_site_data:
            if temp == item_old:
                del new_site_data_temp[index]
    return new_site_data_temp
        
 
        
# 得到去重后的数据为空时不写入 不空时写入
# 站点信息表不用写  因为站点名字时重复的 已经有基本信息了
# 只写入站点数据表
def write_site_data_table(have_removed_site_data:list)->bool:
    """将去重后的数据写入站点数据表中

    Args:
        have_removed_site_data (list): 去重后的站点数据

    Returns:
        bool: 是否写入成功
    """
    if have_removed_site_data:
        data=[]
        for item in have_removed_site_data:
            temp=[]
            temp.append(item['MNCode'])
            temp.append(item['DustValue'])
            if 'NoiseValue'  not in item:
                temp.append(0)
            else:
                temp.append(item['NoiseValue'])
            temp.append(item['LST'])
            temp.append(item['Quality'])
            temp.append(item['Grade'])
            temp.append(item['flag'])
            data.append(temp)
        df = pd.DataFrame(data)
        df.columns = ['mn_code','dust_value','noise_value','lst','quality','grade','flag']
        
        engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
        con = engine.connect()
        
        df.to_sql(name="ja_t_dust_site_data_info", con=con, if_exists="append",index=False,index_label=False)
        print("站点数据写入完成!")
        return True
    else:
        print('无数据写入')
        return False
    





# 删除重复的

# 重点去重 

# 去重后将结果入库


def start(begin_time:str=preday_time(),end_time:str=now_time()):
    """从获取数据到入库

    Args:
        begin_time (str, optional): 当前时间. Defaults to now_time().
        end_time (str, optional): 前一天时间. Defaults to preday_time().
    """

    # 先模拟登陆 # 抓取
    # auto_login.web_login()

    # 3个场景的数据
    sitedata1 = monitor_site_data(1,begin_time,end_time)
    sitedata2 = monitor_site_data(4,begin_time,end_time)
    sitedata3 = monitor_site_data('3,5',begin_time,end_time)
    sitedata = sitedata1 + sitedata2 + sitedata3
    print(len(sitedata))

    # 读取数据
    site_history_data = read_from_site_data_table()

    # 对比去重
    result = compare_old_and_new_site_data(site_history_data,sitedata)
    print('对比结果为：',len(result))

    # 从刚爬取的数据中判断 断电或断网的时间段 长时间无波动的时间段 临近超标异常 滑动平均值突变
    # 若存在，则写入扬尘异常表中
    net_disc.main(result)

    # 从刚爬取的数据中判断 数据超低的时间点
    # 若存在，则写入扬尘异常表中
    value_low.main(result)

    # 从刚爬取的数据中判断 数据超标的时间点
    # 若存在，则写入扬尘异常表中
    value_exceed.main(result)

    # 写入站点数据表
    if result:
        write_site_data_table(result)
    else:
        print('数据库已存在该时间段数据')





if __name__ == '__main__':
    #循环执行
    # def circle():
    # schedule.every().day.at("13:46").do(start)

    # while True:
    #     # 检查定时任务
    #     schedule.run_pending()
    #     time.sleep(1)


    start_time=time.time()
    start('2023-08-30 00:00:00','2023-08-31 23:59:59')
    end_time=time.time()

    print("共耗时:{:.2f}秒".format(end_time-start_time))
    print('共{}分钟：',(end_time-start_time)/60)



