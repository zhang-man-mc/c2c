# 从数据库历史数据中直接分析异常

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import sys
# sys.path.append('D:\\z\\workplace\\VsCode\\show\\fugitive_dust')
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






# 读取数据
def read_from_site_data_table(begin_time:str=preday_time(),end_time:str=now_time())->list:
    """从数据库站点数据表中读取某时间段的数据

    Args:
        begin_time (str, optional): 当前时间. Defaults to now_time().
        end_time (str, optional): 前一天时间. Defaults to preday_time().

    Returns:
        list: 数据库中站点历史数据
    """
    engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    con_read = engine.connect()
    df = pd.read_sql('select * from ja_t_dust_site_data_info',con=con_read)
    con_read.close()  
    # #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
    res = df.values.tolist() 
    return res

# 删除重复的


# 重点去重 

# 去重后将结果入库





def start(begin_time:str=preday_time(),end_time:str=now_time()):
    """从获取数据到入库

    Args:
        begin_time (str, optional): 当前时间. Defaults to now_time().
        end_time (str, optional): 前一天时间. Defaults to preday_time().
    """

    # 读取历史数据
    site_history_data = read_from_site_data_table()
    print('历史数据条数为：',len(site_history_data))
 


    # 从刚爬取的数据中判断 断电或断网的时间段 长时间无波动的时间段
    # 若存在，则写入扬尘异常表中
    net_disc.main(site_history_data)

    # 从刚爬取的数据中判断 数据超低的时间点
    # 若存在，则写入扬尘异常表中
    value_low.main(site_history_data)

    # 从刚爬取的数据中判断 数据超标的时间点
    # 若存在，则写入扬尘异常表中
    value_exceed.main(site_history_data)






if __name__ == '__main__':
    start()






