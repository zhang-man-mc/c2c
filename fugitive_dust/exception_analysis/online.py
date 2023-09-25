import logic_processing.judge_network_is_disconnect as net_disc
import logic_processing.judge_dust_value_ultra_low as value_low
import logic_processing.judge_dust_value_exceeing as value_exceed
# from datebase.repository import Repository
import datebase.repository as repository
from utils.log_utils import LogUtils
import copy

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



def main(site_data,begin_time,end_time):
    LogUtils.info('异常分析开始')
    if site_data:
        # 读数据
        # site_history_data = repository.Repository().read_from_site_data_table(begin_time,end_time)

        # 对比去重
        # result = compare_old_and_new_site_data(site_history_data,site_data)

        # 从刚爬取的数据中判断 数据缺失时间段 长时间无波动的时间段 临近超标异常 滑动平均值突变
        # 若存在，则写入扬尘异常表中
        a1,a2,a3,a4,a5,a6 = net_disc.main(site_data)

        #  数据超低
        # 若存在，则写入扬尘异常表中
        a7 = value_low.main(site_data)

        # 数据超标
        # 若存在，则写入扬尘异常表中
        a8 = value_exceed.main(site_data) 

        sum = a1+a2+a3+a4+a5+a6+a7+a8
        r1 = a1/sum *100
        r2 = a1/sum *100
        r3 = a1/sum *100
        r4 = a1/sum *100
        r5 = a1/sum *100
        r6 = a1/sum *100
        r7 = a1/sum *100
        r8 = a1/sum *100
        
        LogUtils.info(f'数据缺失异常:{a1}   ,异常比例为: {r1}%')
        LogUtils.info(f'数据长时段无波动异常:{a2} ,异常比例为: {r2}%')
        LogUtils.info(f'临近超标异常:{a3}   ,异常比例为: {r3}%')
        LogUtils.info(f'单日超标次数临界异常:{a4} ,异常比例为: {r4}%')
        LogUtils.info(f'滑动平均值突变异常:{a5} ,异常比例为: {r5}%')
        LogUtils.info(f'量级突变异常:{a6}   ,异常比例为: {r6}%')
        LogUtils.info(f'数据超低异常:{a1}   ,异常比例为: {r7}%')
        LogUtils.info(f'数据超标异常:{a1}   ,异常比例为: {r8}%')
        LogUtils.info(f'异常分析结束! 异常总条数为：{sum}' )
        return True
    else:
        return False
    
