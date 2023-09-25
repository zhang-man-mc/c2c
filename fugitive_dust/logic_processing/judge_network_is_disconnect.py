# 判断掉线
# 超过45分钟以上无数据算掉线（不包括45分钟）。存入的是时间段，区间边界是异常时间的起始，包含非异常的时间点
# 数据长时段无波动 。存入的是时间段，区间边界是异常时间的起始，不包含非异常的时间点

#修正：断网区间边界取得是无数据的时间点   23/8/21 pm


# 从刚爬取的数据中判断
     # 对刚爬取的数据中NoiseValue字段进行添补
     # 将站点相同名字分组     
     # 将元素化为列表
     # 对列表按照采集时间有小到大的排序
     # 在刚爬取中比较
          # 连续超过45分钟以上的则记录该家站点 起始时间
               # 
     # 和数据库中该站点最新数据比较相差时长
          # 连续超过45分钟以上的则记录该家站点 起始时间

#将判定结果的掉线时间段写入异常表中

import sys
import os
sys.path.append(os.path.dirname(__file__))
from datebase.repository import Repository
from utils.log_utils import LogUtils
from utils.date_utils import DateUtils
from setting.exception_nanlysis_parms import EAnalysis
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import logic_processing.judge_dust_value_longtime_unchanged as longtime_unchanged
import logic_processing.judge_near_exceeding_standard as near_exceeding_standard
import logic_processing.judge_exceeding_borderline_num as exceeding_borderline_num
import logic_processing.judge_moving_average as moving_average
import logic_processing.judge_dust_value_mutation as value_mutation

rep = Repository()
def add_dict(site_data:list)->list:
     """对NoiseValue字段进行添补

     Args:
         site_data (list): 刚从网站获取的数据

     Returns:
         list: 使所有的元素都有'NoiseValue'字段，并且处于字典的最后一个位置
     """
     for item in site_data:
          if 'NoiseValue' not in item:
               item['NoiseValue'] = 0
          else:
               temp = item['NoiseValue']
               del item['NoiseValue']
               item['NoiseValue'] = temp
     
     return site_data


# def divide_into_groups(site_data:list)->list:
#      """将元素中相同的站点名字分为一组

#      Args:
#          site_data (list): 站点数据，元素为字典

#      Returns:
#          list: 列表的每一元素为一组相同的站点名字数据，组的类型也为列表
#      """
#      groups = []
#      # 多字段分组
#      user_sort = sorted(site_data, key=lambda x: x['Name'])
#      # 多字段分组
#      user_group = groupby(user_sort, key=lambda x: x['Name'])
#      for key, group in user_group:
#           # 将分组保存在一个列表中
#           groups.append(list(group))
#      # print('groups:',groups)
#      return groups

def divide_into_groups(site_data: list) -> list:

    groups = {}
    for item in site_data:
        name = item['Name']
        if name in groups:
            groups[name].append(item)
        else:
            groups[name] = [item]
    return list(groups.values())




def dict_to_list(item_is_dict:list)->list:
     """取字典的value,并把元素转为列表

     Args:
         item_is_dict (list): 字典的列表

     Returns:
         list: 返回列表的列表
     """
     list_temp = []
     for item in item_is_dict:
          list_temp.append(list(item.values())) 
     return list_temp


def sorted_by_time(data:list)->list:
     """根据子列表中的采集时间按升序排列

     Args:
         data (list): 站点数据

     Returns:
         list: 子列表根据采集时间升序排列的列表
     """
     return sorted(data, key=lambda x: x[7])





def add_or_sub_minutes(time_str:str,minutes_num:int,type:str)->str:
     """对时间字符串进行加减分钟数运算

     Args:
         time_str (str): 时间字符串，形如'2023-08-21 12:30:00'
         minutes_num (int): 分钟数
         type (str): 可选值为'add','sub'

     Returns:
         str: _description_
     """
     # 转换为 datetime 对象
     dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
     if type == 'add':
          # 加上minutes_num分钟
          new_dt = dt + timedelta(minutes=minutes_num)
     elif type == 'sub' :    
          # 减去10分钟
          new_dt = dt - timedelta(minutes=minutes_num)
     else:
          return 'error'
     # 转换为新的时间字符串
     new_time_str = new_dt.strftime('%Y-%m-%d %H:%M:%S')
     return new_time_str



def offline_judged_by_newdata(data:list)->list:
     """对刚爬取的时间升序数据进行判断,相邻数据是否有超过45分钟的。若有,则保存该两站点信息

     Args:
         data (list): 按采集时间升序排列的站点数据

     Returns:
         list: 掉线或者断网的时间段
     """
     # 结果列表，用于保存满足条件的相邻时间间隔超过45分钟的时间对
     result_list = []
     # 遍历时间列表，比较相邻元素的时间间隔
     for i in range(len(data) - 1):
          if DateUtils.is_time_difference_exceed_some_mins(data[i][7],data[i+1][7],EAnalysis.miss_data_minutes):
               # 参数：站点编号 开始时间  结束时间
               # beginTime = add_or_sub_minutes(data[i][7],15,'add')
               # endTime = add_or_sub_minutes(data[i+1][7],15,'sub')
               # result_list.append((data[0][10],beginTime, endTime))
               result_list.append((data[0][10],data[i][7], data[i+1][7]))
     return result_list



def offline_judged_by_newdata_and_history(data:list)->list:
     """将爬取到的某站点的最小的时间与该站点数据库中最新的一条时间进行对比

     Args:
         data (list):  按采集时间升序排列的站点数据

     Returns:
         list: 掉线或者断网的时间段
     """
     result_list = []
     res = rep.latest_data(data)

     # 数据库中存在该站点的最新数据，则与刚获取的数据时间上进行比较
     if res:
          # 将timestamps.Timestamp格式转为时间字符串
          res[0][4] = res[0][4].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
          # 时间相差超过45min
          if DateUtils.is_time_difference_exceed_some_mins(res[0][4],data[0][7],EAnalysis.miss_data_minutes):
               beginTime = add_or_sub_minutes(res[0][4],15,'add')
               endTime = add_or_sub_minutes(data[0][7],15,'sub')
               result_list.append((data[0][10],beginTime,endTime))
               return result_list
          # 没有超过45min，则返回空结果
          else:
               return []
     # 数据库无该站点的最新数据，则返回空结果
     else:
          return []

def main(site_data:list):
     """爬取的数据分为断网和长时段无波动两中异常情形。

     Args:
         site_data (list): 刚爬取的数据,元素为字典
     """
     result_net_disconnect = []
     result_value_long_time_unchange = []
     result_near_exceeding_standard = []
     result_exceeding_borderline_num = []
     result_moving_average = []
     result_value_mutation = []
     # 刚爬取的数据
     r1 = add_dict(site_data)
     # r2已经是分组过的列表了
     r2 = divide_into_groups(r1)

     for item in r2:
          r3 = dict_to_list(item)

          # 量级突变
          result_vm = value_mutation.main(r3,EAnalysis.mutation_num,EAnalysis.mutation_rate)
          result_value_mutation = result_value_mutation + result_vm

          # 排序
          r4 = sorted_by_time(r3)

          # 数据缺失异常
          result1 = offline_judged_by_newdata(r4)
          # 这个子列表中的数据长度不一致
          # result2 = offline_judged_by_newdata_and_history(r4)
          # 该组站点的掉线时间区间列表
          # result_net_disconnect = result_net_disconnect +result1 + result2
          result_net_disconnect = result_net_disconnect +result1 



          # 数据长时间无波动判断
          result1_lu = longtime_unchanged.exceed_one_hour(r4) 
          # result2_lu = longtime_unchanged.low_data_exceeding_one_hour(r4)  
          result_value_long_time_unchange = result_value_long_time_unchange +result1_lu 

          # 临近超标异常
          result_ne = near_exceeding_standard.main(r4)
          result_near_exceeding_standard = result_near_exceeding_standard + result_ne

          # 单日超标次数临界异常写入
          result_bl = exceeding_borderline_num.travel_site_data(r4)
          result_exceeding_borderline_num = result_exceeding_borderline_num + result_bl

          # 变化趋势异常
          result_ma = moving_average.cal_slide_average(r4,EAnalysis.change_trend_group,EAnalysis.change_trend_interval,EAnalysis.change_trend_rate)
          result_moving_average = result_moving_average + result_ma



     # 断网数据写入
     if result_net_disconnect:
          rep.write_exception_table(result_net_disconnect)
     # 无波动数据写入  
     if result_value_long_time_unchange:
           rep.long_time_unchanged_write_exception_table(result_value_long_time_unchange)  
     # 临近超标异常写入
     if result_near_exceeding_standard:
           rep.near_exceeding_standard_write_to_exception_table(result_near_exceeding_standard)
     # 单日超标次数临界异常写入
     if result_exceeding_borderline_num:
          rep.borderline_num_write_exception_table(result_exceeding_borderline_num)
     # 滑动平均值突变异常写入
     if result_moving_average:
          rep.moving_average_write_exception_table(result_moving_average)
     # 变化趋势异常
     if result_value_mutation:
          rep.write_to_dust_exception_table(result_value_mutation)

     a1  = len(result_net_disconnect)
     a2 = len(result_value_long_time_unchange)
     a3 = len(result_near_exceeding_standard)
     a4 = len(result_exceeding_borderline_num)
     a5 = len(result_moving_average)
     a6 = len(result_value_mutation)

     LogUtils.info(f'数据缺失异常:{a1}')
     LogUtils.info(f'数据长时段无波动异常:{a2}')
     LogUtils.info(f'临近超标异常:{a3}')
     LogUtils.info(f'单日超标次数临界异常:{a4}')
     LogUtils.info(f'滑动平均值突变异常:{a5}')
     LogUtils.info(f'量级突变异常:{a6}')

     # sum = a1 + a2 + a3 + a4 + a5 + a6
     return a1,a2,a3,a4,a5,a6

# 测试
if __name__ == '__main__':
#      site_data=[
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.024, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:00:00', 'LST1': '1691132400', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.024, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:45:00', 'LST1': '1691131500', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.024, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:15:00', 'LST1': '1691129700', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.029, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:45:00', 'LST1': '1691131500', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:15:00', 'LST1': '1691129700', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},

# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 2.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 3.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.078, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.002, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.009, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},

#  {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.009, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 13:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# ]

     # 滑动平均值突变测试
     site_data=[
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.024, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:00:00', 'LST1': '1691132400', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.1, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:45:00', 'LST1': '1691131500', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.2, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:15:00', 'LST1': '1691129700', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
#      {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.3, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:45:00', 'LST1': '1691131500', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:15:00', 'LST1': '1691129700', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},

{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.1, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.15, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.25, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.078, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.002, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.109, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 3.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 2.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},

 {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.009, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 7.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 100.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 13:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
]
     main(site_data)

     # print(add_or_sub_minutes('2023-08-21 12:30:00',50,'sub'))
          