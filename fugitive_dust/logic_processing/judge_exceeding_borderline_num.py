#单日超标次数临界异常

# 假设数据已按设备编号分组，已按采集时间升序排列
# 按时间同一天分组



import sys
sys.path.append('./')

import pandas as pd
from sqlalchemy import create_engine

from datetime import datetime
from logic_processing.judge_near_exceeding_standard import is_between_seven_to_nineteen,search_time
from setting.exception_nanlysis_parms import EAnalysis




# 找出同一天的时间放一组。
# def group_by_same(sorted_site_data:list)->list:
#     temp = []
#     for item in sorted_site_data:
#         temp.append(item)
#     pass

def group_by_date(sorted_site_data:list)->list:
    """找出同一天的时间放一组

    Args:
        sorted_site_data (list): 已按采集时间升序排列的某站点数据

    Returns:
        list: 返回已按同一天的时间一组的数据
    """
    result = []
    current_date = None
    current_group = []

    for item in sorted_site_data:
        # 解析时间字符串为日期时间对象
        time_obj = datetime.strptime(item[7], '%Y-%m-%d %H:%M:%S')
        date = time_obj.date()

        if date != current_date:
            if current_group:
                result.append(current_group)
            current_group = [item]
            current_date = date
        else:
            current_group.append(item)         
    if current_group:
        result.append(current_group)

    return result



# 发现一天中的异常超标6次,却没有达到7次
def find_exceeding(same_data:list,number:int,not_reach_number:int=7)->int:
    """发现一天中的异常超标6次,却没有达到7次

    Args:
        same_data (list): 日期为同一天的数据
        number (int): 超标的次数

    Returns:
        int: 等于6次则返回True,否则返回False
    """
    count = 0
    for item in same_data:
        if item[3] >= EAnalysis.exceeding_standard:
            count = count + 1
    if count == number and count < not_reach_number:
        return True
    else :
        return False

def borderline_num_write_exception_table(result_list:list):
    """写入异常表

    Args:
        result_list (list): 异常的数据
    """
    # 对数据进行添加..
    # data = []
    # for item in result_list:
    #     temp=[]
    #     # 设备编号
    #     temp.append(item[0][10]) 
    #     temp.append('单日超标次数临界异常')
    #     temp.append(6)
    #     temp.append('金山区')
    #     temp.append(item[0][7])
    #     temp.append(item[-1][7])
    #     data.append(temp)
    # df = pd.DataFrame(data)
    # df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time']
    # # 写入dust_exception_data表
    # engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    # con = engine.connect()
    # 
    # df.to_sql(name="dust_exception_data", con=con, if_exists="append",index=False,index_label=False)
    print("单日超标次数临界异常写入完成!")




def travel_site_data(site_data:list)->list:
    """遍历该站点数据，找出异常,写入扬尘异常表

    Args:
        site_data (list): 某个站点数据

    """
    daytime = search_time(site_data,7,19)
    group_date = group_by_date(daytime)
    # print('按日期分组后：',group_date)
    result = []
    for item in group_date:
        # 发现异常
        if item and find_exceeding(item,EAnalysis.day_exceed_borderline_low_num,EAnalysis.Day_exceed_borderline_high_num):
            # # 保存该异常信息
            # temp =[]
            # # 记录该异常的日期
            # temp.append(item[0][10])
            # temp.append('单日超标次数临界异常')
            # temp.append('5')
            # temp.append('金山区')
            # temp.append(item[0][7])
            # temp.append(item[0][7])
            result.append(item)
    # print('结果为：',result)
    # 写入
    # if result:
    #     borderline_num_write_exception_table(result)
    return result
    





if __name__ == '__main__':
    a = ['2023-07-31 06:00:00','2023-08-01 06:00:00','2023-08-01 07:00:00','2023-08-01 08:00:00','2023-08-01 09:00:00','2023-08-02 00:00:00','2023-08-02 01:00:00','2023-08-03 04:00:00']
    # print(group_by_date(a))
    b = [['121.06796', '30.903036', '万枫公路环东三路', 0.045, 1, 16, '金山区', '2023-07-01 06:45:00', '1690848000', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.1], 
         
         ['121.06796', '30.903036', '万枫公路环东三路', 1, 1, 16, '金山区', '2023-08-01 07:00:00', '1690848900', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7], 
         ['121.06796', '30.903036', '万枫公路环东三路', 1.2, 1, 16, '金山区', '2023-08-01 07:15:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
         ['121.06796', '30.903036', '万枫公路环东三路', 1.91, 1, 16, '金山区', '2023-08-01 07:30:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
         ['121.06796', '30.903036', '万枫公路环东三路', 1.94, 1, 16, '金山区', '2023-08-01 07:45:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
           ['121.06796', '30.903036', '万枫公路环东三路', 0.94, 1, 16, '金山区', '2023-08-01 08:45:00', '1690850700', '阿里巴巴上海枫泾镇飞天园区项 目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.9],
          ['121.06796', '30.903036', '万枫公路环东三路', 1.95, 1, 16, '金山区', '2023-08-01 09:00:00', '1690851600', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.4],
          ['121.06796', '30.903036', '万枫公路环东三路', 1.95, 1, 16, '金山区', '2023-08-01 09:15:00', '1690851600', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.4],
          ['121.06796', '30.903036', '万枫公路环东三路', 0.92, 1, 16, '金山区', '2023-08-01 09:30:00', '1690851600', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.4],
            
            ['121.06796', '30.903036', '万枫公路环东三路', 1.06, 1, 16, '金山区', '2023-08-02 19:00:00', '1690852500', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.2],
            
            ['121.06796', '30.903036', '万枫公路环东三路', 0.4, 1, 16, '金山区', '2023-08-02 19:30:00', '1690853400', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.6], 
           
            ['121.06796', '30.903036', '万枫公路环东三路', 1.96, 1, 16, '金山区', '2023-08-05 11:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3],
            ['121.06796', '30.903036', '万枫公路环东三路', 2.96, 1, 16, '金山区', '2023-08-05 12:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3],
            ['121.06796', '30.903036', '万枫公路环东三路', 3.96, 1, 16, '金山区', '2023-08-05 13:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3],
            ['121.06796', '30.903036', '万枫公路环东三路', 0.96, 1, 16, '金山区', '2023-08-05 14:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3],
            ['121.06796', '30.903036', '万枫公路环东三路', 3.96, 1, 16, '金山区', '2023-08-05 15:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3],
            ['121.06796', '30.903036', '万枫公路环东三路', 4.96, 1, 16, '金山区', '2023-08-05 19:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3],
            ['121.06796', '30.903036', '万枫公路环东三路', 5.96, 1, 16, '金山区', '2023-08-05 20:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3]
            ]
    
    print(group_by_date(b))

    
    # borderline_num_write_exception_table(travel_site_data(b))