# 临近超标异常 ：判断每个15分钟均值，如果单日中连续数据处于0.9至1.0的次数超过X（暂定4）个，即认定为异常；

# 对爬取的数据按按站点名字分组，并且采集时间升序排列
# 在断网异常中一起判断即可，它也做了前1项的工作
# 从爬取到的数据 筛选出 日期时间在7点至19点的数据
from datetime import datetime, timedelta
from setting.exception_nanlysis_parms import EAnalysis

def is_between_seven_to_nineteen(time_str:str,beginTime:int,endTime:int)->bool:
    """判断日期时间字符串是否在07:00点至19:00点间

    Args:
        time_str (str): 时间字符串串，形如 '2023-08-23 16:11:00'
        beginTime (int): 开始区间小时
        endTime (int): 结束区间小时

    Returns:
        bool: 在区间内则返回True，否则返回False
    """
    # 将时间字符串转换为datetime对象
    time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    
    # 提取小时和分钟部分
    hour = time.hour
    minute = time.minute
    
    # 判断时间是否在07:00至19:00之间
    if (hour == beginTime and minute >= 0) or (hour > beginTime and hour < endTime) or (hour == endTime and minute == 0):
        return True
    else:
        return False
    
def search_time(havaSortedSiteData:list,beginTime:str,endTime:str)->list:
    """ 截取指定小时区间的数据

    Args:
        havaSortedSiteData (list): 按采集时间升序排列的某站点数据
        beginTime (str): 区间开始的小时
        endTime (str): 区间结束的小时 （比如19点，包括19:00,但不包括之后的时间点）

    Returns:
        list: 返回指定小时区间的数据
    """
    result = []
    for i in range(len(havaSortedSiteData)):
        if is_between_seven_to_nineteen(havaSortedSiteData[i][7],beginTime,endTime) :
            result.append(havaSortedSiteData[i])
    return result


def group_by_time_interval(data:list)->list:
    """相邻数据时间小于或等于15分钟分为一组

    Args:
        data (list): 某站点的按采集时间升序排列的数据

    Returns:
        list: 时间连续的列表 。3层列表
    """
    # 保存全部连续的多组数据
    result = []
    # 保存临时连续的的一组数据
    current_group = []

    for i in range(len(data)):
        if i == 0:
            current_group.append(data[i])
        else:
            previous_time = datetime.strptime(data[i-1][7], '%Y-%m-%d %H:%M:%S')
            current_time = datetime.strptime(data[i][7], '%Y-%m-%d %H:%M:%S')
            time_difference = current_time - previous_time

            if time_difference <= timedelta(minutes=15):
                current_group.append(data[i])
            else:
                result.append(current_group)
                # 成为新一组连续值的初始元素
                current_group = [data[i]]
    
    if current_group:
        result.append(current_group)

    # print('连续时间分组为：',result)
    # print('分组长度为：',len(result))
    return result

def is_dust_value_between_value(dust_value:float,small_value:float,large_value:float)->bool:
    """判断颗粒物浓度是否在给定区间值内

    Args:
        dust_value (float): 颗粒物浓度
        small_value (float): 区间下限
        large_value (float): 区间上限

    Returns:
        bool: 在区间内返回True,否则返回False
    """
    if dust_value >= small_value and dust_value < large_value :
        return True
    else:
        return False


def find_continuous_sublist(sub_list_continue_time:list, min_length:int)->list:
    """判断一个子列表的临近超标异常区间 

    Args:
        sub_list_continue_time (list): 连续时间的列表
        min_length (int): 连续在区间内的次数 

    Returns:
        list: 异常时间的区间
    """
    result = []
    start = None

    for i, num in enumerate(sub_list_continue_time):
        if is_dust_value_between_value(num[3],EAnalysis.near_exceed_low_value,EAnalysis.near_exceed_high_value):
            if start is None:
                start = i
        else:
            if start is not None and i - start >= min_length:
                result.append((sub_list_continue_time[start], sub_list_continue_time[i - 1]))
            start = None

    if start is not None and len(sub_list_continue_time) - start >= min_length:
        result.append((sub_list_continue_time[start], sub_list_continue_time[len(sub_list_continue_time) - 1]))
    return result


def estimate_all_continue_time(continue_time:list,min_length:int=4)->list:
    """根据预设的连续次数判断异常区间

    Args:
        continue_time (list): 某个站点所有数据的连续的时间段列表。 (3层列表)
        number (int): 连续突变的次数
        rate (float): 预设的变化率

    Returns:
        list: 异常的区间
    """
    # 每个i是一个连续的时段数据列表（子列表），i形如[['HMHB0JS0100170', 0.054, '2023-07-01 00:00:00'], ['HMHB0JS0100170', 0.049, '2023-07-01 00:15:00'], ['HMHB0JS0100170', 0.047, '2023-07-01 00:30:00'], ['HMHB0JS0100170', 0.047, '2023-07-01 00:45:00'], ['HMHB0JS0100170', 0.048, '2023-07-01 01:00:00']]
    result = []
    result_temp = []
    for i in range(len(continue_time)):
        temp = find_continuous_sublist(continue_time[i],min_length)
        if temp:
            result_temp.append(temp)
    # 去除子元素外多余的列表嵌套
    for item in result_temp:
        result.append(item[0])
   
    if result:
         # 因为列表只有一个子列表
        return  result
    else:
        return []

def main(asc_sorted_site_data:list):
    interval_time_data = search_time(asc_sorted_site_data,7,19)
    # 相邻数据时间分为一组
    group_data = group_by_time_interval(interval_time_data)
    # 判定异常区间
    result = estimate_all_continue_time(group_data,EAnalysis.near_exceed_num)
    return result

def near_exceeding_standard_write_to_exception_table(result_list):
    # 对数据进行添加..
    #  data = []
    #  for item in result_list:
    #       temp=[]
    #       # 设备编号
    #       temp.append(item[0][10]) 
    #       temp.append('临近超标异常')
    #       temp.append(5)
    #       temp.append('金山区')
    #       temp.append(item[0][7])
    #       temp.append(item[1][7])
    #       data.append(temp)
    #  df = pd.DataFrame(data)
    #  df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time']
    #  # 写入dust_exception_data表
    #  engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    #  con = engine.connect()
    # #  
    #  df.to_sql(name="dust_exception_data", con=con, if_exists="append",index=False,index_label=False)
     print("临近超标异常写入完成!")




if __name__ == '__main__' :       
    a = [['121.06796', '30.903036', '万枫公路环东三路', 0.045, 1, 16, '金山区', '2023-08-01 06:45:00', '1690848000', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.1], 
         ['121.06796', '30.903036', '万枫公路环东三路', 0.9, 1, 16, '金山区', '2023-08-01 07:00:00', '1690848900', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7], 
         ['121.06796', '30.903036', '万枫公路环东三路', 0.9, 1, 16, '金山区', '2023-08-01 07:15:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
         ['121.06796', '30.903036', '万枫公路环东三路', 0.91, 1, 16, '金山区', '2023-08-01 07:30:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
         ['121.06796', '30.903036', '万枫公路环东三路', 0.94, 1, 16, '金山区', '2023-08-01 07:45:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],

           ['121.06796', '30.903036', '万枫公路环东三路', 0.94, 1, 16, '金山区', '2023-08-01 08:45:00', '1690850700', '阿里巴巴上海枫泾镇飞天园区项 目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.9],
          ['121.06796', '30.903036', '万枫公路环东三路', 0.95, 1, 16, '金山区', '2023-08-01 09:00:00', '1690851600', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.4],
          ['121.06796', '30.903036', '万枫公路环东三路', 0.95, 1, 16, '金山区', '2023-08-01 09:15:00', '1690851600', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.4],
          ['121.06796', '30.903036', '万枫公路环东三路', 0.92, 1, 16, '金山区', '2023-08-01 09:30:00', '1690851600', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.4],
            
            ['121.06796', '30.903036', '万枫公路环东三路', 1.06, 1, 16, '金山区', '2023-08-01 19:00:00', '1690852500', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.2],
            
            ['121.06796', '30.903036', '万枫公路环东三路', 0.4, 1, 16, '金山区', '2023-08-01 19:30:00', '1690853400', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.6], 
            ['121.06796', '30.903036', '万枫公路环东三路', 0.96, 1, 16, '金山区', '2023-08-01 19:45:00', '1690854300', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 34.3]]
    # b = search_time(a,7,19)
    # for i in b:
    #     print(i)
    # print(len(b))

    # c = group_by_time_interval(a)
    # print(estimate_all_continue_time(c,4))
    # b = [['121.06796', '30.903036', '万枫公路环东三路', 0.045, 1, 16, '金山区', '2023-08-01 06:45:00', '1690848000', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 33.1], 
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.9, 1, 16, '金山区', '2023-08-01 07:00:00', '1690848900', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7], 
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.96, 1, 16, '金山区', '2023-08-01 07:15:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.99, 1, 16, '金山区', '2023-08-01 07:30:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.98, 1, 16, '金山区', '2023-08-01 07:45:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 1.045, 1, 16, '金山区', '2023-08-01 08:00:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.9, 1, 16, '金山区', '2023-08-01 08:15:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.96, 1, 16, '金山区', '2023-08-01 08:30:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.9, 1, 16, '金山区', '2023-08-01 08:45:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
    #       ['121.06796', '30.903036', '万枫公路环东三路', 0.98, 1, 16, '金山区', '2023-08-01 09:00:00', '1690849800', '阿里巴巴上海枫泾镇飞天园区项目01', 'LCXX0JS0150405', 40393, '好', '联岑', '建筑工地', 'N', 32.7],
          
    #       ]
    main(a)


