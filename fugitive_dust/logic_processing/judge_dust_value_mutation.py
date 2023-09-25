from datetime import datetime, timedelta


def group_by_time_interval(data:list)->list:
    """相邻数据时间小于或等于30分钟分为一组

    Args:
        data (list): 某站点的所有数据

    Returns:
        list: 时间连续的列表
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

            if time_difference <= timedelta(minutes=30):
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
def judge_rate(index_small_value:float,index_large_value:float,rate)->bool:
    """判断两个数值的变化率 是否大于预设的变化率

    Args:
        index_small_value (_type_): 列表中前一个值
        index_large_value (_type_): 列表中后一个值
        rate (_type_): 大于或等于则返回True,小于返回False

    Returns:
        bool: _description_
    """
    rateOfChange = abs((index_large_value - index_small_value) / index_small_value)
    if rateOfChange >= rate :
        return True
    else:
        return False
    
def find_sublist_ranges(data:list,number:int,rate:float)->list:
    """"判断一个子列表的异常区间   [0.715,0.071,0.025,0.023,0.078,0.08,0.01,0.1] , rate为0.5 , 结果为[(0, 2), (3, 4), (5, 7)]

    Args:
        data (list): 连续时间的站点数据
        number (int): 连续突变的次数
        rate (float): 预设突变标准

    Returns:
        list: 返回异常的区间，元素为一个元组形式
    """

    result = []
    start_index = 0
    end_index = 0
    # 减去1
    number = number -1
    for i in range(1, len(data)):
        if judge_rate(data[i-1][3],data[i][3],rate):
            end_index = i
        else:
            # 数据连续4个
            if end_index - start_index >= number:
                result.append((data[start_index], data[end_index]))
            start_index = i
            end_index = i
    # 数据连续4个
    if end_index - start_index >= number:
        result.append((data[start_index], data[end_index]))
    # print('结果为：',result)
    return result

def Rate_of_change(continue_time:list,number:int,rate:float)->list:
    """根据预设的变化率判断异常区间

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
        temp = find_sublist_ranges(continue_time[i],number,rate)
        if temp:
            result_temp.append(temp)
    # 去除子元素外多余的列表嵌套
    for item in result_temp:
        result.append(item)
   
    if result:
         # 因为列表只有一个子列表
        return  result[0]
    else:
        return []

def main(site_all_data:list,numble:int,rate:float)->list:
    group_time = group_by_time_interval(site_all_data)
    result = Rate_of_change(group_time,numble,rate)
    return result

