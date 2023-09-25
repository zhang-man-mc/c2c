# 量级突变异常（取最大的异常区间）

# 1.从数据中读出不同的站点名字 设备编号，保存在一个列表中
# 2.根据站点名字读出所有的数据  按连续时间来分段 
# 3.从连续的时间中按找变化率标准判断找出突变的时间区间
        # 3.1.从每一个连续值之间的变化率，得到该段的一个突变列表
        # 3.2.计算所有的连续值直接的变化率。保存所有突变结果


# 4.重复2,3，直到所有的站点分析完毕

# 5.写入数据库

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta


def is_time_diff_low_30_minutes(small_time:str,large_time:str)->bool:
    """判断两个时间是否相差30分钟

    Args:
        small_time (str): 时间1
        large_time (str): 时间2

    Returns:
        bool: 相差小于或等于30分钟，返回True。否则返回False
    """
    date1 = datetime.strptime(small_time, "%Y-%m-%d %H:%M:%S")
    date2 = datetime.strptime(large_time, "%Y-%m-%d %H:%M:%S")
    time_diff =abs(date2 - date1)
    return time_diff <= timedelta(minutes=30)

def get_site_mncode()->list:
    """根据设备编号读出站点数据表所有的数据

    Returns:
        list: 所有不同的设备编号
    """
    # 读取监测点数据库表
    engine = create_engine("mysql+pymysql://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    con_read = engine.connect()
    df = pd.read_sql('select DISTINCT mn_code from ja_t_dust_site_data_info',con=con_read)   #从设备信息表中读取设备编号，店铺名，供应商字段的数据。返回值是DateFrame类型
    con_read.close()  #关闭链接
    res = df.values.tolist()  #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
    # 去除子列表
    site_code = []
    for item in res :
        site_code.append(item[0])
    # print(site_code)
    # print('总的设备编号个数为',len(site_code))
    return site_code

def get_data_by_mncode(mn_code:str)->list:
    """根据设备编号从站点数据表中读出7点至19点的数据

    Args:
        mn_code (str): 设备编号

    Returns:
        _type_: 该设备编号的按采集时间升序排列的所有数据 
    """
    engine = create_engine("mysql+pymysql://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    con_read = engine.connect()
    print(con_read)
    df = pd.read_sql(f'SELECT mn_code,dust_value,lst FROM  ja_t_dust_site_data_info WHERE mn_code = "{mn_code}" and TIME(lst) >= "07:00:00" AND TIME(lst) < "19:00:00" order by lst asc',con=con_read)   #从设备信息表中读取设备编号，店铺名，供应商字段的数据。返回值是DateFrame类型
    con_read.close()  #关闭链接
    res = df.values.tolist()  #DateFrame按照行转成list类型，res存放的是设备信息表中的数据

    # 将timestamps.Timestamp格式转为时间字符串
    for i in range(len(res)):
        res[i][2] =res[i][2].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
    # print('该设备编号的数据为',res)
    # print('该设备编号的数据数为：',len(res))
    return res

# def fina_continues_time_slot(data:list)->list:
#     current = 0
#     next = current + 1
#     continue_time = []
#     # 双指针
#     while current < len(data) -1 :
#         temp=[]
#         temp.append(data[current])
#         # 若相邻项时间差小于或等于30分钟，则保存该项，并且指针向后
#         while  (next < len(data) -1) and is_time_diff_low_30_minutes(data[next-1][2],data[next][2]) :
#             temp.append(data[next]) 
#             next = next +1

#         continue_time.append(temp)
#         current = next
#         next = current + 1
    
#     print('分析得到的连续时间段列表为：',continue_time)
#     return continue_time

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
            previous_time = datetime.strptime(data[i-1][2], '%Y-%m-%d %H:%M:%S')
            current_time = datetime.strptime(data[i][2], '%Y-%m-%d %H:%M:%S')
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
    # print(rateOfChange)
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
        if judge_rate(data[i-1][1],data[i][1],rate):
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


def write_to_dust_exception_table(data:list):
    """将异常区间写入站点数据表中

    Args:
        data (list): 异常区间数据
    """
    data_temp = []
    for item in data:
        temp = []
        # 设备编号
        temp.append(item[0][0])
        temp.append('量级突变异常')
        temp.append('4')
        temp.append('金山区')
        # 异常开始时间
        temp.append(item[0][2])
        # 异常结束时间
        temp.append(item[1][2])
        data_temp.append(temp)
    df = pd.DataFrame(data_temp)
    df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time']
    # 写入dust_exception_data表
    engine = create_engine("mysql+pymysql://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    con = engine.connect()
    
    df.to_sql(name="dust_exception_data", con=con, if_exists="append",index=False,index_label=False)
    print("量级突变异常写入完成!")

def main(numble:int,rate:float):
    """主函数

    Args:
        numble (int): 连续突变的次数
        rate (float): 突变的变化率
    """
    except_time = []
    # 获取站点数据表的所有设备号（不重复）
    all_site_mncode = get_site_mncode()
    # 分别对每一个站点数据进行突变分析
    for item in all_site_mncode:
        # 该设备编号对应的站点数据表中的所有数据
        site_all_data = get_data_by_mncode(item)
        # 将所有数据分成连续时间为一组
        group_time = group_by_time_interval(site_all_data)
        # 判定异常的区间
        result = Rate_of_change(group_time,numble,rate)
        # print('异常的区间为，长度为：',result,len(result))
        if result:
            except_time = except_time + result
    print('所有的异常长度为',len(except_time))
    # write_to_dust_exception_table(except_time)
        


if __name__ == '__main__':
    time = [['AQHJ0JS0150481',0.042	,'2023-07-18 10:30:00'],
             ['AQHJ0JS0150481'	,0.041	,'2023-07-18 10:45:00'], 
             ['AQHJ0JS0150481',	0.043,	'2023-07-18 11:00:00'], 
             ['AQHJ0JS0150481',	0.045	,'2023-07-18 11:15:00'], 
             ['AQHJ0JS0150481'	,0.044,	'2023-07-18 11:30:00'], 

             ['AQHJ0JS0150481',	0.041	,'2023-07-18 11:45:00'], 
             ['AQHJ0JS0150481',	0.047	,'2023-07-18 12:00:00'], 

             ['AQHJ0JS0150481'	,0.043,	'2023-07-18 12:15:00'], 
             ['AQHJ0JS0150481'	,0.045	,'2023-07-18 12:30:00'], 
            ]
    continue_time = [['HMHB0JS0100170', 0.054, '2023-07-01 00:00:00'], ['HMHB0JS0100170', 0.079, '2023-07-01 00:15:00'], ['HMHB0JS0100170', 0.047, '2023-07-01 00:30:00'], ['HMHB0JS0100170', 0.09, '2023-07-01 00:45:00'], ['HMHB0JS0100170', 0.22, '2023-07-01 01:00:00'], ['HMHB0JS0100170', 0.23, '2023-07-01 01:00:00'],['HMHB0JS0100170', 0.51, '2023-07-01 01:00:00']]

    # value = [1,7,10,15,17,18,100,20,24,78,80,81,86,84,101]
    
    
    # find_ranges
    # dust_value= [['HMHB0JS0100170', 0.715, '2023-07-01 00:00:00'], ['HMHB0JS0100170', 0.071, '2023-07-01 00:15:00'], ['HMHB0JS0100170', 0.025, '2023-07-01 00:30:00'], 
    #              ['HMHB0JS0100170', 0.023, '2023-07-01 00:45:00'], ['HMHB0JS0100170', 0.078, '2023-07-01 01:00:00'],['HMHB0JS0100170', 0.079, '2023-07-01 01:00:00']
    #              ]
    # for i in range(len(dust_value)-1):
    #     print(judge_rate(dust_value[i][1],dust_value[i+1][1],0.5))
    # print(find_sublist_ranges(dust_value,0.5))

    # a= [[
    #      ['AQHJ0JS0150481', 0.045, '2023-07-18 12:45:00'], ['AQHJ0JS0150481', 0.086, '2023-07-18 13:00:00'], 
    #      ['AQHJ0JS0150481', 0.043, '2023-07-18 13:15:00'], ['AQHJ0JS0150481', 0.14, '2023-07-18 13:30:00'], 

    #      ['AQHJ0JS0150481', 0.15, '2023-07-18 13:45:00'], ['AQHJ0JS0150481', 0.044, '2023-07-18 14:00:00'], 
    #      ['AQHJ0JS0150481', 0.09, '2023-07-18 14:15:00'], ['AQHJ0JS0150481', 0.042, '2023-07-18 14:30:00'], 
        

    #       ]]

    # main(4,0.5)
    # get_data_by_mncode('ALKA0JS0101896')

    print(get_site_mncode())