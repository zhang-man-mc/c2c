
# 请求数据和返回数据后的数据处理



def get_playload(bt,et,type_id):
    playload = {
            'time': 'Quarter',
            'sTime': bt,
            'eTime': et,
            'div': 16,
            'typeID': type_id
        }
    return playload

def get_site_basis_info_playload(type_id:str,online:int)->dict:
    """返回站点基本信息的请求参数

    Args:
        type_id (str): 场景类型
        online (int): 是否在线

    Returns:
        dict: _description_
    """
    playload = {
            'authCompanyI': '',
            'authRegionID': '',
            'authTypeID': '',
            'siteName':'',
            'type:':type_id,
            'dutyCompany':'',
            'online':online,
            'div': 16,
        }
    return playload


def remove_site_data(site_data:list)->list:
    """将爬取的站点数据只保留成数据表字段有的

    Args:
        site_data (list): 爬取的数据

    Returns:
        list: 站点数据表的形式
    """
    
    if site_data:
        data=[]
        for item in site_data:
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
        return data
    else:
        return site_data
    

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
     """根据爬取后的数据的采集时间按升序排列

     Args:
         data (list): 站点数据

     Returns:
         list: 子列表根据采集时间升序排列的列表
     """
     return sorted(data, key=lambda x: x['LST'])

def find_contain_attribute(list_mncode:list,list_data:list)->dict:
    """找到列表list_data中所有包含key_attribute元素的字典。

    Args:
        key_attribute (list): 站点编号
        list_data (list): 站点数据

    Returns:
        dict: 包含的数据
    """

    if len(list_mncode) == 0 or len(list_data) == 0 :
         return {}
    # return  {item: [elem for elem in list_data if list(elem.keys())[0] == item] for item in list_mncode}
    return  {item: [elem for elem in list_data if elem['MNCode'] == item] for item in list_mncode}


def filter_time_list(time_list:list, value:str)->list:
    # 只保留value时间后的数据。给定的时间小于第一个值，就直接返回。大于所有的值时，返回空列表
    result = []
    
    # 如果给定的时间小于第一个值，就直接返回（前提是time_list升序排列）
    if value < time_list[0]['LST']:
        return time_list

    found = False

    for item in time_list:
        if found:
            result.append(item)

        if item['LST'] == value:
            found = True

    return result 


def remove_duc(site_time:list,obj:dict)->list:
    """去重

    Args:
        site_time (list): 各个站点的最新时间
        obj (dict): 字典。键是设备编号，值是包含设备编号的所有列表
         {'ALKA0JS0101896': [['ALKA0JS0101896', 0.063, 54.5, '2023-07-31 00:00:00', '好', 1, 'A'], ['ALKA0JS0101896', 0.063, 54.5, '2023-07-30 23:45:00', '好', 1, 'A']]}
    Returns:
        list: _description_
    """
    # 保留去重后的结果
    st = []

    #  去重  item[0]对应设备编号，item[1]对应时间
    # 找到对象中的属性对应的数据，判断时间是否截止
    for item in site_time:
        # 设备编号对应数据为空,则跳过
        if len(obj[item[0]]) == 0:
            continue
        # 根据设备编号找到对象中的属性对应的数据
        mn_code_data = obj[item[0]]

        # 对列表中的采集时间按升序排列
        mn_code_sorted = sorted_by_time(mn_code_data)
        # 只保留时间后的数据
        data = filter_time_list(mn_code_sorted,item[1])
        # 保留后的各站点数据累加
        st = st + data
    return st


def cal_average(site_data:list)->float:
    """ 计算平均值

    Args:
        site_data (list): 站点数据

    Returns:
        float: 平均值
    """
    if len(site_data) < 1:
        return 0
    
    # 求颗粒物浓度之和
    sum = 0
    for item in site_data:
        sum = sum + item[3]

    # 计算平均值
    average = sum / len(site_data)
    return average

def updata_mncode_latest_time():
    # 获取不同的编号mn_codes
    # for item in mn_codes:
        pass
        # 查询该设备编号的最新时间
        # 更新该设备编号时间

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



if __name__ == '__main__':
    a = ['A','B','C','D','E','F']
    site = [{'A':1,'value':0.2},{'B':2,'value':0.2},{'C':3,'value':0.2},{'D':5,'value':0.2},{'F':7,'value':0.2},{'B':2,'value':0.3},{'B':2,'value':0.3}]
    
    # print(find_contain_attribute(a,site))
    # print(dict_to_list(site))
    # b = ['2023-07-30 23:45:00', '2023-07-31 00:00:00', '2023-07-31 00:15:00', '2023-08-01 00:00:00']
    # print(filter_time_list(b,'2023-07-29 00:10:00'))
           
    site_data=[

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
    b = ['SHXH0JS01000131','A','C']
    c = dict_to_list(site_data)
    print(find_contain_attribute(b,c))
    # print()