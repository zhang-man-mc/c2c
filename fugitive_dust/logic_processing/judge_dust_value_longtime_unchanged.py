# 数据长时间无波动。存入的时间段，区间边界是异常时间的起始，不会包含非异常的时间点

# 判断刚爬取到的数据中无波动情况的时间段 
# 按站点名字分组
    # 对刚爬取到的站点按采集时间升序排序
    # 连续超过1小时或以上 连续5个值以上相等则是（包括1小时）
    # 连续的值在0.01以下 连续5个值以上则是

# 有必要查数据库历史数据的最新一条？
        # 查
        # 或者将爬取到的数据写入数据库后再整个进行判断，那么得到的结果要覆盖异常类型为3的数据  先删除类型为3的数据，再写入

# 在断网的文件中一起执行
from setting.exception_nanlysis_parms import EAnalysis

def exceed_one_hour(site_data:list)->list:
    """连续超过1小时以上相等

    Args:
        site_data (list): 按采集时间升序排列的站点数据

    Returns:
        list: 返回超过超过1小时以上的时间段首尾
    """
    # 前后指针
    current=0
    after = current + 1
    result_low_time = []
    while current < (len(site_data)) and after < (len(site_data)):
        if site_data[current][3] == site_data[after][3]:
            after = after + 1
            while after < len(site_data) and (site_data[after-1][3] == site_data[after][3] ) :
                after = after +1
            if (after-current) >= EAnalysis.long_time_no_change:
                # 记录时间
                # 并把current改为after+1的位置 
                result_low_time.append((site_data[current],site_data[after-1]))
                current = after 
                after = current +1

            else:
                # current ,after++
                current = current +1
                after = after +1
        else:
            current = current + 1
            after = current + 1
    return result_low_time


def low_data_exceeding_one_hour(site_data:list)->list:
    """数据超低连续超过1小时以上 

    Args:
        site_data (list): 按采集时间升序排列的站点数据

    Returns:
        list: 返回数据超低超过1小时以上的时间段首尾
    """
    # 前后指针
    current=0
    after = current + 1
    # for i in range(1,len(site_data)-1):
    #     pass    
    result_low_time = []
    while current < (len(site_data)) and after < (len(site_data)):
        if site_data[current][3] <= EAnalysis.data_low and site_data[after][3] <= EAnalysis.data_low:
            after = after +1
            while after < (len(site_data)) and site_data[after-1][3] <= 0.01 and site_data[after][3] :
                after = after +1
            if (after-current) >= EAnalysis.long_time_no_change:
                # 记录时间
                # 并把current改为after+1的位置 
                result_low_time.append((site_data[current],site_data[after-1]))
                current = after 
                after = current +1

            else:
                # current ,after++
                current = current +1
                after = after +1
        else:
            current = current + 1
            after = current + 1

    return result_low_time
         
                
def long_time_unchanged_write_exception_table(result_list:list):
     # 对数据进行添加..
    #  data = []
    #  for item in result_list:
    #       temp=[]
    #       temp.append(item[0][10])
    #       temp.append('数据长时段无波动')
    #       temp.append(3)
    #       temp.append('金山区')
    #       temp.append(item[0][7])
    #       temp.append(item[1][7])
    #       data.append(temp)
    #  df = pd.DataFrame(data)
    #  df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time']
    #  # 写入dust_exception_data表
    #  engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    #  con = engine.connect()
    #  
    #  df.to_sql(name="dust_exception_data", con=con, if_exists="append",index=False,index_label=False)
     print("数据长时段无波动异常写入完成!")



if __name__ == '__main__':
    site_data=[{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.04, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:00:00', 'LST1': '1691132400', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.029, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:45:00', 'LST1': '1691131500', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:15:00', 'LST1': '1691129700', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.078, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.002, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.009, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
]
    list_t = []
    for item in site_data:
        list_t.append(list(item.values()))
    print('长度为：',len(list_t))
    result1 = exceed_one_hour(list_t)
    result2 = low_data_exceeding_one_hour(list_t)
    result = result1 + result2
    # print(result)

    long_time_unchanged_write_exception_table(result)

