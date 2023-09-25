#滑动平均值异常

import pandas as pd
from sqlalchemy import create_engine

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


def compare_before_average(average_group:list,interval:int,diff:float)->bool:
    """将列表最后一个值与以间隔interval进行比较,如果相差超过diff，即认定为异常

    Args:
        average_group (list): 平均值列表
        interval (int): 间隔
        diff (float): 差值比例

    Returns:
        bool: 超过diff返回True，不超过则返回False
    """
    # if abs(average_group[-1] - average_group[-1-interval]) / average_group[-1-interval] >= diff :
    #     return True
    # else:
    #     return False

    """连续三次均超过100%

    Returns:
        _type_: _description_
    """
    if len(average_group) < 4:
        return False
    elif abs(average_group[-1] - average_group[-1-interval]) / average_group[-1-interval] >= diff and abs(average_group[-2] - average_group[-2-interval]) / average_group[-2-interval] >= diff and abs(average_group[-3] - average_group[-3-interval]) / average_group[-3-interval] >= diff:
         return True


def cal_slide_average(site_data:list,num:int,interval:int,diff:float)->list:
    """计算滑动平均值时，自动与前面的计算的平均值进行比较

    Args:
        site_data (list): 某站点数据
        num (int): num为一组
        interval (int): 间隔
        diff (float): 差值比例

    Returns:
        list: 返回的是异常数据区间
    """
    mutation_exception = []
    # 站点数据数小于12个
    if len(site_data) < num:
        return []
    # 平均值列表
    average_group  = []
    for index,item in enumerate(site_data):
        if index  <  num-1:
           continue
        elif index == num-1:
           temp =  cal_average(site_data[:num])
           average_group.append(temp) 
        else :
            temp = cal_average(site_data[index-num:index])
            average_group.append(temp) 

        if compare_before_average(average_group,interval,diff):
            # 加入后3组的数据
            mutation_exception.append(site_data[index-num-2:index])      
    return mutation_exception

def moving_average_write_exception_table(mutation_exception:list):
    """写入异常表

    Args:
        mutation_exception (list): 异常区间的数据
    """

    # 预处理
    # exception = []
    # for item in mutation_exception:
    #     temp = []
    #     temp.append(item[0][10])
    #     temp.append('滑动平均值突变')
    #     temp.append('7')
    #     temp.append('金山区')
    #     temp.append(item[0][7])
    #     temp.append(item[-1][7])
    #     temp.append(cal_average(item))
    #     exception.append(temp)
    
    # # print('exception是',exception)

    # # 写入数据库
    # df = pd.DataFrame(exception)
    # df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time','avg_value']
    # # 写入dust_exception_data表
    # engine = create_engine("mysql+mysqlconnector://root:1234@localhost:3306/qianduan_sql?charset=utf8")
    # con = engine.connect()
    # 
    # df.to_sql(name="dust_exception_data", con=con, if_exists="append",index=False,index_label=False)
    print("滑动平均值突变写入完成!")


def main(site_data:list):
    r1 = cal_slide_average(site_data,12,1,1)
    print(r1)
    # moving_average_write_exception_table(r1)

if __name__ == '__main__':

    # 测试不了，元素必须是列表形式才行
    # 对象无法测试
    site_data=[
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 07:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.02, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 09:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.15, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':0.25, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.078, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 10:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.002, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 10.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 50.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 100.109, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
    ]
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 3.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 2.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},

#  {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.004, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.003, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 11:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.009, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:00:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 7.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:30:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 12:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 100.05, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 13:45:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——3号点', 'MNCode': 'SHXH0JS01000131', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
# ]
    
    main(site_data)