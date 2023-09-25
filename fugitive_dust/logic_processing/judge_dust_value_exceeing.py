# 从刚爬取的数据site_data中判断存在数据超标的异常
# 颗粒物浓度大于等于 1 mg/m³时为数据超标 ,只写入超标的时间点

    # 判断站点该时刻的颗粒物浓度是否是数据超标异常。
    # 异常的结果写入扬尘异常表中
import sys
import os
sys.path.append(os.path.dirname(__file__))
from datebase.repository import Repository
from utils.log_utils import LogUtils
from setting.exception_nanlysis_parms import EAnalysis

def is_dust_value_exceeding(site_data:list)->list:
    """判断颗粒物浓度大于 1 mg/m³的站点数据

    Args:
        site_data (list): 爬取的站点数据

    Returns:
        list: 数据超标的站点数据
    """
    result = []
    for item in site_data:
        if item['DustValue'] >= EAnalysis.exceeding_standard :
            result.append(item)
    return result



def main(site_data:list):
    """判断数据超低的站点，将判断结果的站点写入扬尘异常表

    Args:
        site_data (list): 刚爬取的站定数据
    """
    r1 = is_dust_value_exceeding(site_data)
    if r1:
       Repository().value_exceeding_write_exception_table(r1)

    a1 = len(r1)
    return a1


# 测试
if __name__ == '__main__':
    site_data=[{'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.01, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 07:00:00', 'LST1': '1691132400', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
                {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue':1, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:45:00', 'LST1': '1691131500', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'}, 
                {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 0.023, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-08-04 06:15:00', 'LST1': '1691129700', 'Name': '上海利仁混凝土制品有限公司', 'MNCode': 'ALKA0JS0350235', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
                  {'longitude': '121.3818', 'latitude': '30.762127', 'Address': '洙山路303号', 'DustValue': 1.2, 'Grade': 1, 'GroupID': 16, 'GroupName': '金山区', 'LST': '2023-07-26 08:15:00', 'LST1': '1691130600', 'Name': '华平金山银河一号智慧产业园三期A地块项目——2号点', 'MNCode': 'SHXH0JS0100013', 'ProjectID': 37819, 'Quality': '好', 'SName': '安力康', 'TypeName': '码头', 'flag': 'N'},
                 ]
    main(site_data)

