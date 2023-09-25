
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def get_data_by_mncode(mn_code:str,con_read)->list:
    """根据设备编号从站点数据表中读出7点至19点的数据(闭区间)

    Args:
        mn_code (str): 设备编号
        con_read:数据库连接
    Returns:
        _type_: 该设备编号的按采集时间升序排列的所有数据 
    """

    
    df = pd.read_sql(f'SELECT mn_code,dust_value,lst FROM  ja_t_dust_site_data_info WHERE mn_code = "{mn_code}" and TIME(lst) >= "07:00:00" AND TIME(lst) < "19:00:00" order by lst asc',con=con_read)   #从设备信息表中读取设备编号，店铺名，供应商字段的数据。返回值是DateFrame类型
    
    #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
    res = df.values.tolist()  
    # 将timestamps.Timestamp格式转为时间字符串
    for i in range(len(res)):
        res[i][2] =res[i][2].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')

    return res


def get_site_mncode(con_read)->list:
    """读出站点数据表所有不同的设备编号
    Args:
        con_read:数据库连接
    Returns:
        list: 所有不同的设备编号
    """
    
    # 读取监测点数据库表 返回值是DateFrame类型
    df = pd.read_sql('select DISTINCT mn_code from ja_t_dust_site_data_info',con=con_read)   
    res = df.values.tolist()  #DateFrame按照行转成list类型，res存放的是设备信息表中的数据

    # 去除子列表
    site_code = []
    for item in res :
        site_code.append(item[0])

    return site_code



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
        time_obj = datetime.strptime(item[2], '%Y-%m-%d %H:%M:%S')
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


def back_exception_text(exception_type):
    """根据异常数字标识返回中文异常

    Args:
        exception_type (_type_): 异常的数字或字符数字
    """
    if int(exception_type) == 0:
        return '数据缺失'
    elif int(exception_type) == 1:
        return '数据超低'
    elif int(exception_type) == 2:
        return '超标'
    elif int(exception_type) == 3:
        return '数据长时间无波动'
    elif int(exception_type) == 4:
        return '量级突变异常'
    elif int(exception_type) == 5:
        return '临近超标异常'
    elif int(exception_type) == 6:
        return '单日超标次数临界异常'
    elif int(exception_type) == 7:
        return '变化趋势异常'




def write_to_exception_table(data:list,exception_type:str,con):
    """写入异常表
    (默认data的元素为一条待写入的记录 ，
    元素形如[['AQHJ0JS0150481', 0.01, '2023-07-18 10:30:00'], ['AQHJ0JS0150481', 0.008, '2023-07-18 12:30:00']]
     3个字段分别为站点编号,颗粒物浓度,采集时间 )

    Args:
        data (list): 异常数据
        exception_type (str): 异常类型
        con (_type_): 数据库连接
    """
    if len(data)==0:
        print('数据为空，写入失败！')
        return 
    data_temp = []
    for item in data:
        temp = []
        # 设备编号
        temp.append(item[0][0])
        temp.append(back_exception_text(exception_type))
        temp.append(exception_type)
        temp.append('金山区')
        # 异常开始时间
        temp.append(item[0][2])
        # 异常结束时间
        temp.append(item[1][2])
        data_temp.append(temp)

    df = pd.DataFrame(data_temp)
    df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time']

    print(df)
    # 写入dust_exception_data表
    df.to_sql(name="dust_exception_data", con=con, if_exists="append",index=False,index_label=False)
    print(back_exception_text(exception_type)+'写入完成!')






if __name__ == '__main__':
    a= ['20']
    # print(group_by_date())
    print(back_exception_text('5'))
