# 通用函数
from openpyxl import Workbook


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



def list_to_excel(data:list,excel_column_name:list,file_name:str,aoto_increse:bool):
    """将列表数写入Excel 
        (代码自动将第一列设为序号，且自增)

    Args:
        data (list): 待写入的数据
        excel_column_name (list): Excel的每列的名称
        file_name (str): Excel的文件名
    """
    # 初始化一个工作簿对象
    wb = Workbook()

    # 使用默认的活动工作表（Sheet），也可以自定义工作表名
    ws = wb.active

    if aoto_increse:
        # 自定义列名
        ws.append(['序号']+excel_column_name)

        # 将数据写入工作表，同时加入自增序号
        for i, row in enumerate(data, start=1):
            ws.append([i] + row)
    else:
        # 自定义列名
        ws.append(excel_column_name)

        #  将数据写入工作表
        for row in data:
            ws.append(row)

    # 保存工作簿为 Excel 文件
    wb.save(file_name+'.xlsx')

    print('已写入Excel')

     
if __name__ == '__main__' :
#      data = [
#     ["John", 25, "USA"],
#     ["Alice", 30, "Canada"],
#     ["Mike", 35, "Australia"]
# ]
#      column = ["Name", "Age", "Country"]
#      file_name = '小明'
    #  list_to_excel(data,column,file_name,False)

    import math

    # 定义列表的列表（包含浮点数）
    data = [
        [1.1, 2.2, 3.3],
        [4.4, 5.5, 6.6],
        [7.7, 8.8, 9.9]
    ]

    start_column_index = 1  # 指定从第1列开始累加

    # 计算指定列开始的同列浮点数的累加结果
    result = [math.fsum(column[start_column_index:]) for column in zip(*data)]

    print(result)  # 输出 [16.5, 18.6]