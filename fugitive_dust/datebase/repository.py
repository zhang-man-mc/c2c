import sys
import os
sys.path.append(os.path.dirname(__file__))
from datebase.database_connect import datebase_single_obj

import pandas as pd
import utils.log_utils as log_utils
from get_page.addess_function import cal_average
from get_page.addess_function import remove_site_data
# from databse.datable_table import DTable
from datebase.datable_table import DTable
from sqlalchemy import text
class Repository:
    """所有的写数据库操作
    """
    log = None
    def __init__(self):
        self.log = log_utils.LogUtils
    
    def write_site_data_table(self,have_removed_site_data:list)->bool:
        """爬去后的监测数据写入站点数据表中

        Args:
            have_removed_site_data (list): 去重后的站点数据

        Returns:
            bool: 是否写入成功
        """
        if have_removed_site_data:
            a = remove_site_data(have_removed_site_data)
            df = pd.DataFrame(a)
            df.columns = DTable.site_data_info
            con = datebase_single_obj.connect_remote_database_write()
            df.to_sql(name=DTable.table[0], con=con, if_exists="append",index=False,index_label=False)
            log_utils.LogUtils.info("浓度数据写入完成! \n\n\n")
            return True
        else:
            log_utils.LogUtils.warn('无浓度数据写入 \n\n\n')
            return False
       
    
    def dust_site_basis_info_store_to_mysql(self):
        log_utils.LogUtils.info('写入基本信息表方法被调用')
        pass

    def read_diffierent_mncode(self):
        con_read = datebase_single_obj.connect_remote_database_read()
        df = pd.read_sql(f'select DISTINCT mn_code from {DTable.table[0]}',con=con_read)
        # #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
        res = df.values.tolist() 
        log_utils.LogUtils.info(f'读取不同的设备编号成功！')
        return res




    def read_site_latest_time_by_mncode(self,mn_code:str):
        con_read = datebase_single_obj.connect_remote_database_read()
        df = pd.read_sql(f'select mn_code, max(lst) from ja_t_dust_site_data_info where mn_code = "{mn_code}"',con=con_read)   #从设备信息表中读取设备编号，店铺名，供应商字段的数据。返回值是DateFrame类型
        res = df.values.tolist()  #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
        for i in range(len(res)):
            res[i][1] =res[i][1].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
        log_utils.LogUtils.info('读取站点最新时间成功！')
        return res
    



    def read_site_latest_time(self):
        """查询读取站点最新数据的时间

        Returns:
            _type_: _description_
        """
        con_read = datebase_single_obj.connect_remote_database_read()
        df = pd.read_sql('select mn_code,latest_time from du_js_t_site_latest_time ',con=con_read)   #从设备信息表中读取设备编号，店铺名，供应商字段的数据。返回值是DateFrame类型
        res = df.values.tolist()  #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
        for i in range(len(res)):
            res[i][1] =res[i][1].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
        log_utils.LogUtils.info('读取该站点最新时间成功！')
        return res
    
    def delete_latest_time_data(self):
        con = datebase_single_obj.connect_remote_database_write()
      
        con.execute(text(f"truncate table {DTable.table[4]}"))
        log_utils.LogUtils.info('删除最新时间成功')
     
    def update_latest_time(self,new_time:list):
        # 读数据库连接
        con = datebase_single_obj.connect_remote_database_write()
      
        con.execute(text(f"truncate table {DTable.table[4]}"))
        log_utils.LogUtils.info('删除最新时间成功')


        df = pd.DataFrame(new_time)
        df.columns = DTable.latest_time_data
        print(df)

        con = datebase_single_obj.connect_remote_database_write()
        # 写入dust_exception_data表
        df.to_sql(name=DTable.table[4], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info('更新站点最新时间成功')

    # 读取数据
    def read_from_site_data_table(self,begin_time:str,end_time:str)->list:
        """从站点数据表中读取某时间段的数据

        Args:
            begin_time (str, optional): 当前时间.
            end_time (str, optional): 前一天时间. 

        Returns:
            list: 站点历史数据
        """
        con_read = datebase_single_obj.connect_remote_database_read()
        df = pd.read_sql(f'select mn_code,dust_value,noise_value,lst from {DTable.table[0]} where lst between "{begin_time}" and "{end_time}"',con=con_read)
        # #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
        res = df.values.tolist() 
        log_utils.LogUtils.info(f'读取站点数据成功！')
        return res

    def write_exception_table(self,result_list):
        # 对数据进行添加..
        data = []
        for item in result_list:
            temp=[]
            temp.append(item[0])
            temp.append('断网或掉线')
            temp.append(0)
            temp.append('金山区')
            temp.append(item[1])
            temp.append(item[2])
            data.append(temp)
            
        df = pd.DataFrame(data)
        df.columns = DTable.exception_part_data
        # 写入dust_exception_data表
        con = datebase_single_obj.connect_remote_database_write()
        
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("断点或断网写入完成!")


    def long_time_unchanged_write_exception_table(self,result_list:list):
        # 对数据进行添加..
        data = []
        for item in result_list:
            temp=[]
            temp.append(item[0][10])
            temp.append('数据长时段无波动')
            temp.append(3)
            temp.append('金山区')
            temp.append(item[0][7])
            temp.append(item[1][7])
            data.append(temp)
        df = pd.DataFrame(data)
        df.columns = DTable.exception_part_data
        # 写入dust_exception_data表
        con = datebase_single_obj.connect_remote_database_write()
        
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("数据长时段无波动异常写入完成!")

    def near_exceeding_standard_write_to_exception_table(self,result_list):
        #  对数据进行添加..
        data = []
        for item in result_list:
            temp=[]
            # 设备编号
            temp.append(item[0][10]) 
            temp.append('临近超标异常')
            temp.append(5)
            temp.append('金山区')
            temp.append(item[0][7])
            temp.append(item[1][7])
            data.append(temp)
        df = pd.DataFrame(data)
        df.columns = DTable.exception_part_data
        # 写入dust_exception_data表
        con = datebase_single_obj.connect_remote_database_write()
        #  
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("临近超标异常写入完成!")



    def borderline_num_write_exception_table(self,result_list:list):
        """写入异常表

        Args:
            result_list (list): 异常的数据
        """
        # 对数据进行添加..
        data = []
        for item in result_list:
            temp=[]
            # 设备编号
            temp.append(item[0][10]) 
            temp.append('单日超标次数临界异常')
            temp.append(6)
            temp.append('金山区')
            temp.append(item[0][7])
            temp.append(item[-1][7])
            data.append(temp)
        df = pd.DataFrame(data)
        df.columns = DTable.exception_part_data
        con = datebase_single_obj.connect_remote_database_write()
        # 写入dust_exception_data表
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("单日超标次数临界异常写入完成!")



    def moving_average_write_exception_table(self,mutation_exception:list):
        """写入异常表

        Args:
            mutation_exception (list): 异常区间的数据
        """

        # 预处理
        exception = []
        for item in mutation_exception:
            temp = []
            temp.append(item[0][10])
            temp.append('滑动平均值突变')
            temp.append('7')
            temp.append('金山区')
            temp.append(item[0][7])
            temp.append(item[-1][7])
            temp.append(cal_average(item))
            exception.append(temp)
        

        # 写入数据库
        df = pd.DataFrame(exception)
        df.columns = ['mn_code','exception','exception_type','region','begin_time','end_time','avg_value']
        con = datebase_single_obj.connect_remote_database_write()
        # 写入dust_exception_data表
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("滑动平均值突变写入完成!")


    def value_low_write_exception_table(self,result_list:list):
        """将数据超低的站点数据写入扬尘异常表

        Args:
            result_list (list): 数据超低的站点数据
        """
        data=[]
        for item in result_list:
            temp=[]
            temp.append(item['MNCode'])
            temp.append('数据超低')
            temp.append(1)
            temp.append('金山区')
            # 开始时间
            temp.append(item['LST'])
            # 结束时间
            temp.append(item['LST'])
            data.append(temp)

            
        df = pd.DataFrame(data)
        df.columns = DTable.exception_part_data
        con = datebase_single_obj.connect_remote_database_write()
        # 写入dust_exception_data表
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("数据超低异常写入完成!") 



    def value_exceeding_write_exception_table(self,result_list:list):
        """将数据超低的站点数据写入扬尘异常表

        Args:
            result_list (list): 数据超低的站点数据
        """
        data=[]
        for item in result_list:
            temp=[]
            temp.append(item['MNCode'])
            temp.append('数据超标')
            temp.append(2)
            temp.append('金山区')
            # 开始时间
            temp.append(item['LST'])
            # 结束时间
            temp.append(item['LST'])
            data.append(temp)
        df = pd.DataFrame(data)
        df.columns = DTable.exception_part_data
        con = datebase_single_obj.connect_remote_database_write()
        # 写入dust_exception_data表
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("数据超标异常写入完成!") 

    def write_to_dust_exception_table(self,data:list):
        """将量级突变异常区间写入站点数据表中

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
            temp.append(item[0][7])
            # 异常结束时间
            temp.append(item[1][7])
            data_temp.append(temp)
        df = pd.DataFrame(data_temp)
        df.columns = DTable.exception_part_data
        con = datebase_single_obj.connect_remote_database_write()
        # 写入dust_exception_data表
        df.to_sql(name=DTable.table[2], con=con, if_exists="append",index=False,index_label=False)
        log_utils.LogUtils.info("量级突变异常写入完成!")

        # sql 存储
        # 数据库表的列名建立类


    def latest_data(self,data:list)->list:
        site_name = data[0][9]
        # 读取监测点数据库表
        con_read = datebase_single_obj.connect_remote_database_read()

        df = pd.read_sql(f'select a.* from ja_t_dust_site_data_info as a join ja_t_dust_site_info as b on a.mn_code = b.mn_code where b.name = "{site_name}" order by lst desc limit 1',con=con_read)   #从设备信息表中读取设备编号，店铺名，供应商字段的数据。返回值是DateFrame类型
        con_read.close()  #关闭链接
        res = df.values.tolist()  #DateFrame按照行转成list类型，res存放的是设备信息表中的数据
        return res
if __name__ == '__main__':
    # a = Repository().read_site_latest_time()
    # print(a)
    pass