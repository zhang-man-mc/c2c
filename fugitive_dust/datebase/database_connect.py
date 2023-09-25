

from sqlalchemy import create_engine

class DataBase:
    """ 远程数据库 """
    con_rm_read = None
    con_rm_write = None
    ip_rm = '114.215.109.124'
    user_rm = 'fumeRemote'
    password_rm = 'feiyu2023'
    port_rm = 3306
    data_base_name_rm = 'fume'


    """ 本机 """
    con_read = None
    con_write = None
    ip = 'localhost'
    user = 'root'
    password = '1234'
    port = 3306
    data_base_name = 'qianduan_sql'



    """连接远程数据库
    """
    def connect_remote_database_read(self):
        
        # if self.con_rm_read == None:
        #     engine = create_engine(f"mysql+pymysql://{self.user_rm}:{self.password_rm}@{self.ip_rm}:{self.port_rm}/{self.data_base_name_rm}?charset=utf8")
        #     self.con_rm_read = engine.connect()
        # return self.con_rm_read

        if self.con_read == None:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.ip}:{self.port}/{self.data_base_name}?charset=utf8")
            
            self.con_read = engine.connect()
        return self.con_read

    def connect_remote_database_write(self):
        """ 写"""
        # if self.con_rm_write == None:
        #     engine = create_engine(f"mysql+pymysql://{self.user_rm}:{self.password_rm}@{self.ip_rm}:{self.port_rm}/{self.data_base_name_rm}?charset=utf8")
        #     self.con_rm_write = engine.connect()
        # return self.con_rm_write
    
        if self.con_write == None:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.ip}:{self.port}/{self.data_base_name}?charset=utf8")
            self.con_write = engine.connect()
        return self.con_write


    """ 连接远程数据库 
    """
    def connect_local_database_read(self):
        """ 读数据
        """
        if self.con_read == None:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.ip}:{self.port}/{self.data_base_name}?charset=utf8")
            self.con_read = engine.connect()
        return self.con_read


    def connect_local_database_write(self):
        """ 写数据
        """
        if self.con_write == None:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.ip}:{self.port}/{self.data_base_name}?charset=utf8")
            self.con_write = engine.connect()
        return self.con_write


    # def disconnect(self):
    #     """断开连接
    #     """
    #     if self.con != None:
    #         self.con.close()







# 其他文件导入此对象即可 
datebase_single_obj = DataBase()