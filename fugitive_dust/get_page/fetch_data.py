import time
# import sys
# sys.path.append('')

from utils.date_utils import DateUtils
from request.api import Api
from utils.log_utils import LogUtils
from get_page.addess_function import *
from exception_analysis.online import main as exception_main

class FetchData:

    # 站点数据
    def fetch_dust_data(self,call_back) -> bool:
        """获取建筑工地，搅拌站，码头堆场的监测数据

        Args:
            call_back (str): 回调函数，写入数据库
           
        Returns:
            list: 获取到的站点数据
        """

        # time时间切割成以天为单位（1,2），（2,3）
        site_time,time_interval = DateUtils.get_dust_data_duration()
        # 获取不同的时间
        
        mn_code_group = []
        for item in site_time:
            mn_code_group.append(item[0])

        for item in time_interval:
            LogUtils.info(f'获取数据时段：{item[0]}-{item[1]}')
            r1 = get_playload(item[0],item[1],1)
            # 搅拌站
            r2 = get_playload(item[0],item[1],4)
            # 码头堆场
            r3 = get_playload(item[0],item[1],'3,5')

            construction_site = Api.fetch_site_data(r1)
            mix_site = Api.fetch_site_data(r2)
            wharf_site = Api.fetch_site_data(r3)
            
            # 保存三个场景的数据
            d = []

            if construction_site != False:
                try:
                    d1 = eval(construction_site)
                    d = d + d1
                except SyntaxError:
                    raise SyntaxError("获取到的数据语法错误")
            else:
                d1 = []
                LogUtils.warn('建筑工地数据获取异常')

            if mix_site != False:
                try:
                    d2 = eval(mix_site)
                    d = d + d2
                except SyntaxError:
                    raise SyntaxError("获取到的数据语法错误")
            else:
                d2 = []
                LogUtils.warn('搅拌站数据获取异常')

            if wharf_site != False:
                try:
                    d3 = eval(wharf_site)
                    d = d + d3
                except SyntaxError:
                    raise SyntaxError("获取到的数据语法错误")
            else:
                d3 = []
                LogUtils.warn('码头数据获取异常')
  
            # 当三个场景数据获取都出现异常 ，返回False
            if construction_site == False or mix_site == False or wharf_site == False:
                return False
            
            # 实例获取到的数据数量
            site_len = len(d)
            # 总数据长度为0
            if site_len == 0:
                LogUtils.warn(f'获取数据总条数为:{len(d)} ( 建筑工地: {len(d1)}，搅拌站: {len(d2)}，码头: {len(d3)})')
                time.sleep(10)
                return True
            else:
                # 计算数据完整率
                # 时段内的数据*数据库站点的数量 = 理论值
                should_num = DateUtils.site_data_num(item[0],item[1])*len(site_time)
                # 实际大于理论，是由于网页又新增站点，而站点基本信息表未更新新增的，造成理论值中站点数量相对减少
                if site_len > should_num:
                    rate = 100
                # 实际小于理论 ，按正常的算术
                else:
                    rate = site_len/should_num *100
                
                LogUtils.info(f'获取数据总条数为:{len(d)} ( 建筑工地: {len(d1)}，搅拌站: {len(d2)}，码头: {len(d3)})   ,数据完整率为: {rate}%')
                # 添加NosieValue字段
                dd = add_dict(d)

                # 将mn_code_group中的设备编号与adjusted_data中的设备编号对应上
                obj = find_contain_attribute(mn_code_group,dd)

                # 去重
                st = remove_duc(site_time,obj)

                LogUtils.info(f'内存中去重完成！ 可写入数据数位: {len(st)}')

                # 如果有历史数据，则进行异常分析
                if len(st) != 0 :
                    exception_main(st,item[0],item[1])
                else:
                    LogUtils.info('暂无数据需异常分析！')

                # 回调函数 把数据写入站点表
                call_back(st)

                # 每次循环，爬取一个时间间隔的所有站点数据
                time.sleep(10)
            
      



    # 站点基本信息
    def fetch_basis_data(self,call_back) -> list:
        
        # r = get_site_basis_info_playload('1,4,3,5',1)
        # site_info = Api.fetch_site_basis_info(r)
        # if site_info != False:
        #     d = eval(site_info)

        # if len(d) == 0:
        #     LogUtils.warn(f'站点基本信息总条数为:{len(d)}')
        #     time.sleep(10)
        #     return True
        # else:
        #     LogUtils.info(f'站点基本信息数据:{d}')
        #     LogUtils.info(f'站点基本信息总条数为:{len(d)}')

        #     # # 去重
        #     site_basis_info = remove_duc()
        #     LogUtils.info(f'内存中去重完成！ 可写入数据数位: {len(site_basis_info)}')

        #     # 回调函数 把数据写入站点表
        #     call_back(site_basis_info)

        #     # 每次循环，爬取一个时间间隔的所有站点数据
        #     time.sleep(10)
        pass