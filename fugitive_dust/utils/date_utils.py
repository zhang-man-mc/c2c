# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import sys
import datebase.repository as repository

class DateUtils:

    @staticmethod
    def now_time()->str:
        """返回当前日期时间

        Returns:
            str: 当前日期时间
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def preday_time()->str:
        """返回前一天的日期时间

        Returns:
            str: 前一天的日期时间
        """
        now_time = datetime.now()
        previous_time = now_time - timedelta(days=1)
        return previous_time.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_min_time(time_list:list)->str:
        """返回最小的时间

        Args:
            time_listL (list): 时间字符串列表

        Returns:
            str: 最小的时间
        """
        min_time = min(time_list, key=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
        return min_time


    @staticmethod
    def split_time(start_time:str, end_time:str)->list:
        """将时段切割成以每日0点为单位

        Args:
            start_time (str): 开始时间
            end_time (str): 结束时间

        Returns:
            list: 以天为间隔的单元，元祖列表
        """

        start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        result = []

        current_time = start_time
        while current_time < end_time:
            # 获取当天的结束时间
            next_day_end_time = current_time.replace(hour=23, minute=59, second=59)

            # 将当前时间和当天的结束时间添加到结果列表中
            result.append((current_time.strftime('%Y-%m-%d %H:%M:%S'), next_day_end_time.strftime('%Y-%m-%d %H:%M:%S')))

            # 更新当前时间为下一天的开始时间
            current_time = next_day_end_time + timedelta(seconds=1)

        return result


    @staticmethod
    def cal_time_interval(times:list)->tuple:
        """返回最新的超过7天的时间间隔

        Args:
            times (list): 时间字符串列表

        Returns:
            tuple: 元组。最新的超过7天的时间点
        """
        # 保存超过7天的时间间隔
        temp  = []
        for i in range(1,len(times)) :
            a = DateUtils.time_distance(times[i-1],times[i])
            if a >= 7:
                temp.append((times[i],times[i+1]))

        # 返回最新的超过1个礼拜的间隔
        if len(temp) != 0:
            return temp[-1]
        else:
            return temp

    @staticmethod
    def get_dust_data_duration()->list:
        """返回所有站点的最新时间时间，切割后的整体的开始结束时间

        Returns:
            list: 
        """
        # 开始时间：返回所有每个监测点最新时间的一条数据里的最早时间
        all_site_latest_time = repository.Repository().read_site_latest_time()

        # 得到所有站点中最早的时间
        t = []
        for item in all_site_latest_time:
            t.append(item[1])
        # bt = get_min_time(t)
        asc_time = sorted(t)
        a = DateUtils.cal_time_interval(asc_time)

        # 以右端点为开始时间
        if a != []:
            bt = a[1]
        else:
            bt = DateUtils.get_min_time(t)

        et = DateUtils.now_time()
        t = DateUtils.split_time(bt,et)

        # 元素范围最大为1天 
        # 0点拆
        # 结束时间：现在
        # bt = preday_time()
        # et = now_time()
        # 返回一个类
            # 列表 （保存所有站点的最新时间时间，那最新时间做去重对比）
            # 列表[] 起始时间
        # 返回所有站点的最新时间时间，切割后的整体的开始结束时间
        return  all_site_latest_time,t

    @staticmethod
    def get_hour(time):
        t = datetime.strptime(time,'%Y-%m-%d %H:%M:%S')
        return t.hour

    @staticmethod
    def is_between_seven_to_nineteen(time_str:str,beginTime:int,endTime:int)->bool:
        """判断日期时间字符串是否在07:00点至19:00点间

        Args:
            time_str (str): 时间字符串串，形如 '2023-08-23 16:11:00'
            beginTime (int): 开始区间小时
            endTime (int): 结束区间小时

        Returns:
            bool: 在区间内则返回True，否则返回False
        """
        # 将时间字符串转换为datetime对象
        time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        
        # 提取小时和分钟部分
        hour = time.hour
        minute = time.minute
        
        # 判断时间是否在07:00至19:00之间
        if (hour == beginTime and minute >= 0) or (hour > beginTime and hour < endTime) or (hour == endTime and minute == 0):
            return True
        else:
            return False
        

    @staticmethod
    def is_time_difference_exceed_some_mins(small_time:str, large_time:str,times:int=45)->bool: 
        """时间相差超过45分钟则返回True,小于或等于45分钟则返回False

        Args:
            small_time (str): 较小的时间
            large_time (str): 较大的时间
            timrs(int):分钟数

        Returns:
            bool: 判断结果
        """
        date1 = datetime.strptime(small_time, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.strptime(large_time, "%Y-%m-%d %H:%M:%S")
        time_diff = date2 - date1
        return time_diff > timedelta(minutes=times)

    @staticmethod
    def add_or_sub_minutes(time_str:str,minutes_num:int,type:str)->str:
        """对时间字符串进行加减分钟数运算

        Args:
            time_str (str): 时间字符串，形如'2023-08-21 12:30:00'
            minutes_num (int): 分钟数
            type (str): 可选值为'add','sub'

        Returns:
            str: _description_
        """
        # 转换为 datetime 对象
        dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        if type == 'add':
            # 加上minutes_num分钟
            new_dt = dt + timedelta(minutes=minutes_num)
        elif type == 'sub' :    
            # 减去10分钟
            new_dt = dt - timedelta(minutes=minutes_num)
        else:
            return 'error'
        # 转换为新的时间字符串
        new_time_str = new_dt.strftime('%Y-%m-%d %H:%M:%S')
        return new_time_str

    @staticmethod
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

    @staticmethod
    def is_time_diff_one_week(small_time:str,large_time:str)->bool:
        """判断两个时间是否超过7天

        Args:
            small_time (str): 时间1
            large_time (str): 时间2

        Returns:
            bool: 相差小于或等于7天，返回True。否则返回False
        """
        date1 = datetime.strptime(small_time, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.strptime(large_time, "%Y-%m-%d %H:%M:%S")
        time_diff =abs(date2 - date1)
        return time_diff >= timedelta(weeks=1)

    @staticmethod
    def time_distance(time1:str,time2:str)->int:
        """时间相差几天

        Args:
            time1 (str): 时间1
            time2 (str): 时间2

        Returns:
            int: 相差的天数
        """
        date1 = datetime.strptime(time1, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.strptime(time2, "%Y-%m-%d %H:%M:%S")
        time_diff =abs(date2 - date1)
        return time_diff.days
    
    @staticmethod
    def site_data_num(bt:str,et:str)->int:
        """计算两个时间点相差几个15分钟 （开始的时间点算一个）

        Args:
            bt (str): _description_
            et (str): _description_

        Returns:
            int: _description_
        """
        current = datetime.strptime(bt, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=15)
        end_time = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")
        count = 1
        while current <= end_time:
            count = count + 1
            current = current + timedelta(minutes=15)
        return count
if __name__ == '__main__':

    # print(get_hour('2033-09-01 10:12:45'))
    a = ['2023-09-01 12:00:00','2023-09-01 17:59:00','2023-08-31 12:00:00','2023-07-31 12:00:00','2023-06-23 12:00:00','2023-08-31 18:00:00']
    # print(sorted(a))
    # print(min(a))
    # print(get_min_time(a))
    # print(DateUtils.get_dust_data_duration())
    # print(DateUtils.split_time('2023-09-10 09:45:00','2023-09-10 15:15:00'))

    # print('[ERROR] ' + '[' + now_time()+']: ' + 'hello')

    # print(is_time_diff_one_week(a[0],a[1]))
    # print(DateUtils.time_distance(a[0],a[1]))
    print(DateUtils.site_data_num('2023-09-01 00:00:00','2023-09-01 23:59:59'))