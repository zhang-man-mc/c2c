import sys
sys.path.append('../fugitive_dust')
import math
class DTable:
    # 扬尘数据表
    site_data_info = ['mn_code','dust_value','noise_value','lst','quality','grade','flag']
    # 站点基本信息表
    site_basis_info = ['mn_code','address','name','code','begin_date','duty_company','duty_company_id','end_date','engineering_stage','group_id','group_name','is_online','is_trouble','jhpt_update_time','kindex','latitude','linkman','longitude','phone','province','ring_id','ring_name','type_id','typename','stop_time','active','trouble_num']
    
    exception_data = ['mn_code','exception','exception_type','region','begin_time','end_time','avg_value']
    
    exception_part_data = ['mn_code','exception','exception_type','region','begin_time','end_time']

    latest_time_data = ['mn_code','latest_time']

    # 表名
    table =  ['ja_t_dust_site_data_info','ja_t_dust_site_info','dust_exception_data','du_js_t_site_latest_time','du_js_t_site_latest_time']





# current_time = '2023-01-01 12:00:00'
# a = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
# b = datetime.strptime('23:59:59', "%H:%M:%S")
# next_day = a.replace(hour=0, minute=0, second=0, microsecond=0) + b
# print(a.replace(hour=0, minute=0, second=0, microsecond=0))
# print(a)
# print(next_day)

