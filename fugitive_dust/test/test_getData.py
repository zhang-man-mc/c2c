from utils.log_utils import LogUtils
import time
def get_site_data(mode,times):
    return fetch_dust_data(mode,times)



def fetch_dust_data(mode,times):
    if mode == 0:
        LogUtils.info('获取一次就成功')
        return True
    elif mode == 1  :
        if times > 1 :
            LogUtils.info('获取失败2次后 第三次成功')
            return True
        else:
            return False
    elif mode == 2:
        LogUtils.info('获取一直失败')
        return False