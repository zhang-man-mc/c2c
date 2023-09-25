import time
from utils.log_utils import LogUtils
def login(mode):
    """登陆
    """
    login_success = False
    count = 0

    while count < 5 and login_success == False:
        login_success = web_login(mode,count)
        time.sleep(5)
        count = count + 1
    return login_success


def web_login(mode,times):
    # 一次就成功
    # 失败2次后 第三次成功
    # 一直失败
    # match mode:
    #     case 0 :
    #         return True
    #     case 1:
    #         if times >2 :
    #             return True
    #         else :
    #             return False
    #     case 2:
    #         return False

    if mode == 0:
        LogUtils.info('登陆一次就成功')
        return True
    elif mode == 1  :
        if times > 1 :
            LogUtils.info('登陆失败2次后 第三次成功')
            return True
        else:
            return False
    elif mode == 2:
        LogUtils.info('登陆一直失败')
        return False
    
    

        
