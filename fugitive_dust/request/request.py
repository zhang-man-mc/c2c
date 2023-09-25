import urllib3
import requests
from utils.log_utils import LogUtils

class MyRequest:
    session = None

    def get_session(self):
        if self.session == None :
            urllib3.disable_warnings()
            self.session = requests.session()
            self.session.headers = {
                "Accept":"application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding":"gzip, deflate, br",
                "Connection":"keep-alive",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            }
        # requests.utils.cookiejar_from_dict(my_cookie.cookie,cookiejar=self.session.cookies,overwrite = True)
        return self.session
    
    def post(self,url,data):
        r =  self.get_session().post(url, data=data,timeout=60, verify=False)
        if r.status_code != 200:
            # 打印参数
            LogUtils.error(url+',' + str(r.status_code)+ str(data) )
            return False
        return r.text

my_Request = MyRequest()