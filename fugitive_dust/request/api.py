from request.request import my_Request
from request.dust_url import UrlConfig
from utils.log_utils import LogUtils

class Api:

    @staticmethod
    def fetch_site_data(playload):
        """获取监测站点数据
        Args:
            data (_type_): _description_

        Returns:
            _type_: _description_
        """
        # false  
        result = my_Request.post(UrlConfig.url_site_data,playload)
        if result == False:
            # 报错
            LogUtils.error('请求数据失败')
        return result

    @staticmethod
    def fetch_site_basis_info(playload):
        result = my_Request.post(UrlConfig.url_run_status_count,playload)
        if result == False:
            LogUtils.error('获取站点基本信息失败')
        return result
