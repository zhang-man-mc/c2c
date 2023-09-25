# 需要爬取的扬尘页面url

class UrlConfig:
    ip = 'https://10.90.1.226'

    login_url = ip + ':8089'
    base_url = ip+':9402'

       
    url_login = login_url+'/portal/login.jsp'
    

    # 管理-运行状况统计页面
    # 请求方式  post
    url_run_status_count = base_url+'/DustManger/ReportData/RunstatusCount'

    # 查询-监测点页面
    # 请求方式 post
    url_site_data = base_url+'/DustManger/ReportData/GetSiteData'

    # 统计-点位浓度比较页面
    # 请求方式 post
    url_site_data_compare = base_url+'/DustManger/RoadDustData/GetSiteDataCompareData'

    # 查询-监测点清单页面
    # 请求方式 post
    url_monitor_site = base_url+'/DustManger/ReportData/MonitorOnlineSite'

    # 统计-对比分析页面
    # 请求方式 post
    url_compare_analysis = base_url+'/DustManger/ReportData/CAnalysisGroupByMonth'

   

    # 0 管理-运行状况统计页面url
    # 1 查询-监测点页面url
    # 2 统计-点位浓度比较页面url
    def get_url(status):
        if status == 0:
            return UrlConfig.url_run_status_count
        elif status == 1:
            return UrlConfig.url_site_data
        elif status == 2:
            return UrlConfig.url_site_data_compare
        elif status == 3:
            return UrlConfig.url_monitor_site
        elif status == 4:
            return UrlConfig.url_compare_analysis

if __name__ == '__main__':
    print(UrlConfig.url_monitor_site)