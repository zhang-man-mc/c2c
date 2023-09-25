
import utils.date_utils as date_utils
import logging


file = open("getData.log",  # 日志文件名字
            encoding="utf-8",  #文件编码
            mode="a"  #模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志 a是追加模式，默认如果不写的话，就是追加模式
            )

logging.basicConfig(format='%(message)s',stream=file,level=logging.DEBUG)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

# class LogUtils:

#     @staticmethod
#     def log(message):
#         str =  '[' + date_utils.DateUtils.now_time()+']: ' + message
#         print(str)

#     # 一切按照预期进行
#     @staticmethod
#     def info(message):
#         str =  '[INFO] ' + '[' + date_utils.DateUtils.now_time()+']: ' + message
#         print(str)

#     # 有潜在问题
#     @staticmethod
#     def warn(message):
#         str =  '[WARN] ' + '[' + date_utils.DateUtils.now_time()+']: ' + message
#         print(str)

#     # 严重的问题,软件不能运行
#     @staticmethod
#     def error(message):
#         str =  '[ERROR] ' + '[' + date_utils.DateUtils.now_time()+']: ' + message
#         print(str)




class LogUtils:

    @staticmethod
    def log(message):
        str =  '[' + date_utils.DateUtils.now_time()+']: ' + message
        print(str)

    # 一切按照预期进行
    @staticmethod
    def info(message):
        str =  '[INFO] ' + '[' + date_utils.DateUtils.now_time()+']: ' + message
        logging.info(str)
        print(str)

    # 有潜在问题
    @staticmethod
    def warn(message):
        str =  '[WARN] ' + '[' + date_utils.DateUtils.now_time()+']: ' + message
        logging.warning(str)
        print(str)

    # 严重的问题,软件不能运行
    @staticmethod
    def error(message):
        str =  '[ERROR] ' + '[' + date_utils.DateUtils.now_time()+']: ' + message
        logging.error(str)
        print(str)

