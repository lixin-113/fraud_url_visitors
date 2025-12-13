import os 
import logging


class my_logger:
    def __init__(self):
        self.logger=None
        
    # 配置日志
    def setup_logger(self,log_filename:str,log_dir:str,level=logging.INFO):
        
        """设置并返回一个 logger 实例"""
        # 创建 logs 目录（如果不存在）
        
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_path = os.path.join(log_dir, log_filename)

        # 创建 logger
        logger = logging.getLogger('MyAppLogger')
        logger.setLevel(level)

        # 避免重复添加 handler
        if not logger.handlers:
            # 创建文件处理器
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(level)

            # 创建控制台处理器（可选）
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)

            # 设置日志格式
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # 添加处理器到 logger
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger
    def setup_log(self,log_filename,log_dir):
        # 全局 logger 实例（可以在模块级别初始化一次）
        self.logger = self.setup_logger(log_filename,log_dir)
    

    # 你的日志函数（也可以直接使用 logger.info() 等）
    def log_message(self,message:str,level:str="info"):
        """
            message:日志内容
            message:日志类型
        """
        if level == 'debug':
            self.logger.debug(message)
        elif level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'critical':
            self.logger.critical(message)
        else:
            self.logger.info(f"[Unknown level] {message}")


    def close_logger(self):
        """关闭 logger，释放资源"""
        if self.logger:
            handlers = self.logger.handlers[:]
            for handler in handlers:
                handler.close()
                self.logger.removeHandler(handler)
logger=my_logger()


# 测试
if __name__ == '__main__':
    logger=my_logger()
    logger.setup_log("app.log","/data/app/logs")
    logger.log_message(f"wogagfagdgag")