from config import LOGPATH,OUTPUTNAME
from get_wxa_files import get_wxa_files
# from fraud_url_visitors import feature_main
from services.fraud_url_visitors import feature_main
from services.get_wxa_fea import wxa_feature_main
from services.classify_visitors_model import model_classify_main
# from services.jiangyin_classify_model import jy_model_classify_main
import time
from datetime import datetime,timedelta
from core.global_logger import logger
import schedule
import os


def visitors_rank_task(location:str="nantong"):

    """每天8点执行的任务"""
    yesterday = datetime.now() - timedelta(days=1)
    task_date = yesterday.strftime("%Y%m%d")
    log_path = os.path.join(LOGPATH,task_date)
    logger.setup_log("main.log",log_path)
 
    logger.logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 执行定时任务！")
    logger.logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 执行定时任务，开始处理日期为 {task_date} 的数据。。。")
    
    # try:
    #     logger.log_message(f"正在进行拉取日期为--{task_date}--网信安数据中。。。")
    #     # print("ff")
    #     # get_wxa_files(date=task_date)
    # except Exception as e:
    #     logger.log_message(f"{task_date}--在拉取网信安数据时出现{e}！！！","error")

    try:
        logger.logger.info(f"正在进行日期为--{task_date}--的访客数据特征分析中。。。")
        feature_data=feature_main(location,task_date)
    except Exception as e:
        logger.logger.error(f"日期为--{task_date}--进行访客数据特征分析中出现{e}！！！")
    
    try:
        logger.logger.info(f"正在进行日期为--{task_date}--的网信安数据特征分析中。。。")
        total_data=wxa_feature_main(feature_data,task_date)
    except Exception as e:
        logger.logger.error(f"日期为--{task_date}--进行网信安数据特征分析中出现{e}！！！")

    
    try:
        logger.logger.info(f"正在进行日期为--{task_date}--的访客风险等级分级中。。。")
        logger.logger.info(f"准备进行地点为--{location}--的访客风险等级分级中。。。")
        model_classify_main(total_data,location,task_date,task_date+OUTPUTNAME)
        # if location == "jiangyin":
        #     jy_model_classify_main(total_data,location,task_date,task_date+OUTPUTNAME)
        # else:
        #     model_classify_main(total_data,location,task_date,task_date+OUTPUTNAME)
    except Exception as e:
        logger.logger.error(f"日期为--{task_date}--进行访客风险等级分级中出现{e}！！！")


    logger.logger.info(f"日期为--{task_date}--进行访客风险等级分级已经完成，等待执行下一日期的定时任务中。。。")
    logger.close_logger()



def task_main():
    visitors_rank_task("kunshan")
    visitors_rank_task("nantong")
    visitors_rank_task("jiangyin")

    return


def main():
    
    # 安排每天 16:30 执行
    schedule.every().day.at("16:30").do(task_main)

    print("定时任务已启动，等待每天 16:30 执行...")

    # 持续运行调度器
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次（也可以 sleep(1)，但会更耗 CPU）
 


if __name__=="__main__":

    main()


