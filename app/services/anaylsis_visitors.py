from config import OUTPUTPATH,OUTPUTNAME,MAP_DICT
import os
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import ast
from core.global_logger import logger

# 得到历史 7 天的历史数据将其整合为一个df
def get_target_date_path(res_list:list,date:str,file_path:list,need_time:int=8):
    start_time = (datetime.strptime(date,"%Y%m%d")-timedelta(days=need_time)).date()
    end_time = datetime.strptime(date,"%Y%m%d").date()
    logger.logger.info(f"正在整合历史结果数据从 {start_time} 到 {end_time} 的数据。。。")
    task_res_list=[]
    try:
        for file_date in res_list:
            file_path_date = datetime.strptime(file_date,"%Y%m%d").date()
            print(start_time,end_time,file_path_date)
            if start_time < file_path_date < end_time:
                res_path = os.path.join(file_path,file_date)
                filename=f"{file_date}{OUTPUTNAME}"
                res_path = os.path.join(res_path,filename)
                task_res_list.append(res_path)
                logger.logger.info(f"已经将日期为 {file_path_date} 的路径 {res_path} 添加到列表中。。")

            elif (file_path_date >= end_time) | (file_path_date <= start_time):
                logger.logger.warning(f"其为 {file_date} 不属于指定时间 {start_time} 到 {end_time}")
                
            else:
                logger.logger.error(f"日期信息错误，请查看 {file_date}！！！")
    except Exception as e:
        logger.logger.error(f"在整合历史数据从 {start_time} 到 {end_time} 的数据时出现：{e}！！！")
    logger.logger.info(f"所要整合的日期的列表已经处理完毕，部分路径为 {str(task_res_list)[:50]}")
    total_data=pd.DataFrame(columns=["msisdn","date_time","res","reason"])
    if not task_res_list:
        return total_data
    try:
        logger.logger.info(f"正在整合任务列表路径中的历史数据中。。。")
        for task_file_path  in task_res_list:
            tmp_data=pd.read_csv(task_file_path,sep="\t",dtype={"msisdn":str,"date_time":str,"res":str,"reason":str})
            total_data=pd.concat([total_data,tmp_data],axis=0)
            logger.logger.info(f"文件 {str(task_file_path)[16:]} 已经添加完成。。。")

    except Exception as e:
        logger.logger.error(f"在整合任务列表路径中的历史数据中出现：{e}！！！")
    logger.logger.info(f"日期为 {date} 所要整合的历史数据已经整合完成。。。")
    return total_data

 # 获取历史7天的访客分级结果
def get_history_res(location:str,date:str):
    visitors_date_res_dir=os.path.join(OUTPUTPATH,location)
    visitors_date_res_list=os.listdir(visitors_date_res_dir)
    logger.logger.info(f"路径{visitors_date_res_dir} 下的部分日期为 {str(visitors_date_res_list)[:50]}")
    target_data=get_target_date_path(visitors_date_res_list,date,visitors_date_res_dir)

    return target_data


# 得到每一个手机号匹配出历史的数据，将有历史数据和无历史数据的分开，并配置当日等级
def anaysis_visitors_fea_by_history(data,history):
    merged=pd.merge(data[['msisdn','date_time']],
                    history[['msisdn','date_time','res']],
                    on="msisdn",
                    how="left",
                    suffixes=("_df1","_df2"))
    
    merged.fillna("",inplace=True)
    result_res = merged.groupby('msisdn').agg({"date_time_df2":list,"res":list}).reset_index()
    result_res['res'] = result_res['res'].astype(str)
    result_res['date_time_df2'] = result_res['date_time_df2'].astype(str)
    stone_part=result_res['res'] == "['']"
    stone_data=result_res[stone_part].copy()
    flex_data=result_res[~stone_part].copy()
    
    stone_data=pd.merge(stone_data[['msisdn']],data[['msisdn','res']],on='msisdn',how='left')
    flex_data=pd.merge(flex_data,data[['msisdn','res']],on='msisdn',how='left',suffixes=("_history","_now"))

    logger.logger.info(f"根据今日手机号码有无历史数据进行分离完成。。。")
    logger.logger.info(f"部分无历史数据的数据为：\n{stone_data[:5]}")
    logger.logger.info(f"部分有历史数据的数据为：\n{flex_data[:5]}")
    return stone_data,flex_data
    
# 根据历史数据获得的信息，去对当日等级进行等级升级
def rule_model(row,date,map_dict):
    history_rank_list=ast.literal_eval(row['res_history'])
    history_date_list=ast.literal_eval(row['date_time_df2'])
    data_length=len(history_rank_list)
    information_dict={}

    # 创建临时信息字典
    tmp_dict={"出现次数":0,"风险等级映射":np.zeros(data_length),"最大风险等级":0}
    information_dict['0-3']=tmp_dict.copy()
    information_dict['4-7']=tmp_dict.copy()
    information_dict['0-7']=tmp_dict.copy()
    del tmp_dict


    
    for i in range(data_length):
        phone_date=datetime.strptime(history_date_list[i],"%Y%m%d").date()

        # 设置时间分段
        start_time=(datetime.strptime(date,"%Y%m%d")-timedelta(days=8)).date()
        tmp_time=(datetime.strptime(date,"%Y%m%d")-timedelta(days=4)).date()
        end_time=(datetime.strptime(date,"%Y%m%d")-timedelta(days=1)).date()
        # 0-3 天
        if tmp_time < phone_date <= end_time:
            information_dict['0-3']["出现次数"]+=1
            information_dict['0-3']["风险等级映射"][i]=map_dict[history_rank_list[i]]
            information_dict['0-3']["最大风险等级"]=max(information_dict['0-3']['最大风险等级'],max(information_dict['0-3']['风险等级映射']))

        # 4-7 天
        if start_time < phone_date <= tmp_time:
            information_dict['4-7']["出现次数"]+=1
            information_dict['4-7']["风险等级映射"][i]=map_dict[history_rank_list[i]]
            information_dict['4-7']["最大风险等级"]=max(information_dict['4-7']['最大风险等级'],max(information_dict['4-7']['风险等级映射']))

        # 0-7 天
        if start_time < phone_date <= end_time:
            information_dict['0-7']["出现次数"]+=1
            information_dict['0-7']["风险等级映射"][i]=map_dict[history_rank_list[i]]
            information_dict['0-7']["最大风险等级"]=max(information_dict['0-7']['最大风险等级'],max(information_dict['0-7']['风险等级映射']))

        # 异常天数
        else:
            logger.logger.error(f"在根据日期分段对历史数据信息进行提取时出现问题！！！")
            logger.logger.error(f"处理历史日期数据为：{history_date_list}，当前处理的风险等级为：{history_rank_list}。。。")


    # print(information_dict)


    # 1.当日为紧急，风险等级不变
    if row['res_now'] == "紧急":
        return f"{row['res_now']}，分类原因：【无。】"
    
    if information_dict['0-3']['出现次数'] > 0:
        # 2.历史风险等级的时间在1-3天内，出现1或多次紧急，且当日风险等级低于紧急，风险等级最终为紧急；出现2或3 次风险等级高于中危或含有1个高危，且当日风险等级为高危，风险等级最终为紧急。
        if (np.sum(information_dict['0-3']['风险等级映射'] ==4) >= 1 ) & (map_dict[row['res_now']] < 4):
            return f"紧急，分类原因：【0-3 历史数据信息：{information_dict['0-3']},出现 1 或多次”紧急“ 且 当日风险等级低于“紧急”】"
        
        if ((np.sum(information_dict['0-3']['风险等级映射'] ==2 ) >= 2) | ((np.sum(information_dict['0-3']['风险等级映射'] >=2 ) >= 2) & (information_dict['0-3']['最大风险等级'] == 3))) & (map_dict[row['res_now']] == 3):
            return f"紧急，分类原因：【0-3 历史数据信息：{information_dict['0-3']},（出现 2 或多次“中危”  或 出现 2 或多次 风险等级高于“中危”中有高危） 且 当日风险等级为”紧急“】" 


        # 3.历史风险等级的时间在1-3天内，风险等级最高且只出现一次高危，且当日风险等级为中危，最终风险等级为高危；出现2或3 次风险等级高于高危，且当日风险等级为中危 ，风险等级最终为紧急。
        if (information_dict['0-3']['最大风险等级']==3) & (information_dict['0-3']['出现次数'] == 1) & (map_dict[row['res_now']] == 2):
            return f"高危，分类原因：【0-3 历史数据信息：{information_dict['0-3']},最大风险等级为“高危” 且 当日风险等级为“中危”】"
        
        if (np.sum(information_dict['0-3']['风险等级映射'] >=3 ) >= 2)  & (map_dict[row['res_now']] == 2):
            return f"紧急，分类原因：【0-3 历史数据信息：{information_dict['0-3']},出现 2 或多次 风险等级高于“高危” 且 当日风险等级为”中危“】" 
        

        # 4.历史风险等级的时间在1-3天内，风险等级最高且只出现一次高危，且当日风险等级为低危，最终风险等级为高危；出现2或3 次风险等级高于高危 ，为最终风险等级为紧急。
        if (information_dict['0-3']['最大风险等级']==3) & (information_dict['0-3']['出现次数'] == 1) & (map_dict[row['res_now']] == 1):
            return f"高危，分类原因：【0-3 历史数据信息：{information_dict['0-3']},最大风险等级为“高危”,且只出现一次 且 当日风险等级为”低危“】"
        
        if (np.sum(information_dict['0-3']['风险等级映射'] >=3 ) >= 2)  & (map_dict[row['res_now']] == 1):
            return f"紧急，分类原因：【0-3 历史数据信息：{information_dict['0-3']},出现 2 或多次 风险等级高于“高危”或 出现3 次风险等级高于低危含有高危 且 当日风险等级为”低危“】" 
    
  
    if information_dict['4-7']['出现次数'] > 0:
        # 5.历史风险等级的时间在4-7天内 ，出现 >=2 次风险等级不高于中危 ，且当日风险等级为中危，最终风险等级为高危。
        if (np.sum((information_dict['4-7']['风险等级映射'] <=2) & (information_dict['4-7']['风险等级映射'] > 0)) >= 2) & (map_dict[row['res_now']] == 2):
            return f"高危，分类原因：【4-7 历史数据信息：{information_dict['4-7']},出现 2 或多次 风险等级低于“中危” 且 当日风险等级为“中危”】" 
    

        # 6.历史风险等级的时间在4-7天内，出现 >=2 次风险等级不高于中危，且当日风险等级为低危，最终风险等级为中危；出现 >=2 次风险等级高于高危，最终风险等级为高危。 
        if (np.sum((information_dict['4-7']['风险等级映射'] <=2) & (information_dict['4-7']['风险等级映射'] > 0)) >= 2) & (map_dict[row['res_now']] == 1):
            return f"中危，分类原因：【4-7 历史数据信息：{information_dict['4-7']},出现 2 或多次 风险等级低于“中危” 且 当日风险等级为“低危”】" 
        
        if (np.sum(information_dict['4-7']['风险等级映射'] >=3 ) >= 2)  & (map_dict[row['res_now']] == 1):
            return f"高危，分类原因：【4-7 历史数据信息：{information_dict['4-7']},出现 2 或多次 风险等级高于“高危” 且 当日风险等级为“低危”】" 
    
    
    if information_dict['0-7']['出现次数'] > 0:
        # 7.历史风险等级的时间在0-7 天内出现 3次且风险等级都为中危或低危，且当日为低危，最终风险等级为中危；若出现3次且最高风险等级高危则最终风险等级为高危；若出现3次且最高风险等级含有紧急则最终风险等级为高危。
        if (np.sum((information_dict['0-7']['风险等级映射'] <=2) & (information_dict['4-7']['风险等级映射'] > 0)) ==3) & (map_dict[row['res_now']] == 1):
            return f"中危，分类原因：【0-7 历史数据信息：{information_dict['0-7']},出现 3 次风险等级低于“中危” 且 当日风险等级为“低危”】"
        
        if (information_dict['0-7']['出现次数'] ==3) & (information_dict['0-7']['最大风险等级']==3) & (map_dict[row['res_now']] <= 2 ):
            return f"高危，分类原因：【0-7 历史数据信息：{information_dict['0-7']},出现 3 次风险等级 且 最大风险等级为“高危” 且 当日风险等级低于“中危”】"

        if (information_dict['0-7']['出现次数'] ==3) & (information_dict['0-7']['最大风险等级']==4) & (information_dict['4-7']['风险等级映射'] >= 3) & (map_dict[row['res_now']] <= 2 ):
                return f"紧急，分类原因：【0-7 历史数据信息：{information_dict['0-7']},出现 3 次风险等级 且 最大风险等级为“紧急” 且当日风险等级低于“中危”】"

        # 8.历史出现风险等级的时间在0-7天内出现 4 次不且风险等级都为中危或低危， 且当日为低危，最终风险等级为高危；若出现4次且最高风险等级为高危或紧急，则最终风险等级为紧急。
        if ((information_dict['0-7']['出现次数'] ==4) & (information_dict['0-7']['最大风险等级'] <= 2)) & (map_dict[row['res_now']] ==1):
            return f"高危，分类原因：【0-7 历史数据信息：{information_dict['0-7']},出现 4 次风险等级 且 最大风险风险等级低于“中危” 且 当日风险等级为“低危”】"
        
        if (information_dict['0-7']['出现次数'] ==4) & (np.sum(information_dict['0-7']['风险等级映射'] >=3) >=2) & (information_dict['0-7']['最大风险等级'] >= 3):
            return f"紧急，分类原因：【0-7 历史数据信息：{information_dict['0-7']},出现 4 次风险等级 且 最大风险等级高于“高危”】"
        

        # 9.历史出现风险等级的时间在0-7 天内出现 5次以上不限风险等级，最终风险等级为紧急。
        if (information_dict['0-7']['出现次数'] >= 5) & (np.sum(information_dict['0-7']['风险等级映射'] >=2 ) >=4 ):
            return f"紧急，分类原因：【0-7 历史数据信息：{information_dict['0-7']},出现 5 或多次 风险等级】"
        

        # 10.历史风险等级出现紧急，且当日风险等级低于高危，最终风险等级为高危。
        if (information_dict['0-7']['最大风险等级'] == 4) & (map_dict[row['res_now']] <= 2) & (information_dict['0-7']['出现次数'] >=2):
            return f"高危，分类原因：【0-7 历史数据信息：{information_dict['0-7']},最大风险等级为“紧急” 且 当日风险等级低于“高危”】"
        
        # 11.历史风险等级出现高危或中危，且今日风险等级为低危，最终风险等级为中危。
        if ((information_dict['0-7']['最大风险等级']==2) | (information_dict['0-7']['最大风险等级']==3)) & (information_dict['0-7']['出现次数'] >=2) & (map_dict[row['res_now']] ==1):
            return f"中危，分类原因：【0-7 历史数据信息：{information_dict['0-7']},最大风险等级为“高危”或“低危” 且 当日风险等级为“低危”】"
    

    return f"{row['res_now']}，分类原因：【无匹配规则。】{information_dict}"


def set_marks_by_histroy(data,histroy_data,date:str):
    try:
        logger.logger.info("正在对有历史数据和无历史数据的号码进行分离。。。")
        data_copy=data[['msisdn','final_label_res']].copy()
        data_copy['res']=data_copy['final_label_res']
        data_copy['date_time']=date
        stone_data,flex_data=anaysis_visitors_fea_by_history(data_copy,histroy_data)
    except Exception as e:
        logger.logger.error(f"在对有历史数据和无历史数据进行分离时出现：{e}！！！")

    logger.logger.info(f"当前设置的风险等级分值为：{MAP_DICT}")
    try:
        logger.logger.info("正在对有历史数据的数据的今日结果进行修改中。。。")
        if len(flex_data)!=0:
            flex_data['new_res']=flex_data.apply(lambda da:rule_model(da,date,MAP_DICT),axis=1)
        else:
            flex_data['new_res']=""
            logger.logger.error(f"分离的历史数据存在问题\n{flex_data[:5]}。。。")
    except Exception as e:
        logger.logger.error(f"在对有历史数据的数据的今日结果进行修改时出现{e}！！！")


    
    flex_data['res']=flex_data['res_now']
    # stone_data['new_res']=stone_data['res']
    stone_data['new_res']=stone_data['res']+"，"+"【无历史数据】"
    new_flex_data=flex_data[['msisdn','res','new_res']].copy()
    output=pd.concat([stone_data,new_flex_data],axis=0)
    logger.logger.info(f"对历史数据的今日结果修改完成。。。")
    logger.logger.info(f"部分最终结果数据为：\n{output}")

    return output

def anaylsis_history_main(data,location:str,date:str):
    try:
        logger.logger.info("正在整合历史 7 天的结果数据中。。。")
        history_data=get_history_res(location,date)
    except Exception as e:
        logger.logger.error(f"在整合历史 7 天的结果数据时出现：{e}！！！")

    try:
        logger.logger.info("正在根据历史数据对部分今日结果进行修改。。。")
        comb_data=set_marks_by_histroy(data,history_data,date)
    except Exception as e:
        logger.logger.error(f"在根据历史数据对部分今日结果数据进行修改时出现：{e}！！！")


    logger.logger.info(f"结果数据的分布情况为：\n{comb_data['new_res'].value_counts()}")
    return comb_data

if __name__=="__main__":
    logger.setup_log("test.log","/data/app/logs")
    data=pd.read_csv("/data/app/output/jiangyin/20251203/20251203res.csv",sep="\t",dtype={"msisdn":str,"date_time":str,"res":str,"reason":str})
    anaylsis_history_main(data,"jiangyin","20251203")
    
