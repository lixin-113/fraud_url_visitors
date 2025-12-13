import pandas as pd
import pickle
import copy
from config import OUTPUTPATH,RF_MODEL_PATH,LOGPATH
from core.global_logger import logger
import os 
from .anaylsis_visitors import anaylsis_history_main
from .adjust_rank_by_type import adjust_rank_by_type
import time

def model_set_marks(proba):
    if proba >= 0.92:
        return f"紧急，随机森林预测类型：紧急,随机森林预测概率：{proba}"
    if (proba < 0.92) & (proba > 0.8):
        return f"高危，随机森林预测类型：高危,随机森林预测概率：{proba}"
    if (proba <= 0.8) & (proba > 0.5):
         return f"中危，随机森林预测类型：中危,随机森林预测概率：{proba}"
    return f"低危，随机森林预测类型：低危,随机森林预测概率：{proba}"



def rule_set_label(row):

    if (row['访问网址时间银行交易'] >= 2) & \
            ((row['访问网址时间话单异常联系人'] >= 2) | (row['访问网址时间短信异常联系人'] >= 2)) & \
            ((row['访问网址时间短信'] >= 1) | (row['访问网址时间通话'] >=1 )):

            return f"紧急，规则模型分类结果：紧急,分类原因：【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 （访问网址时间当天话单异常联系人（1-7）访问次数(>=2)：{row['访问网址时间话单异常联系人']} 或 访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']}） 且 访问网址前后1小时有接收电话的访问网址次数(>=1)：{row['访问网址时间接收电话']}】"

    if (row['访问网址时间银行交易'] >= 2)  & (row['访问网址时间短信']>=2):
        return f"紧急，规则模型分类结果：紧急,分类原因：【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 访问网址时间前后1小时有短信通联的访问网址次数(>=2)：{row['访问网址时间短信']}】"
    

    if (row['访问网址时间银行交易'] >=1 ) & (row['访问网址时间通话'] > 0 ) & ((row['访问网址时间接收电话']==row['访问网址时间话单异常联系人']) | (row['访问网址时间拨打电话']==row['访问网址时间话单异常联系人']) & (row['访问网址时间拨打电话']+row['访问网址时间接收电话']==row['访问网址时间话单异常联系人'])):
        return f"紧急，规则模型分类结果：紧急,分类原因：【访问网址时间话单拨打或发送都是异常联系人】"
    
    if (row['访问网址时间银行交易'] >= 1) &(row['访问网址时间短信'] > 0 ) & ((row['访问网址时间接收短信']==row['访问网址时间短信异常联系人']) | (row['访问网址时间接收短信']==row['访问网址时间短信异常联系人']) & (row['访问网址时间接收短信']+row['访问网址时间发送短信']==row['访问网址时间短信异常联系人'])):
        return f"紧急，规则模型分类结果：紧急,分类原因：【访问网址时间话单拨打或发送都是异常联系人】"
    
    if (row['访问网址时间短信异常联系人'] >= 2 ) & ( row['访问网址时间话单异常联系人'] >= 4) & (row['访问网址时间拨打电话'] >= 2):
        return f"高危，规则模型分类结果：高危,分类原因：【访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']} 且 访问网址时间当天与话单异常联系人（1-7）通联的访问网址次数(>=4)：{row['访问网址时间话单异常联系人']} 且 访问网址时间前后1小时有接收短信的访问网址次数(>=2)：{row['访问网址时间短信']}】" 
    
    # if (row['访问网址时间银行交易'] >= 1)  &  (row['话单异常联系人'] >= 2):
    #         return "高危"
    

    if (row['访问网址时间短信异常联系人'] > 0) & (row['访问网址时间话单异常联系人'] > 2):
        return f"中危，规则分类结果：中危,分类原因：【访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>0)：{row['访问网址时间短信异常联系人']} 且 访问网址时间当天与话单异常联系人（1-7）通联的访问网址次数(>2)：{row['访问网址时间话单异常联系人']}】"
         
    if (row['访问网址时间短信'] > 5 ) | (row['访问网址时间通话'] > 3):
            return f"中危，规则分类结果：中危,分类原因：【访问网址时间前后1小时有短信通联的访问网址次数(>5)：{row['访问网址时间短信']} 或 访问网址时间前后1小时有话单通联的访问网址次数(>3)：{row['访问网址时间通话']}】"
    
    # if (row['访问网址时间银行交易'] >=25 ):
    #     return f"紧急，规则分类结果：紧急,分类原因：【访问网址时间当天访问银行的访问网址次数(>=30)：{row['访问网址时间银行交易']}】"
    

    if (row['访问网址时间银行交易'] >=15 ):
        return f"高危，规则分类结果：高危,分类原因：【访问网址时间当天访问银行的访问网址次数(>=15)：{row['访问网址时间银行交易']}】"


    if (row['访问网址时间银行交易'] >=5 ):
        return f"中危，规则分类结果：中危,分类原因：【访问网址时间当天访问银行的访问网址次数(>=5)：{row['访问网址时间银行交易']}】"
    
    if row['访问网址时间短信异常联系人'] >= 40:
        return f"紧急，规则分类结果：紧急,分类原因：【访问网址时间短信异常联系人(>=40)：{row['访问网址时间短信异常联系人']}】"

    return f"低危，规则分类结果：低危,分类原因：【无】"



def set_marks_by_model_wxa(row):
    
    # if row['rf_fraud_label']=="紧急":
    #     if ((row['与AI预警通联']+row['与案件号码通联']+row['与高危预警号码通联']) >=3) & ((row['rule_label']=="高危") | (row['rule_label']=="紧急")):
             
    #         return "紧急"
    if row['rf_fraud_label_res']=="紧急":
        if row['rule_label_res']=="紧急": 
            return f"紧急，分类原因：【随机森林模型分类为“紧急” 且 规则模型输出为“紧急”,最终分类结果为“紧急”。其中随机森林的预测概率为：{row['rf_res']},规则模型输出为紧急的情况为：紧急：1.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 （访问网址时间当天话单异常联系人（1-7）访问次数(>=2)：{row['访问网址时间话单异常联系人']} 或 访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']}） 且 访问网址前后1小时有接收电话的访问网址次数(>=1)：{row['访问网址时间接收电话']}】,2.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 访问网址时间前后1小时有短信通联的访问网址次数(>=2)：{row['访问网址时间短信']}】】"
        

    if (row['与AI预警通联'] >= 1) & (row['与案件号码通联']>=1) & (row['与高危预警号码通联']>=1) :
        if  row['rule_label_res']=='紧急':
            return f"紧急，分类原因：【在设置时间内访客与AI预警通联、与案件号码通联和与高危预警号码通联的次数都大于等于1,最终分类结果为“紧急”。】" 

    if row['与AI预警通联'] >= 1:
        if ((row['rf_fraud_label_res']=="高危") | (row['rf_fraud_label_res']=="紧急")) & ((row['rule_label_res']=="高危") | (row['rule_label_res']=='紧急')):
            return f"紧急，分类原因：【在设置时间内访客与AI预警通联的次数大于等于1 且 随机森林模型分类为“高危”或紧急 且 规则模型分类为“高危”或“紧急”,最终分类结果为“紧急”。其中随机森林的预测概率为：{row['rf_res']},规则模型输出为紧急的情况为：紧急：1.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 （访问网址时间当天话单异常联系人（1-7）访问次数(>=2)：{row['访问网址时间话单异常联系人']} 或 访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']}） 且 访问网址前后1小时有接收电话的访问网址次数(>=1)：{row['访问网址时间接收电话']}】,2.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 访问网址时间前后1小时有短信通联的访问网址次数(>=2)：{row['访问网址时间短信']}】 高危：【访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']} 且 访问网址时间当天与话单异常联系人（1-7）通联的访问网址次数(>=4)：{row['访问网址时间话单异常联系人']} 且 访问网址时间前后1小时有接收短信的访问网址次数(>=2)：{row['访问网址时间短信']}】】"
        # 。。。
        return f"紧急，分类原因：【在设置时间内访客与AI预警通联的次数(>=1)：{row['与AI预警通联']},最终分类结果为“紧急”。】"
            

    if row['与案件号码通联'] >= 1:

        return f"紧急，分类原因：【在设置时间内访客与案件号码通联的次数(>=1)：{row['与案件号码通联']},最终分类结果为“紧急”】"

    if row['与高危预警号码通联'] >= 1:
         
        return f"紧急，分类原因：【在设置时间内访客与高危预警号码通联的次数(>=1)：{row['与高危预警号码通联'] >= 1},最终分类结果为“紧急”】"

    if row['rf_fraud_label_res'] == "紧急":
         
        if row['rule_label_res'] == "紧急":
            return f"紧急，分类原因：【随机森林模型分类为“紧急” 且 规则模型分类为“紧急”,最终分类结果为“紧急”。其中随机森林的预测概率为：{row['rf_res']},其中规则模型输出为紧急的情况为：紧急：1.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 （访问网址时间当天话单异常联系人（1-7）访问次数(>=2)：{row['访问网址时间话单异常联系人']} 或 访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']}） 且 访问网址前后1小时有接收电话的访问网址次数(>=1)：{row['访问网址时间接收电话']}】,2.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 访问网址时间前后1小时有短信通联的访问网址次数(>=2)：{row['访问网址时间短信']}】。】"

        return f"高危，分类原因：【随机森林模型分类为“紧急”,最终分类结果为“高危”。其中随机森林的预测概率为：{row['rf_res']}。】"
    
    if row['rf_fraud_label_res'] == "高危":

        if row['rule_label_res'] == "低危":
            return f"中危，分类原因：【随机森林模型分类为“高危” 且 规则模型分类为“低危”,最终分类结果为“中危”。其中随机森林的预测概率为：{row['rf_res']}。其中规则模型输出为低危的情况为：无。】" 
        
        if (row['rule_label_res']=="紧急") :
            return f"高危，分类原因：【规则模型分类为“紧急“,最终分类结果为“高危”。其中随机森林的预测概率为：{row['rf_res']},其中规则模型输出为紧急和高危的情况为：紧急：1.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 （访问网址时间当天话单异常联系人（1-7）访问次数(>=2)：{row['访问网址时间话单异常联系人']} 或 访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']}） 且 访问网址前后1小时有接收电话的访问网址次数(>=1)：{row['访问网址时间接收电话']}】,2.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 访问网址时间前后1小时有短信通联的访问网址次数(>=2)：{row['访问网址时间短信']}】。】"
        
        return f"高危，分类原因：【随机森林模型分类为“高危”,最终分类结果为高危。其中随机森林的预测概率为：{row['rf_res']}。】"

    if row['rf_fraud_label_res'] == "中危":
        if  (row['rule_label_res'] == "紧急"):
            return f"高危，分类原因：【随机森林模型为分类“中危” 且 规则模型输出为“紧急”,最终分类结果为高危。其中随机森林的预测概率为：{row['rf_res']},其中规则模型为紧急的情况为：紧急：1.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 （访问网址时间当天话单异常联系人（1-7）访问次数(>=2)：{row['访问网址时间话单异常联系人']} 或 访问网址时间当天与短信异常联系人（1-7）通联的访问网址次数(>=2)：{row['访问网址时间短信异常联系人']}） 且 访问网址前后1小时有接收电话的访问网址次数(>=1)：{row['访问网址时间接收电话']}】,2.【访问网址时间当天访问银行的访问网址次数(>=2)：{row['访问网址时间银行交易']} 且 访问网址时间前后1小时有短信通联的访问网址次数(>=2)：{row['访问网址时间短信']}】】。"
        return f"中危，分类原因：【随机森林模型分类为“中危”,最终分类结果为中危。其中随机森林的预测概率为：{row['rf_res']}。】"
    
    return f"低危，分类原因：【无。】"


def split_model_res(data,model_res_feature):

    # 使用str.split方法拆分列
    new_cols = data[model_res_feature].str.split('，', expand=True)
    # print(new_cols)
    
    new_cols.columns = [f"{model_res_feature}_res", f"{model_res_feature}_reason"]

    # 将新拆分的列添加到原始数据框
    data = pd.concat([data, new_cols], axis=1)

    data = data.drop(model_res_feature, axis=1)
    # print(data.columns)

    return data



def model_classify_main(data:pd.DataFrame,location:str,date:str,output_name:str):

    data = data.replace({'True': 1, 'False': 0})

    data.fillna(0,inplace=True)
    data = data.astype({
                '访问网址时间拨打电话':int,'访问网址时间接收电话':int,'访问网址时间接收短信':int,'访问网址时间发送短信':int,'访问网址时间银行交易':int,'短信异常联系人':int,'平均短信长度':int,'最大短信长度':int,'话单异常联系人':int,'平均通话长度':int,'最大通话长度':int,'访问网址时间短信异常联系人':int,'访问网址时间话单异常联系人':int,'访问网址时间通话':int,'访问网址时间短信':int
        })
    data_copy = copy.copy(data)
    data_pre = data_copy[['msisdn','访问网址时间拨打电话','访问网址时间接收电话','访问网址时间接收短信','访问网址时间发送短信','访问网址时间银行交易','短信异常联系人','话单异常联系人','访问网址时间短信异常联系人','访问网址时间话单异常联系人','访问网址时间通话','访问网址时间短信']]
    model_datasets = data_pre.iloc[:,1:]
    
    try:
        logger.logger.info("正在使用随机森林模型对数据集进行预测")
        with open(RF_MODEL_PATH, 'rb') as f:
            rf_model = pickle.load(f)
        rf_model_res=rf_model.predict_proba(model_datasets)
        rf_fraud_p=[i[1] for i in rf_model_res]
        data_copy['rf_res']=rf_fraud_p
    except Exception as e:
        logger.logger.error(f"在使用随机森林模型对数据集进行预测时出现：{e} ！！！")
    
    logger.logger.info("正在进行使用随机森林模型预测概率值进行访客分级。。。")

    try:
        data_copy['rf_fraud_label'] = data_copy['rf_res'].apply(model_set_marks)
    except Exception as e:
        logger.logger.error(f"在使用随机森林模型预测概率值进行访客分级时出现：{e}！！！")
    
    logger.logger.info("正在进行使用规则模型进行访客分级。。。")
    try:
        data_copy['rule_label'] = data_copy.apply(lambda row :rule_set_label(row),axis=1)
    except Exception as e:
        logger.logger.error(f"在使用规则模型进行访客分级时出现：{e}！！！","error")
    logger.logger.info("正在进行使用综合规则模型进行访客分级。。。")



    # 对组合进行分解rf_fraud_label_res，rf_fraud_label_reason,rule_label_res,rule_label_reason
    data_copy=split_model_res(data_copy,'rf_fraud_label')
    # print(data_copy.columns)
    # print(data_copy[['msisdn','rf_fraud_label_res','rf_fraud_label_reason']])
    # print(data_copy['rf_fraud_label_res'].value_counts())
    # print(data_copy['rf_fraud_label_reason'])

    data_copy=split_model_res(data_copy,'rule_label')



    logger.logger.info("正在使用综合规则模型进行访客分级中。。。")
    try:
        data_copy['final_label'] = data_copy.apply(lambda row :set_marks_by_model_wxa(row),axis=1)
    except Exception as e:
        logger.logger.error(f'在进行使用综合规则模型进行访客分级时出现{e}！！！')
    print(data_copy['与AI预警通联'].value_counts())
    # 对组合进行分解final_label_res,final_label_reason
    data_copy=split_model_res(data_copy,'final_label')

    # 对当前结果添加历史数据分析，更改有历史数据的访客的风险等级
    logger.logger.info("正在进行历史数据分析，对有历史数据的访客进行风险等级调整。。。")
    tmp_data=anaylsis_history_main(data_copy,location,date)
    tmp_data['history_label']=tmp_data['new_res']
    tmp_data=split_model_res(tmp_data,'history_label')
    data_copy=pd.merge(data_copy,tmp_data[['msisdn','history_label_res','history_label_reason']],on='msisdn',how='left')
    logger.logger.info("历史数据分析完成。。。")

    # 根据涉诈网址类型调整风险等级
    logger.logger.info("正在根据涉诈网址类型调整访客风险等级中。。。")
    try:
        data_copy['host_type_label'],data_copy['访问网址主要类型'],data_copy['访问网址主要类型次数'],data_copy['访问网址类型计数表']=adjust_rank_by_type(data_copy,location,date)
        data_copy=split_model_res(data_copy,'host_type_label')
        logger.logger.info("根据涉诈网址类型调整访客风险等级已经完成。。。")
    except Exception as e:
        logger.logger.error(f"在根据涉诈网址类型调整访客风险等级时出现：{e}！！！","error")

    feature_and_model_data=data_copy[['msisdn',"访问网址时间拨打电话","访问网址时间接收电话","访问网址时间接收短信","访问网址时间发送短信","访问网址时间银行交易","短信异常联系人","平均短信长度","最大短信长度","话单异常联系人","平均通话长度","最大通话长度","访问网址时间短信异常联系人","访问网址时间话单异常联系人","访问网址时间通话","访问网址时间短信",'final_label_res','final_label_reason','rf_res','rf_fraud_label_res','rf_fraud_label_reason','rule_label_res','rule_label_reason','history_label_res','history_label_reason','host_type_label_res','host_type_label_reason']].copy()
    

    try:
        output_data=data_copy[['msisdn','host_type_label_res','final_label_reason','host_type_label_reason']].copy()
        output_data['date_time']=date
        output_data['res']=output_data['host_type_label_res']
        output_data['reason']=output_data['final_label_reason']+','+output_data['host_type_label_reason']
        output_data=output_data[['msisdn','date_time','res','reason']]
        # print(output_data)
        # print(data_copy['rf_res'])
        location_path=os.path.join(OUTPUTPATH,location)
        location_location_path=os.path.join(location_path,date)
        if not os.path.exists(location_location_path):
            os.makedirs(location_location_path)

        # output_name="模型风险分级_final_nnn.xlsx"
        output_path=os.path.join(location_location_path,output_name)
        logger.logger.info(f"正在导出涉诈网址访客风险等级分级，其路径为{output_path}")
        output_data.to_csv(output_path,index=False,sep="\t")
        logger.logger.info(f"导出的涉诈网址访客风险等级分级结果部分为\n {output_data[:5]}")
        logger.logger.info(f"导出的涉诈网址访客风险等级分级结果计数为\n {output_data['res'].value_counts()}")

        # 输出老版的风险等级分级结果数据
        # output_old_data=data_copy[['msisdn','final_label_res','final_label_reason']].copy()
        # output_old_data['date_time']=date
        # output_path_old=os.path.join(location_location_path,f"old{date}.csv")
        # logger.logger.info(f"正在导出老版涉诈网址访客特征和模型分类数据，其路径为{output_path_old}")
        # output_old_data.to_csv(output_path_old,index=False,sep="\t")
        # logger.logger.info(f"导出的老版涉诈网址访客风特征和模型分类数据\n {output_old_data[:5]}")


        # 输出特征信息数据
        output_feature_path=os.path.join(location_location_path,f"{date}feature_and_model_data.csv")
        logger.logger.info(f"正在导出涉诈网址访客特征和模型分类数据，其路径为{output_feature_path}")
        feature_and_model_data.to_csv(output_feature_path,index=False,sep="\t")
        logger.logger.info(f"导出的涉诈网址访客风特征和模型分类数据\n {feature_and_model_data[:5]}")

    except Exception as e:
        logger.logger.error(f"在导出涉诈网址访客风险等级分级时出现：{e}！！！","error")
    logger.logger.info(f"日期为--{date}--的数据风险等级分级已经完成！！！")

    return 


if __name__=="__main__":
    logger.setup_log("test.log","/data/app/logs/20251031")
    data=pd.read_csv('/data/1123.csv',sep="\t",dtype={'msisdn':str})
    data=data[["msisdn","访问网址时间拨打电话","访问网址时间接收电话","访问网址时间接收短信","访问网址时间发送短信","访问网址时间银行交易","短信异常联系人","平均短信长度","最大短信长度","话单异常联系人","平均通话长度","最大通话长度","访问网址时间短信异常联系人","访问网址时间话单异常联系人","访问网址时间通话","访问网址时间短信"]]
    data["与AI预警通联"]=0
    data["与案件号码通联"]=0
    data["与高危预警号码通联"]=0
    data=model_classify_main(data,"nantong","20251031","ffffff.csv")

    # time.sleep(90)