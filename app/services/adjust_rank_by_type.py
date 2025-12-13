import pandas  as pd
import os
from config import INPUTPATH,TYPE_SET,RISK_TYPE_SET

from core.global_logger import logger
from collections import Counter

def get_main_type(host_type_list):
    counter=Counter(host_type_list)
    main_type=counter.most_common(1)[0][0]
    return pd.Series([main_type, counter[main_type],counter])

def load_process_url_data(location:str,date:str) -> pd.DataFrame:
    """加载并处理网址类型数据"""
    filepath=os.path.join(os.path.join(INPUTPATH,location),date)
    filepath=os.path.join(filepath,"访客数据.txt")
    logger.logger.info(f"正在读取访客数据，其文件路径为：{filepath}。。。")
    url_type={"phone":str,"date_time":str,"host":str,"host_type":str}
    data=pd.read_csv(filepath,dtype=str,sep="\t")
    logger.logger.info(f"访客数据读取完成，其网址类型计数为：\n{data['host_type'].value_counts()}")
    data['msisdn']=data['phone']
    logger.logger.info(f"正在根据 msisdn 分组聚合网址类型。。。")
    grouped_type=data.groupby('msisdn').agg(
        访问网址类型列表=('host_type',list),
        访问网址类型集合=('host_type',set)
        ).reset_index()
    logger.logger.info(f"根据 msisdn 分组聚合网址类型完成。。。")
    
    try:
        logger.logger.info(f"正在根据 网址类型列表 进行网址统计中。。。")
        grouped_type[['访问网址主要类型','访问网址主要类型次数','访问网址类型计数表']]=grouped_type['访问网址类型列表'].apply(get_main_type)
    except Exception as e:
        logger.logger.error(f"在根据 网址类型列表 进行网址统计时出现：{e}！！！")
    
    logger.logger.info(f"网址类型统计完成其部分数据为：\n{grouped_type[:5]}。。。")
    return grouped_type

def adjust_rank_logic(row,type_set,risk_set) -> int:
    """根据网址类型调整风险等级的逻辑"""
    if (row['访问网址类型计数表']=="") | (row['访问网址类型集合']==""):
        return row['final_label_res']+"，分类原因：【无网址类型】"
    
    
    if row['final_label_res']=="紧急":
        if not set({'色情博彩引流','裸聊诈骗','招嫖诈骗','博彩诈骗'}).isdisjoint(row['访问网址类型集合']):
            if row['访问网址时间银行交易'] <= 4:
                return f"高危，分类原因：【含有'色情博彩引流','裸聊诈骗','招嫖诈骗','博彩诈骗'的一种 且 访问网址时间银行交易次数（<=4）：{row['访问网址时间银行交易']}】"


    if row['final_label_res']=="高危":
        if not row['访问网址类型集合'].isdisjoint(risk_set):
            if row['访问网址时间银行交易'] >= 2:
                return f"紧急，分类原因：【含有'虚假服务诈骗,'公检法','冒充电商客服诈骗','虚假购物诈骗','仿冒政府网站诈骗','刷单诈骗','客服系统','理财诈骗','贷款诈骗'中的一种 且 访问网址时间银行交易次数（>=2）：{row['访问网址时间银行交易']}】"
        if (row['访问网址类型计数表']['理财诈骗'] > 5) & (row['访问网址时间银行交易'] >= 1):

            return f"紧急，分类原因：多次访问【理财诈骗】类型的涉诈网址（次数>5）：{row['访问网址类型计数表']['理财诈骗']} 且 访问网址时间银行交易次数（<=1）：{row['访问网址时间银行交易']}】。"


        if (row['访问网址类型计数表']['贷款诈骗'] > 5)& (row['访问网址时间银行交易'] >= 1):
            
            return f"紧急，分类原因：多次访问【贷款诈骗】类型的涉诈网址（次数>5）：{row['访问网址类型计数表']['贷款诈骗']} 且 访问网址时间银行交易次数（<=1）：{row['访问网址时间银行交易']}】。"
        
        if (row['访问网址类型计数表']['刷单诈骗'] > 5)& (row['访问网址时间银行交易'] >= 1):
            
            return f"紧急，分类原因：多次访问【刷单诈骗】类型的涉诈网址（次数>5）：{row['访问网址类型计数表']['刷单诈骗']} 且 访问网址时间银行交易次数（<=1）：{row['访问网址时间银行交易']}】。"


        if (row['访问网址类型计数表']['冒充电商客服诈骗'] > 5)& (row['访问网址时间银行交易'] >= 1):
            
            return f"紧急，分类原因：多次访问【冒充电商客服诈骗】类型的涉诈网址（次数>5）：{row['访问网址类型计数表']['冒充电商客服诈骗']} 且 访问网址时间银行交易次数（<=1）：{row['访问网址时间银行交易']}】。"


    if row['final_label_res']=="中危":
        if not row['访问网址类型集合'].isdisjoint(risk_set):
            if row['访问网址时间银行交易'] >= 2:
                return f"高危，分类原因：【含有'虚假服务诈骗,'公检法','冒充电商客服诈骗','虚假购物诈骗','仿冒政府网站诈骗','刷单诈骗','客服系统','理财诈骗','贷款诈骗'中的一种 且 访问网址时间银行交易次数（>=2）：{row['访问网址时间银行交易']}】"


    return row['final_label_res']+'，分类原因：【无调整】'

def adjust_rank_by_type(total_data,location:str,date:str) -> pd.DataFrame:
    """根据不同的网址类型，调整访客的风险等级

    Returns:
        pd.DataFrame: 调整后的访客数据DataFrame
    """
    try:
        logger.logger.info(f"正在加载并处理地点为--{location}--，日期为--{date}--的网址类型数据。。。")
        url_data=load_process_url_data(location,date)
    except Exception as e:
        logger.logger.error(f"加载并处理地点为--{location}--，日期为--{date}--的网址类型数据时出现{e}！！！")
        return total_data['final_label_res']
    total_data_copy=total_data[['msisdn','访问网址时间银行交易','final_label_res']].copy()
    total_data_copy=total_data_copy.merge(url_data,on="msisdn",how="left")
    print(total_data_copy[:5])
    # print(fff)
    total_data_copy['访问网址类型计数表']=total_data_copy['访问网址类型计数表'].fillna("")
    try:
        logger.logger.info(f"正在根据网址类型调整地点为--{location}--,日期为--{date}--的访客风险等级。。。")
        total_data_copy['adjusted_res']=total_data_copy.apply(lambda row: adjust_rank_logic(row,TYPE_SET,RISK_TYPE_SET),axis=1)
    except Exception as e:
        logger.logger.error(f"根据网址类型调整地点为--{location}--，日期为--{date}--的访客风险等级时出现{e}！！！")
        return total_data['final_label_res']
    logger.logger.info(f"根据网址类型进行访客风险等级更改已经完成。。。")
    logger.logger.info(f"部分调整结果预览：\n{total_data_copy[:5]}")


    return  total_data_copy['adjusted_res'],total_data_copy['访问网址主要类型'],total_data_copy['访问网址主要类型次数'],total_data_copy['访问网址类型计数表']

if __name__ == "__main__":  
    def split_model_res(data,model_res_feature):
        new_cols = data[model_res_feature].str.split('，', expand=True)
        new_cols.columns = [f"{model_res_feature}_res", f"{model_res_feature}_reason"]
        data = pd.concat([data, new_cols], axis=1)
        data = data.drop(model_res_feature, axis=1)
        return data
    
    logger.setup_log("test.log","/data/app/logs/20251211")


    # data=pd.read_csv(r'/data/app/output/nantong/20251202/20251202feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    # data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    # data=split_model_res(data,'host_type_label')
    # data.to_csv(r'/data/app/output/nantong/20251202/20251202res.csv',index=None)

    # data=pd.read_csv(r'/data/app/output/nantong/20251203/20251203feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    # data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    # data=split_model_res(data,'host_type_label')
    # data.to_csv(r'/data/app/output/nantong/20251203/20251203res.csv',index=None)


    # data=pd.read_csv(r'/data/app/output/nantong/20251204/20251204feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    # data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    # data=split_model_res(data,'host_type_label')
    # data.to_csv(r'/data/app/output/nantong/20251204/20251204res.csv',index=None)


    # data=pd.read_csv(r'/data/app/output/nantong/20251205/20251205feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    # data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    # data=split_model_res(data,'host_type_label')
    # data.to_csv(r'/data/app/output/nantong/20251205/20251205res.csv',index=None)

    # data=pd.read_csv(r'/data/app/output/nantong/20251207/20251207feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    # data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    # data=split_model_res(data,'host_type_label')
    # data.to_csv(r'/data/app/output/nantong/20251207/20251207res.csv',index=None)


    # data=pd.read_csv(r'/data/app/output/nantong/20251208/20251208feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    # data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    # data=split_model_res(data,'host_type_label')
    # data.to_csv(r'/data/app/output/nantong/20251208/20251208res.csv',index=None)


    data=pd.read_csv(r'/data/app/output/nantong/20251209/20251209feature_and_model_data.csv',sep="\t",dtype={"msisdn":str,"访问网址时间银行交易":int})
    data['host_type_label'],data['访问网址主要类型'],data['访问网址主要类型次数'],data['访问网址类型计数表']=adjust_rank_by_type(data,"nantong","20251209")
    print(data.columns)
    data=split_model_res(data,'host_type_label')
    print(data.columns)
    data_=data[['msisdn','final_label_res','host_type_label_res','host_type_label_reason']].copy()

    print(data.columns)
    data_.to_csv(r'/data/app/test_data/data/20251209res.csv',index=None)


