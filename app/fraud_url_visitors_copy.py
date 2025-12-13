import pandas as pd 
from datetime import datetime
from functools import reduce
import numpy as np
from config import INPUTPATH,LOGPATH
import os
import logging 



YEAR=datetime.now().year

def process_data_user(data):
    col=[i.split('.')[1] for i in data.columns]
    col[1]='msisdn'
    data.columns=col
    # Use vectorized datetime conversion instead of apply/lambda
    data['年月'] = pd.to_datetime(data['stat_date'], format='%Y%m%d', errors='coerce')

def process_data_time_user(data):
    data['年月'] = pd.to_datetime(data['date'], format='%Y%m%d')

def process_data_mstransit(data):
    data.columns=[i.split('.')[1] for i in data.columns]
    # Use efficient datetime parsing with format specification
    data['通话时间']=pd.to_datetime(data['sdate'], format='%Y%m%d%H%M%S', errors='coerce')

def process_data_note(data):
    # Use vectorized operation instead of string concatenation in loop
    deal_time_str = str(YEAR) + data['deal_time'].astype(str)
    data['操作时间'] = pd.to_datetime(deal_time_str, format='%Y%m%d%H%M%S', errors='coerce')

def process_data_flow(data):
    data.columns=[i.split('.')[1] for i in data.columns]
    # Use efficient datetime parsing
    data['访问时间'] = pd.to_datetime(data['data_time'], errors='coerce')

def process_data_url(data):
    col=[i.split('.')[1] for i in data.columns]
    col[0]='msisdn'
    data.columns=col
    # Use efficient datetime parsing
    data['访问时间'] = pd.to_datetime(data['data_time'], errors='coerce')

def process_data_bank(data):
    data.columns=[i.split('.')[1] for i in data.columns]
    # Use vectorized datetime conversion instead of apply/lambda
    data['交易时间'] = pd.to_datetime(data['cdr_date'], format='%Y%m%d', errors='coerce')

#获取指定时间前7天到指定时间前60天的异常数据 - vectorized version
def filter_needed_time_range(data, fea_time_1, fea_time_2, needed_time_0, needed_time_1):
    """
    data
    fea_time_1   :原数据时间
    fea_time_2   :固定时间时间
    needed_time_0:指定异常数据时间前多少天
    needed_time_1:指定异常数据时间前多少天
    """
    # Calculate time boundaries in a vectorized way
    upper_bound = data[fea_time_2] - pd.Timedelta(days=needed_time_0)
    lower_bound = data[fea_time_2] - pd.Timedelta(days=needed_time_1)
    
    # Create mask for rows within the time range
    mask = (data[fea_time_1] <= upper_bound) & (data[fea_time_1] >= lower_bound)
    return mask
                                      

#获取异常数据 - optimized version
def select_usual_phone(data,fea:str,fea_time_1,fea_time_2,needed_time_0=1,needed_time_1=8):
    """
    data
    fea_time_1   :原数据时间
    fea_time_2   :指定时间
    needed_time_0:指定异常数据时间前多少天
    needed_time_1:指定异常数据时间前多少天
    """
    # Create mask for the time range more efficiently using vectorized operations
    mask = filter_needed_time_range(data, fea_time_1, fea_time_2, needed_time_0, needed_time_1)
    new_data = data.loc[mask].copy()
    
    # 对每个手机号分组，并对某个特征进行计数选出出现次数较少的几个
    if not new_data.empty:
        location_counts = new_data.groupby(['msisdn', fea]).size().reset_index(name='count')
        # 创建结果字典 using vectorized operations instead of loops
        result_dict = location_counts.groupby('msisdn').apply(
            lambda x: x.nsmallest(len(x), 'count')[fea].tolist()
        ).to_dict()
    else:
        result_dict = {}

    return result_dict, mask

def select_unusual_phone_vectorized(df, fea, usual_phone_dict):
    """
    Vectorized version of select_unusual_phone
    """
    # Create a series with the same index as the dataframe
    result = pd.Series(False, index=df.index, dtype=bool)
    
    # Find rows where msisdn is in the dictionary
    mask = df['msisdn'].isin(usual_phone_dict.keys())
    
    if mask.any():
        # Create a series mapping msisdn to their unusual features
        unusual_features = df.loc[mask, 'msisdn'].map(usual_phone_dict)
        
        # Check if the feature is not in the unusual list
        for idx in df.loc[mask].index:
            if df.loc[idx, '指定时间']:  # If '指定时间' is True, skip
                continue
            if df.loc[idx, fea] not in unusual_features.loc[idx]:
                result.loc[idx] = True
    
    return result
    


def find_something_with_hours_vectorized(df1, df2, feature_time, feature_target, hours=1):
    """
    Vectorized version to find if there are matching records within hours range
    """
    # Create time window boundaries for each row in df1
    df1_with_windows = df1[['msisdn', feature_time]].copy()
    df1_with_windows['start_time'] = df1_with_windows[feature_time] - pd.Timedelta(hours=hours)
    df1_with_windows['end_time'] = df1_with_windows[feature_time] + pd.Timedelta(hours=hours)
    
    # Perform merge to find matching records
    # This is more efficient than row-by-row processing
    merged = pd.merge(
        df1_with_windows[['msisdn', 'start_time', 'end_time', feature_time]],
        df2[['msisdn', feature_time, feature_target]],
        on='msisdn',
        how='inner'
    )
    
    # Filter based on time constraints
    matches = merged[
        (merged[feature_time + '_x'] >= merged['start_time']) & 
        (merged[feature_time + '_x'] <= merged['end_time']) & 
        (merged[feature_target])
    ]
    
    # Create a boolean series indicating which rows in df1 have matches
    result = df1.index.isin(matches.index)
    
    return result


def find_something_with_hours_bank_vectorized(df1, df2, feature_time, feature_target):
    """
    Vectorized version for bank data lookup by day
    """
    # Create merge key based on day
    df1_temp = df1[['msisdn', feature_time]].copy()
    df1_temp['day'] = df1_temp[feature_time].dt.day
    
    df2_temp = df2[['msisdn', feature_time, feature_target]].copy()
    df2_temp['day'] = df2_temp[feature_time].dt.day
    
    # Merge on msisdn and day
    merged = pd.merge(
        df1_temp,
        df2_temp,
        on=['msisdn', 'day'],
        how='inner'
    )
    
    # Filter for target feature
    matches = merged[merged[feature_target]]
    
    # Create a boolean series indicating which rows in df1 have matches
    result = df1.index.isin(matches.index)
    
    return result


def process_mutual_features_vectorized(df_url, df_mstransit, df_note, df_bank):
    """
    Vectorized processing of mutual features between URL visits and other data
    """
    # Create new columns with default values
    features = [
        '访问网址时间拨打电话', '访问网址时间接收电话', '访问网址时间接收短信', 
        '访问网址时间发送短信', '访问网址时间银行交易', 
        '访问网址时间短信异常联系人', '访问网址时间话单异常联系人'
    ]
    
    for feature in features:
        df_url[feature] = False
    
    # Process each feature using vectorized operations
    df_url['访问网址时间拨打电话'] = find_something_with_hours_vectorized(
        df_url, df_mstransit, 'time', '话单拨打')
        
    df_url['访问网址时间接收电话'] = find_something_with_hours_vectorized(
        df_url, df_mstransit, 'time', '话单接收')
        
    df_url['访问网址时间接收短信'] = find_something_with_hours_vectorized(
        df_url, df_note, 'time', '短信接收')
        
    df_url['访问网址时间发送短信'] = find_something_with_hours_vectorized(
        df_url, df_note, 'time', '短信发送')
        
    df_url['访问网址时间银行交易'] = find_something_with_hours_bank_vectorized(
        df_url, df_bank, 'time', '银行交易')
        
    df_url['访问网址时间短信异常联系人'] = find_something_with_hours_vectorized(
        df_url, df_note, 'time', '短信异常联系人')
        
    df_url['访问网址时间话单异常联系人'] = find_something_with_hours_vectorized(
        df_url, df_mstransit, 'time', '话单异常联系人')
    
    return df_url


def comb_time(data_mstransit,data_note,data_url,data_bank,needed_time:int=7):
    """
    needed_time      :指定时间前多少天的数据
    data_mstransit   :话单数据
    data_note        :短信数据
    data_flow        :流量数据
    
    """

    # Use direct assignment instead of copy.copy() to be more memory efficient
    data_mstransit_local = data_mstransit
    data_mstransit_local['time'] = data_mstransit_local['通话时间']
    data_mstransit_local['call_type_mstransit'] = data_mstransit_local['call_type']


    data_note_local = data_note
    data_note_local['time'] = data_note_local['操作时间']
    data_note_local['call_type_note'] = data_note_local['call_type']


    data_url_local = data_url  # data_url was originally data_flow in param name
    data_url_local['time'] = data_url_local['访问时间']


    data_bank_local = data_bank
    data_bank_local['time'] = data_bank_local['交易时间']
    data_bank_local['银行交易'] = data_bank_local['time'].notna()  # Simplified boolean assignment


    data_note_local['短信接收'] = data_note_local['call_type_note'] == '1'
    data_note_local['短信发送'] = data_note_local['call_type_note'] == '10'

    data_mstransit_local['话单拨打'] = data_mstransit_local['call_type_mstransit'] == '101'
    data_mstransit_local['话单接收'] = data_mstransit_local['call_type_mstransit'] == '102'


    # 短信异常联系人 - using vectorized method
    req_time_note_phone, data_note_local['指定时间'] = select_usual_phone(data_note_local,fea='other_party',fea_time_1='操作时间',fea_time_2='年月',needed_time_0=1,needed_time_1=8)
    data_note_local['短信异常联系人'] = select_unusual_phone_vectorized(data_note_local, fea='other_party', usual_phone_dict=req_time_note_phone)
    del req_time_note_phone

    # 话单异常联系人 - using vectorized method
    req_time_mstransit_phone, data_mstransit_local['指定时间'] = select_usual_phone(data_mstransit_local,fea='other_party',fea_time_1='通话时间',fea_time_2='年月',needed_time_0=1,needed_time_1=8)
    data_mstransit_local['话单异常联系人'] = select_unusual_phone_vectorized(data_mstransit_local, fea='other_party', usual_phone_dict=req_time_mstransit_phone)
    del req_time_mstransit_phone

    # Initialize features columns
    data_url_local = data_url_local.assign(
        访问网址时间拨打电话=False,
        访问网址时间接收电话=False,
        访问网址时间接收短信=False,
        访问网址时间发送短信=False,
        访问网址时间银行交易=False,
        访问网址时间短信异常联系人=False,
        访问网址时间话单异常联系人=False
    )

    # Process mutual features using vectorized operations
    data_url_local = process_mutual_features_vectorized(
        data_url_local, data_mstransit_local, data_note_local, data_bank_local)

    # Combine dataframes more efficiently
    selected_columns = ['msisdn','访问网址时间拨打电话','访问网址时间接收电话','访问网址时间接收短信','访问网址时间发送短信','访问网址时间银行交易','短信异常联系人','info_length','call_duration','话单异常联系人','访问网址时间话单异常联系人','访问网址时间短信异常联系人']

    # Create a list to store the selected data from each dataframe
    selected_dataframes = []
    
    # Process each dataframe and select relevant columns
    if '访问网址时间拨打电话' in data_url_local.columns:
        url_selected = data_url_local[selected_columns].fillna(0)
        selected_dataframes.append(url_selected)
    
    # For other dataframes, we need to ensure they have the same columns with default values
    if not data_mstransit_local.empty and 'msisdn' in data_mstransit_local.columns:
        # Create a subset with only needed columns, filling missing ones with defaults
        mstransit_selected = data_mstransit_local.reindex(columns=selected_columns, fill_value=False).fillna(0)
        selected_dataframes.append(mstransit_selected)
    
    if not data_note_local.empty and 'msisdn' in data_note_local.columns:
        note_selected = data_note_local.reindex(columns=selected_columns, fill_value=False).fillna(0)
        selected_dataframes.append(note_selected)
    
    if not data_bank_local.empty and 'msisdn' in data_bank_local.columns:
        bank_selected = data_bank_local.reindex(columns=selected_columns, fill_value=False).fillna(0)
        selected_dataframes.append(bank_selected)

    # Concatenate all selected dataframes
    merged = pd.concat(selected_dataframes, axis=0, ignore_index=True, sort=False)
    
    # Perform groupby aggregation
    grouped = merged.groupby('msisdn', as_index=False).agg(
        访问网址时间拨打电话=('访问网址时间拨打电话', 'sum'),
        访问网址时间接收电话=('访问网址时间接收电话', 'sum'),
        访问网址时间接收短信=('访问网址时间接收短信', 'sum'),
        访问网址时间发送短信=('访问网址时间发送短信', 'sum'),
        访问网址时间银行交易=('访问网址时间银行交易', 'sum'),
        短信异常联系人=('短信异常联系人', 'sum'),
        平均短信长度=('info_length', 'mean'),
        最大短信长度=('info_length', 'max'),
        话单异常联系人=('话单异常联系人', 'sum'),
        平均通话长度=('call_duration', 'mean'),
        最大通话长度=('call_duration', 'max'),
        访问网址时间短信异常联系人=('访问网址时间短信异常联系人', 'sum'),
        访问网址时间话单异常联系人=('访问网址时间话单异常联系人', 'sum'),
    )
    
    # Add calculated fields
    grouped['访问网址时间通话'] = grouped['访问网址时间拨打电话'] + grouped['访问网址时间接收电话']
    grouped['访问网址时间短信'] = grouped['访问网址时间接收短信'] + grouped['访问网址时间发送短信']

    return grouped
def feature_main(location : str,date:str ):

    log_path=os.path.join(LOGPATH,date)
    
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    log_name=os.path.join(log_path,"feature_main.log")
    # 设置日志
    logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(log_name,encoding='utf-8'),  # 输出到文件
                logging.StreamHandler()
            ]
        )
    
    location_path=os.path.join(INPUTPATH,location)
    
    location_date_path=os.path.join(location_path,date)

    logging.info(f"正在进行访客特征分析的日期为：{date}...")
    try:
        mstransit_name='全量话单数据.txt'
        logging.info(f"正在读取的文件数据为：{mstransit_name}...")
        mstransit_path=os.path.join(location_date_path,mstransit_name)
        mstransit_type={'t.city_code':str,'t.phone':str,'t.visitor_date':str,'t.msisdn':str,'t.call_type':str,'t.other_party':str,'t.sdate':str,'t.duration60':str,'t.call_duration':int,'t.lac':str,'t.cell_id':str,'t.cdr_date':str}
        data_mstransit=pd.read_csv(mstransit_path,sep="\t",dtype=mstransit_type)
        logging.info(f"文件数据:{mstransit_name},已经读取完成！！！")


        note_name='短信数据.txt'
        logging.info(f"正在读取的文件数据为：{note_name}...")
        note_path=os.path.join(location_date_path,note_name)
        note_type={'msisdn':str,'other_party':str,'call_type':str,'deal_time':str,'finish_t':str,'info_length':int}
        data_note=pd.read_csv(note_path,sep="\t",dtype=note_type)
        logging.info(f"文件数据:{note_name},已经读取完成！！！")

        url_name='访客数据.txt'
        logging.info(f"正在读取的文件数据为：{url_name}...")
        url_path=os.path.join(location_date_path,url_name)
        url_type={'t.phone':str,'t.data_time':str,'t.host':str,'t.city':str}
        data_url=pd.read_csv(url_path,sep="\t",dtype=url_type)
        logging.info(f"文件数据:{url_name},已经读取完成！！！")

        user_name='手机号去重清单.txt'
        logging.info(f"正在读取的文件数据为：{user_name}...")
        user_path=os.path.join(location_date_path,user_name)
        user_type={'t.city_code':str,'t.phone':str,'t.stat_date':str,'t.day_number1':str,'t.day_number2':str}
        data_user=pd.read_csv(user_path,sep="\t",dtype=user_type)
        logging.info(f"文件数据:{user_name},已经读取完成！！！")


        bank_name='app银行类别的明细数据.txt'
        logging.info(f"正在读取的文件数据为：{bank_name}...")
        bank_path=os.path.join(location_date_path,bank_name)
        bank_type={'t.city_code':str,'t.phone':str,'t.visitor_date':str,'t.msisdn':str,'t.imei':str,'t.region_id':str,'t.app_id':str,'t.match_cnt':str,'t.upload':str,'t.download':str,'t.conn_during':str,'t.cnt_rules':str,'t.cdr_date':str,'t.app_name':str}
        data_bank=pd.read_csv(bank_path,sep="\t",dtype=bank_type)
        logging.info(f"文件数据:{bank_name},已经读取完成！！！")

        data_time_user=pd.DataFrame({
            "date":[date]
        })
    except Exception as e:
        logging.error(f"在读取数据过程中出现:{e}！！")
    logging.info(f"文件数据已经全部读取完成！！！")

    logging.info("正在进行数据预处理中。。。")
    try:
        process_data_mstransit(data_mstransit)
        process_data_note(data_note)
        process_data_url(data_url)
        process_data_user(data_user)
        process_data_bank(data_bank)
        process_data_time_user(data_time_user)

        data_mstransit['年月']=data_time_user['年月'][0]
        data_note['年月'] = data_time_user['年月'][0]

    except Exception as e:
        logging.warning(f"在数据预处理中出现{e}！！")

    logging.info("数据预处理已经全部完成！！！")

    del data_time_user
    logging.info("正在进行访客数据特征分析中。。。")
    try:
        print(data_mstransit,data_note,data_url,data_bank)
        grouped_time=comb_time(data_mstransit,data_note,data_url,data_bank)
    except Exception as e:
        logging.warning(f"在进行访客数据特征分析中出现{e}!!!")

    logging.info(f"日期为--{date}--的访客数据特征分析完成！！！ ")
    need_data=grouped_time[["msisdn","访问网址时间拨打电话","访问网址时间接收电话","访问网址时间接收短信","访问网址时间发送短信","访问网址时间银行交易","短信异常联系人","平均短信长度","最大短信长度","话单异常联系人","平均通话长度","最大通话长度","访问网址时间短信异常联系人","访问网址时间话单异常联系人","访问网址时间通话","访问网址时间短信"]]
    
    #是否要进行merge（如果不全是20251031的访客）
    df_unique = data_user[['msisdn']].drop_duplicates(subset='msisdn', keep='first').copy()
    output_data=pd.merge(df_unique,need_data,on='msisdn',how='left')
    output_data.fillna(0,inplace=True)
    
    return output_data


if __name__=="__main__":
    # set INPUTPATH for test
    data=feature_main("nantong","20251031")
    print(data)
    data.to_excel("ffjf.xlsx")
# 地点 nantong，kunshan
# 日期  20251107
# from datetime import datetime

    # main(location = 'nantong',date = "20251026" )

# res 
# 号码，时间，紧急程度，备注