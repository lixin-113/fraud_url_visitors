import pandas as pd 
from datetime import datetime
import copy
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
    data['年月'] = pd.to_datetime(data['stat_date'].astype(str).apply(lambda text:datetime.strptime(text,"%Y%m%d")))

    return 

def process_data_time_user(data):

    data['年月'] = pd.to_datetime(data['date'], format='%Y%m%d')

    return 

def process_data_mstransit(data):

    data.columns=[i.split('.')[1] for i in data.columns]

    data['通话时间']=pd.to_datetime(data['sdate'])

    return


def process_data_note(data):
    data['操作时间'] = pd.to_datetime(f"{YEAR}"+data['deal_time'],format='%Y%m%d%H%M%S')

    return 


def process_data_flow(data):
    data.columns=[i.split('.')[1] for i in data.columns]
    data['访问时间'] = pd.to_datetime(data['data_time'])
    return


def process_data_url(data):
    col=[i.split('.')[1] for i in data.columns]
    col[0]='msisdn'
    data.columns=col
    data['访问时间'] = pd.to_datetime(data['data_time'])
    # print(data['访问时间'].dt.hour)
    return


def process_data_bank(data):
    data.columns=[i.split('.')[1] for i in data.columns]
    data['交易时间'] = pd.to_datetime(data['cdr_date'],format="%Y%m%d")

    return

#获取指定时间前多少天的数据
def filter_needed_time(data,fea_time_1,fea_time_2,needed_time):
    """
    data
    needed_time  :指定时间的前多少天
    fea_time_1   :原数据时间
    fea_time_2   :指定时间

    """


    needed_time_ago=data[fea_time_2]-pd.Timedelta(days=needed_time)

    if (data[fea_time_1] > needed_time_ago) & (data[fea_time_1] < data[fea_time_2]):
        return True
    else:
        return False
    
#获取指定时间前7天到指定时间前60天的异常数据
def filter_needed_time_other(row,fea_time_1,fea_time_2,needed_time_0,needed_time_1):

    """
    data
    fea_time_1   :原数据时间
    fea_time_2   :固定时间时间
    needed_time_0:指定异常数据时间前多少天
    needed_time_1:指定异常数据时间前多少天
    
    """
    needed_time_ago=row[fea_time_2]-pd.Timedelta(days=needed_time_0)
    try:
        needed_time_ago=needed_time_ago.strftime("%Y%m%d")
    except:
        print(row)
    needed_time_ago_2=row[fea_time_2]-pd.Timedelta(days=needed_time_1)
    needed_time_ago_2=needed_time_ago_2.strftime("%Y%m%d")



    if (row[fea_time_1].strftime("%Y%m%d") <= needed_time_ago) & (row[fea_time_1].strftime("%Y%m%d") >= needed_time_ago_2):
        return True
    else:
        return False
                                      

#获取异常数据
def select_usual_phone(data,fea:str,fea_time_1,fea_time_2,needed_time_0=1,needed_time_1=8):
    """
    data
    fea_time_1   :原数据时间
    fea_time_2   :指定时间
    needed_time_0:指定异常数据时间前多少天
    needed_time_1:指定异常数据时间前多少天
    
    """
    data_copy=copy.copy(data)
    m=data_copy.apply(lambda da:filter_needed_time_other(da,fea_time_1,fea_time_2,needed_time_0=needed_time_0,needed_time_1=needed_time_1),axis=1)
    new_data=copy.copy(data_copy[m])
    # 对每个手机号分组，并对某个特征进行计数选出出现次数较少的几个
    location_counts = new_data.groupby(['msisdn', fea]).size().reset_index(name='count')
    # 创建结果字典
    result_dict = {}

    # 遍历每个手机号
    for phone, group in location_counts.groupby('msisdn'):
        top_n_least = group.sort_values(by='count')[fea].tolist()
        result_dict[phone] = top_n_least
    # print(result_dict)
    return result_dict,m

def select_unusual_phone(row,fea,usual_phone):
    if row['msisdn'] not in usual_phone:
        return False

    if row['指定时间']:
        return False
    
    temp=usual_phone[row['msisdn']]

    return row[fea] not in temp  
    


#时间线
def find_something_with_hours(row, df2,  feature_time:str, feature_target:str,hours:int=1):
    start_time = row[feature_time] - pd.Timedelta(hours=hours)
    end_time = row[feature_time] + pd.Timedelta(hours=hours)
    # 在data数据中筛选出相同手机号且时间在几个小时内的feature_target特征数据
    locations = df2[(df2['msisdn'] == row['msisdn']) & 
                    (df2[feature_time] >= start_time) & 
                    (df2[feature_time] <= end_time) & df2[feature_target]][feature_target].tolist()
    if locations:
        # return len(locations)
        return True
    return False


def find_something_with_hours_bank(row, df2,  feature_time:str, feature_target:str,hours:int=1):
    url_day=int(row['time'].day)
    # print(url_day)
    # print(type(url_day))

    url_msisdn=np.int64(row['msisdn'])

    # print(df2[feature_time].dt.day.dtype)

    # 在data数据中筛选出相同手机号且时间在几个小时内的feature_target特征数据
    locations = df2[(df2['msisdn'] == url_msisdn) & (df2[feature_time].dt.day == url_day)][feature_target].tolist()
    
    if locations:

        return True
    
    return False

def fully_vectorized_solution(df1, df2, target_fea,phone_fea='msisdn',df1_time='time',df2_time='time',hours=1):
    """
    完全向量化的解决方案，性能最佳
    """
    # 预处理数据
    web_times = df1[df1_time].values
    web_phones = df1[phone_fea].values
    
    bank_times = df2[df2_time].values
    bank_phones = df2[phone_fea].values
    bank_flags = df2[target_fea].values
    
    # 只保留有效的银行交易
    valid_mask = bank_flags
    bank_times_valid = bank_times[valid_mask]
    bank_phones_valid = bank_phones[valid_mask]
    
    # 创建结果数组
    result = np.zeros(len(df1), dtype=bool)
    
    # 按手机号码分组处理
    unique_phones = np.unique(web_phones)
    
    for phone in unique_phones:
        # 找到该手机的所有网站访问
        web_mask = web_phones == phone
        web_phone_times = web_times[web_mask]
        web_indices = np.where(web_mask)[0]
        
        # 找到该手机的所有银行交易
        bank_mask = bank_phones_valid == phone
        if not bank_mask.any():
            continue
            
        bank_phone_times = bank_times_valid[bank_mask]
        
        # 对于每个网站访问时间，检查时间窗口
        for i, visit_time in enumerate(web_phone_times):
            start_time = visit_time - np.timedelta64(hours, 'h')
            end_time = visit_time + np.timedelta64(hours, 'h')
            
            time_mask = (bank_phone_times >= start_time) & (bank_phone_times <= end_time)
            if time_mask.any():
                result[web_indices[i]] = True
    
    
    return result
def muti_process(row, data_mstransit, data_note, data_bank):
    
    data_mstransit_copy=copy.copy(data_mstransit)
    data_note_copy=copy.copy(data_note)
    data_bank_copy=copy.copy(data_bank)



    row['访问网址时间拨打电话'] = find_something_with_hours(
                                            row=row,
                                            df2=data_mstransit_copy,
                                            feature_time='time',
                                            feature_target='话单拨打')
    

    row['访问网址时间接收电话'] =find_something_with_hours(
                                            row=row,
                                            df2=data_mstransit_copy,
                                            feature_time='time',
                                            feature_target='话单接收')
    

    row['访问网址时间接收短信'] =find_something_with_hours(
                                            row=row,
                                            df2=data_note_copy,
                                            feature_time='time',
                                            feature_target='短信接收')
    

    row['访问网址时间发送短信'] =find_something_with_hours(
                                            row=row,
                                            df2=data_note_copy,
                                            feature_time='time',
                                            feature_target='短信发送')
    

    row['访问网址时间银行交易'] = find_something_with_hours_bank(
                                            row=row,
                                            df2=data_bank_copy,
                                            feature_time='time',
                                            feature_target='银行交易')
    
    row['访问网址时间短信异常联系人'] = find_something_with_hours(
                                            row=row,
                                            df2=data_note_copy,
                                            feature_time='time',
                                            feature_target='短信异常联系人')
    
    row['访问网址时间话单异常联系人'] = find_something_with_hours(
                                            row=row,
                                            df2=data_mstransit_copy,
                                            feature_time='time',
                                            feature_target='话单异常联系人')

    # del data_mstransit_copy,data_note_copy,data_bank_copy
    return row


def comb_time(data_mstransit,data_note,data_url,data_bank,needed_time:int=7):
    """
    needed_time      :指定时间前多少天的数据
    data_mstransit   :话单数据
    data_note        :短信数据
    data_flow        :流量数据
    
    """

    data_mstransit_copy = copy.copy(data_mstransit)
    del data_mstransit
    data_mstransit_copy['time'] = data_mstransit_copy['通话时间']
    data_mstransit_copy['call_type_mstransit'] = data_mstransit_copy['call_type']


    data_note_copy = copy.copy(data_note)
    del data_note
    data_note_copy['time'] = data_note_copy['操作时间']
    data_note_copy['call_type_note'] = data_note_copy['call_type']


    data_flow_copy = copy.copy(data_url)
    del data_url
    data_flow_copy['time'] = data_flow_copy['访问时间']
    data_flow_copy['网址'] = data_flow_copy['time']==data_flow_copy['time']


    data_bank_copy = copy.copy(data_bank)
    del data_bank
    data_bank_copy['time'] = data_bank_copy['交易时间']
    data_bank_copy['银行交易'] = data_bank_copy['time']==data_bank_copy['time']


    data_note_copy['短信接收']=data_note_copy['call_type_note'] == '1'
    data_note_copy['短信发送']=data_note_copy['call_type_note'] == '10'

    data_mstransit_copy['话单拨打']=data_mstransit_copy['call_type_mstransit'] == '101'
    data_mstransit_copy['话单接收']=data_mstransit_copy['call_type_mstransit'] == '102'


    # 短信异常联系人

    req_time_note_phone,data_note_copy['指定时间'] = select_usual_phone(data_note_copy,fea='other_party',fea_time_1='操作时间',fea_time_2='年月',needed_time_0=1,needed_time_1=8)
    data_note_copy['短信异常联系人'] = data_note_copy.apply(lambda da:select_unusual_phone(da,fea='other_party',usual_phone=req_time_note_phone),axis=1)
    del req_time_note_phone

    # 话单异常联系人

    req_time_mstransit_phone,data_mstransit_copy['指定时间'] = select_usual_phone(data_mstransit_copy,fea='other_party',fea_time_1='通话时间',fea_time_2='年月',needed_time_0=1,needed_time_1=8)
    data_mstransit_copy['话单异常联系人'] = data_mstransit_copy.apply(lambda da:select_unusual_phone(da,fea='other_party',usual_phone=req_time_mstransit_phone),axis=1)
    del req_time_mstransit_phone

    # data_flow_copy['访问网址时间拨打电话'] = None
    # data_flow_copy['访问网址时间接收电话'] = None
    # data_flow_copy['访问网址时间接收短信'] = None
    # data_flow_copy['访问网址时间发送短信'] = None
    # data_flow_copy['访问网址时间银行交易'] = None
    # data_flow_copy['访问网址时间短信异常联系人'] = None
    # data_flow_copy['访问网址时间话单异常联系人'] = None

    # data_flow_copy=data_flow_copy.apply(lambda da:muti_process(da,data_mstransit_copy,data_note_copy,data_bank_copy),axis=1)

    data_flow_copy['date_forbank'] = data_flow_copy['time'].dt.date
    data_bank_copy['date_forbank'] = data_bank_copy['time'].dt.date
    bank_daily = data_bank_copy.groupby(['msisdn', 'date_forbank']).size().to_frame('has_bank').reset_index()
    bank_data = data_flow_copy.merge(bank_daily, on=['msisdn', 'date_forbank'], how='left')
    data_flow_copy['访问网址时间银行交易'] = bank_data['has_bank'].notna()




    data_flow_copy['访问网址时间拨打电话'] = fully_vectorized_solution(data_flow_copy,data_mstransit_copy,'话单拨打')
    data_flow_copy['访问网址时间接收电话'] = fully_vectorized_solution(data_flow_copy,data_mstransit_copy,'话单接收')
    data_flow_copy['访问网址时间接收短信'] = fully_vectorized_solution(data_flow_copy,data_note_copy,'短信接收')
    data_flow_copy['访问网址时间发送短信'] = fully_vectorized_solution(data_flow_copy,data_note_copy,'短信发送')

    data_flow_copy['访问网址时间短信异常联系人'] = fully_vectorized_solution(data_flow_copy,data_note_copy,'短信异常联系人')
    data_flow_copy['访问网址时间话单异常联系人'] = fully_vectorized_solution(data_flow_copy,data_mstransit_copy,'话单异常联系人')
    
    
    


    merged = pd.concat([data_mstransit_copy,data_note_copy,data_flow_copy,data_bank_copy],axis=0)
    del data_mstransit_copy,data_note_copy,data_flow_copy,data_bank_copy

    # print(merged)
    merged_copy = copy.copy(merged)
    del merged

    # merged_copy.fillna(0,inplace=True)
    merged_copy=merged_copy[['msisdn','访问网址时间拨打电话','访问网址时间接收电话','访问网址时间接收短信','访问网址时间发送短信','访问网址时间银行交易','短信异常联系人','info_length','call_duration','话单异常联系人','访问网址时间话单异常联系人','访问网址时间短信异常联系人']]
    merged_copy.fillna(0,inplace=True)
    # print(merged_copy)
    grouped=merged_copy.groupby('msisdn').agg(
        
            访问网址时间拨打电话=('访问网址时间拨打电话','sum'),
            访问网址时间接收电话=('访问网址时间接收电话','sum'),
            

            访问网址时间接收短信=('访问网址时间接收短信','sum'),
            访问网址时间发送短信=('访问网址时间发送短信','sum'),

            访问网址时间银行交易=('访问网址时间银行交易','sum'),

            短信异常联系人=('短信异常联系人','sum'),
            平均短信长度=('info_length','mean'),
            最大短信长度=('info_length','max'),

            话单异常联系人=('话单异常联系人','sum'),
            平均通话长度=('call_duration','mean'),
            最大通话长度=('call_duration','max'),


            访问网址时间短信异常联系人=('访问网址时间短信异常联系人','sum'),
            访问网址时间话单异常联系人=('访问网址时间话单异常联系人','sum'),

           
    ).reset_index()
    grouped['访问网址时间通话'] = grouped['访问网址时间拨打电话'] + grouped['访问网址时间接收电话']

    grouped['访问网址时间短信'] = grouped['访问网址时间接收短信'] + grouped['访问网址时间发送短信']


    # print(grouped)

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
    # data=feature_main("kunshan","20251031")
    
    print(data)
    # data.to_excel("old.xlsx")
# 地点 nantong，kunshan
# 日期  20251107
# from datetime import datetime

    # main(location = 'nantong',date = "20251026" )

# res 
# 号码，时间，紧急程度，备注