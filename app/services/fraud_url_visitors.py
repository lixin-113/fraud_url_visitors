from config import INPUTPATH,LOGPATH
from core.global_logger import logger
import pandas as pd 
from datetime import datetime,timedelta
import copy
from functools import reduce
import numpy as np
import os

YEAR=datetime.now().year

def process_data_user(data):
    col=[i.split('.')[1] for i in data.columns]
    col[2]='msisdn'
    data.columns=col
    data['年月'] = pd.to_datetime(data['stat_date'],format="%Y%m%d")

    return 

def process_data_time_user(data):

    data['年月'] = pd.to_datetime(data['date'], format='%Y%m%d')

    return 
def process_data_mstransit(data):

    data.columns=[i.split('.')[0] for i in data.columns]

    data['通话时间']=pd.to_datetime(data['sdate'])

    return


def process_data_note(data):
    data['操作时间'] = pd.to_datetime(f"{YEAR}"+data['deal_time'],format='%Y%m%d%H%M%S')

    return 


def process_data_flow(data):
    data.columns=[i.split('.')[0] for i in data.columns]
    data['访问时间'] = pd.to_datetime(data['data_time'])
    return


def process_data_url(data):
    col=[i.split('.')[0] for i in data.columns]
    col[0]='msisdn'
    data.columns=col
    data['访问时间'] = pd.to_datetime(data['data_time'])
    # print(data['访问时间'].dt.hour)
    return


def process_data_bank(data):
    data.columns=[i.split('.')[0] for i in data.columns]
    data['交易时间'] = pd.to_datetime(data['cdr_date'],format="%Y%m%d")

    return
    

# 获取指定时间前1天到指定时间前8天的异常数据
def filter_needed_time_other(data,fea_time_1:str,fea_time_2:str,needed_time_0:int,needed_time_1:int):

    """
    data
    fea_time_1   :原数据时间
    fea_time_2   :固定时间时间
    needed_time_0:指定异常数据时间前多少天
    needed_time_1:指定异常数据时间前多少天
    
    """
    try:

        data_copy=data[[fea_time_1,fea_time_2]].copy()
        date_timestamp=data_copy[fea_time_2][0]
        start_time=date_timestamp - timedelta(days=needed_time_1)
        end_time=date_timestamp
        logger.logger.info(f"正在处理日期从 {start_time} 到 {end_time}")
        usual_fea=(data_copy[fea_time_1] >= start_time) & (data_copy[fea_time_1] < end_time)
    except Exception as e:
        logger.logger.error(f"在处理日期从 {date_timestamp} 到 {end_time} 时出现{e}")
    # 存在bug 1-8 内的数据为True 8 之前也为false，在生产上不会
    return usual_fea
                                      
                                     

def select_usual_phone(data, fea_time_1, fea_time_2,phone_fea:str="msisdn",target_fea:str="other_party", needed_time_0:int=1, needed_time_1:int=8):
    try:
        logger.logger.info(f"正在处理特征为 {fea_time_1} 的异常联系人。。。")
        usual_fea = filter_needed_time_other(
            data, fea_time_1, fea_time_2,
            needed_time_0=needed_time_0,
            needed_time_1=needed_time_1
        )
        new_data = data[usual_fea].copy()

        # 返回常用联系人唯一组合（DataFrame）
        usual_pairs = new_data[[phone_fea, target_fea]].drop_duplicates()
        return usual_pairs, usual_fea

    except Exception as e:
        logger.logger.error(f"在处理特征为 {fea_time_1} 的异常联系人时出现 {e}！！！")
        return pd.DataFrame(columns=['msisdn', fea_time_1]), pd.Series([], dtype=bool)
    
def select_unusual_phone(data,usual_pairs,target_fea:str="other_party",phone_fea:str="msisdn",time_fea:str='指定时间'):
    try:
        logger.logger.info(f"正在进行 异常联系人 匹配中")
        data_copy=data[[phone_fea,target_fea,time_fea]].copy()
        data_copy.fillna({target_fea : False}, inplace=True)

        merged = data_copy.merge(
                    usual_pairs,
                    on=[phone_fea, target_fea],
                    how='left',
                    indicator=True
                )
        
        data_copy['res'] = (~data_copy[time_fea] & (merged['_merge'] == 'left_only'))
    except Exception as e:
        logger.logger.error(f"在 异常联系人 匹配时出现{e}！！！")
    res_fea=data_copy['res']
    del data_copy

    return res_fea


def fully_vectorized_solution_2(df1, df2, target_fea,phone_fea='msisdn',df1_time='time',df2_time='time',hours=1):
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

def fully_vectorized_solution(df1, df2, target_fea,phone_fea='msisdn',feature_time='time',hours=1):
    logger.log_message("正在进行 特征处理中。。。")
    try:
        df1_copy=df1.copy()
        df2_copy=df2.copy()
        # 只筛选 df2 中目标特征为 True 的列
        df2_target_fea = df2_copy[df2_copy[target_fea]].copy()
        if df2_target_fea.empty:
            df1_copy['has_something'] = False
        else:
            # 将df1 每个手机号码的时间 对df2 中的对应手机号的时间进行拼接
            merged = pd.merge(
                df1_copy[[phone_fea, feature_time]].reset_index(),
                df2_target_fea[[phone_fea, feature_time]],
                on=phone_fea,
                how='left',
                suffixes=('_df1', '_df2')
            )

            
            if not merged.empty:

                # 判断 df1 与 df2 时间差值
                time_diff = (merged['time_df1'] - merged['time_df2']).abs()
                merged['within_hour'] = time_diff <= pd.Timedelta(hours=hours)
                
                # 对每个原始 df1 行的索引进行groupby，只要有一个匹配就标记为 True
                has_match = merged.groupby('index')['within_hour'].any()

                # 将 has_match 根据索引映射到 df1 中 其余为 False 
                df1_copy['has_something'] = df1_copy.index.map(has_match).fillna(False)
            else:
                df1_copy['has_something'] = False
    except Exception as e:
        logger.logger.error(f"在特征分析时出现{e}！！！")
    return df1_copy['has_something']




def comb_time(data_mstransit,data_note,data_url,data_bank):
    """
    needed_time      :指定时间前多少天的数据
    data_mstransit   :话单数据
    data_note        :短信数据
    data_flow        :流量数据
    
    """
 
    data_mstransit['time'] = data_mstransit['通话时间']
    data_mstransit['call_type_mstransit'] = data_mstransit['call_type']
    data_mstransit_copy=data_mstransit[['msisdn','time','通话时间','call_type_mstransit','call_duration','other_party','年月']].copy()
    del data_mstransit


    data_note['time'] = data_note['操作时间']
    data_note['call_type_note'] = data_note['call_type']
    data_note_copy=data_note[['msisdn','time','操作时间','call_type_note','info_length','other_party','年月']].copy()
    del data_note


    data_url['time'] = data_url['访问时间']
    data_flow_copy=data_url[['msisdn','time','访问时间']].copy()
    del data_url

    data_bank['time'] = data_bank['交易时间']
    data_bank['银行交易'] = data_bank['time']==data_bank['time']
    data_bank_copy=data_bank[['msisdn','time','交易时间']].copy()
    del data_bank


    data_note_copy['短信接收']=data_note_copy['call_type_note'] == '1'
    data_note_copy['短信发送']=data_note_copy['call_type_note'] == '10'

    data_mstransit_copy['话单拨打']=data_mstransit_copy['call_type_mstransit'] == '101'
    data_mstransit_copy['话单接收']=data_mstransit_copy['call_type_mstransit'] == '102'


    # 短信异常联系人
    try:
        logger.logger.info("正在处理短信异常联系人。。。")
        note_usual_pairs, data_note_copy['指定时间'] = select_usual_phone(data_note_copy, fea_time_1='操作时间', fea_time_2='年月')
        data_note_copy['短信异常联系人'] = select_unusual_phone(data_note_copy, note_usual_pairs)
        del note_usual_pairs
        logger.logger.info("短信异常联系人已经处理完成！！！")
    except Exception as e:
        logger.logger.error(f"在处理短信异常联系人时出现：{e}！！！")


    # 话单异常联系人
    try:
        logger.logger.info("正在处理话单异常联系人。。。")
        mstransit_usual_pairs,data_mstransit_copy['指定时间'] = select_usual_phone(data_mstransit_copy,fea_time_1='通话时间',fea_time_2='年月')
        data_mstransit_copy['话单异常联系人'] = select_unusual_phone(data_mstransit_copy, mstransit_usual_pairs)

        del mstransit_usual_pairs
        logger.logger.info("话单异常联系人已经处理完成！！！")
    except Exception as e:
        logger.logger.error(f"在处理话单异常联系人时出现：{e}！！！")


    try:
        logger.logger.info("正在处理特征 访问网址时间银行交易 中。。。")
        data_flow_copy['date_forbank'] = data_flow_copy['time'].dt.date
        data_bank_copy['date_forbank'] = data_bank_copy['time'].dt.date
        bank_daily = data_bank_copy.groupby(['msisdn', 'date_forbank']).size().to_frame('has_bank').reset_index()
        bank_data = data_flow_copy.merge(bank_daily, on=['msisdn', 'date_forbank'], how='left')
        data_flow_copy['访问网址时间银行交易'] = bank_data['has_bank'].notna()
        logger.logger.info("特征 访问网址时间银行交易 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间银行交易 时出现{e}！！！")



    try:
        logger.logger.info("正在处理特征 访问网址时间拨打电话 中。。。")
        data_flow_copy['访问网址时间拨打电话'] = fully_vectorized_solution(data_flow_copy,data_mstransit_copy,'话单拨打')
        logger.logger.info("特征 访问网址时间拨打电话 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间拨打电话 时出现{e}！！！")
        
    try:
        logger.logger.info("正在处理特征 访问网址时间接收电话 中。。。")
        data_flow_copy['访问网址时间接收电话'] = fully_vectorized_solution(data_flow_copy,data_mstransit_copy,'话单接收')
        logger.logger.info("特征 访问网址时间接收电话 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间接收电话 时出现{e}！！！")

    try:
        logger.logger.info("正在处理特征 访问网址时间接收短信 中。。。")
        data_flow_copy['访问网址时间接收短信'] = fully_vectorized_solution(data_flow_copy,data_note_copy,'短信接收')
        logger.logger.info("特征 访问网址时间接收短信 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间接收短信 时出现{e}！！！")

    try:
        logger.logger.info("正在处理特征 访问网址时间发送短信 中。。。")
        data_flow_copy['访问网址时间发送短信'] = fully_vectorized_solution(data_flow_copy,data_note_copy,'短信发送')
        logger.logger.info("特征 访问网址时间发送短信 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间发送短信 时出现{e}！！！")
    
    try:
        logger.logger.info("正在处理特征 访问网址时间短信异常联系人 中。。。")
        data_flow_copy['访问网址时间短信异常联系人'] = fully_vectorized_solution(data_flow_copy,data_note_copy,'短信异常联系人')
        logger.logger.info("特征 访问网址时间短信异常联系人 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间短信异常联系人 时出现{e}！！！")

    try:
        logger.logger.info("正在处理特征 访问网址时间话单异常联系人 中。。。")
        data_flow_copy['访问网址时间话单异常联系人'] = fully_vectorized_solution(data_flow_copy,data_mstransit_copy,'话单异常联系人')
        logger.logger.info("特征 访问网址时间话单异常联系人 已经处理完成。。。")
    except Exception as e:
        logger.logger.error(f"在处理特征 访问网址时间话单异常联系人 时出现{e}！！！")
    
    
    merged = pd.concat([data_mstransit_copy,data_note_copy,data_flow_copy,data_bank_copy],axis=0)
    del data_mstransit_copy,data_note_copy,data_flow_copy,data_bank_copy


    try:
        logger.logger.info("正在进行分析手机号码的特征整合中。。。")
        merged_copy=merged[['msisdn','访问网址时间拨打电话','访问网址时间接收电话','访问网址时间接收短信','访问网址时间发送短信','访问网址时间银行交易','短信异常联系人','info_length','call_duration','话单异常联系人','访问网址时间话单异常联系人','访问网址时间短信异常联系人']].copy()
        merged_copy.fillna(0,inplace=True)

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
    except Exception as e:
        logger.logger.error(f"在进行分析手机号码的特征整合时出现{e}！！！")

    # print(grouped)

    return grouped
def feature_main(location : str,date:str):
    
    location_path=os.path.join(INPUTPATH,location)
    location_date_path=os.path.join(location_path,date)
    logger.logger.info(f"正在进行访客特征分析的日期为：{date}...")

    try:
        mstransit_name='全量话单数据.txt'
        logger.logger.info(f"正在读取的文件数据为：{mstransit_name}...")
        mstransit_path=os.path.join(location_date_path,mstransit_name)
        mstransit_type={'city_code':str,'phone':str,'visitor_date':str,'msisdn':str,'call_type':str,'other_party':str,'sdate':str,'duration60':str,'call_duration':int,'lac':str,'cell_id':str,'cdr_date':str}
        data_mstransit=pd.read_csv(mstransit_path,sep="\t",dtype=mstransit_type)
        logger.logger.info(f"文件数据:{mstransit_name},已经读取完成！！！")
    except Exception as e:
        logger.logger.error(f"在读取 {mstransit_name} 数据过程中出现:{e}！！")   
    
    try:

        note_name='短信数据.txt'
        logger.logger.info(f"正在读取的文件数据为：{note_name}...")
        note_path=os.path.join(location_date_path,note_name)
        note_type={'msisdn':str,'other_party':str,'call_type':str,'deal_time':str,'finish_t':str,'info_length':int}
        data_note=pd.read_csv(note_path,sep="\t",dtype=note_type)
        logger.logger.info(f"文件数据:{note_name},已经读取完成！！！")
    except Exception as e:
        logger.logger.error(f"在读取 {note_name} 数据过程中出现:{e}！！")


    try:
        url_name='访客数据.txt'
        logger.logger.info(f"正在读取的文件数据为：{url_name}...")
        url_path=os.path.join(location_date_path,url_name)
        url_type={'phone':str,'data_time':str,'host':str,'city':str}
        data_url=pd.read_csv(url_path,sep="\t",dtype=url_type)
        logger.logger.info(f"文件数据:{url_name},已经读取完成！！！")
    except Exception as e:
        logger.logger.error(f"在读取 {url_name} 数据过程中出现:{e}！！")


    try:
        user_name='手机号去重清单.txt'
        logger.logger.info(f"正在读取的文件数据为：{user_name}...")
        user_path=os.path.join(location_date_path,user_name)
        user_type={'t.city':str,'city_code':str,'t.phone':str,'t.stat_date':str}
        data_user=pd.read_csv(user_path,sep="\t",dtype=user_type)
        logger.logger.info(f"文件数据:{user_name},已经读取完成！！！")
    except Exception as e:
        logger.logger.error(f"在读取 {user_name} 数据过程中出现:{e}！！")


    try:
        bank_name='app银行类别的明细数据.txt'
        logger.logger.info(f"正在读取的文件数据为：{bank_name}...")
        bank_path=os.path.join(location_date_path,bank_name)
        bank_type={'city_code':str,'phone':str,'visitor_date':str,'msisdn':str,'imei':str,'region_id':str,'app_id':str,'match_cnt':str,'upload':str,'download':str,'conn_during':str,'cnt_rules':str,'cdr_date':str,'app_name':str}
        data_bank=pd.read_csv(bank_path,sep="\t",dtype=bank_type)
        logger.logger.info(f"文件数据:{bank_name},已经读取完成！！！")

    except Exception as e:
        logger.logger.error(f"在读取 {bank_name} 数据过程中出现:{e}！！")

    data_time_user=pd.DataFrame({
            "date":[date]
        })
    logger.logger.info(f"文件数据已经全部读取完成！！！")

    logger.logger.info("正在进行数据预处理中。。。")
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
        logger.logger.error(f"在数据预处理中出现{e}！！！")

    logger.logger.info("数据预处理已经全部完成！！！")

    del data_time_user

    logger.logger.info("正在进行访客数据特征分析中。。。")
    try:
        grouped_time=comb_time(data_mstransit,data_note,data_url,data_bank)
    except Exception as e:
        logger.logger.error(f"在进行访客数据特征分析中出现{e}！！！")

    logger.logger.info(f"日期为--{date}--的访客数据特征分析完成！！！ ")
    need_data=grouped_time[["msisdn","访问网址时间拨打电话","访问网址时间接收电话","访问网址时间接收短信","访问网址时间发送短信","访问网址时间银行交易","短信异常联系人","平均短信长度","最大短信长度","话单异常联系人","平均通话长度","最大通话长度","访问网址时间短信异常联系人","访问网址时间话单异常联系人","访问网址时间通话","访问网址时间短信"]]
    
    
    df_unique = data_user[['msisdn']].drop_duplicates(subset='msisdn', keep='first').copy()
    output_data=pd.merge(df_unique,need_data,on='msisdn',how='left')
    output_data.fillna(0,inplace=True)
    logger.logger.info(f"正在返回数据，部分数据为\n {output_data[:5]}")
    
    return output_data


if __name__=="__main__":
    # set INPUTPATH for test
    logger.setup_log("test.log","/data/app/logs")
    data=feature_main("nantong","20251031")