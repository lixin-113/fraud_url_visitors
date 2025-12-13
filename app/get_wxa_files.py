import os
from core.global_logger import logger
from datetime import datetime,timedelta
from config import WXA_FILEPATH
import pandas as pd

def classify_files(date:str):
    #for test
    WXA_FILEPATH = '/data/wxa_data'
    
    # 转为 date 对象用于比较
    start_date = (datetime.strptime(date, '%Y%m%d') - timedelta(days=30)).date()

    # 存放每种数据的有效文件路径
    valid_files = {
        'dco_ai_warning_clue_extend': [],
        'dco_high_risk_msisdn_clue_extend': [],
        'dco_involved_msisdn_clue_extend': []
    }

    # 获取网信安数据路径下的文件
    all_files = os.listdir(WXA_FILEPATH)
    for filename in all_files:
        logger.log_message(f'当前处理的文件的信息为 {filename}')
        filepath=os.path.join(WXA_FILEPATH,filename)
        parts = filename.split('-') 
        file_name = parts[0]
        date_str,file_type=parts[-1].split('.')
        # 检查日期格式
        if file_name not in valid_files:
            logger.log_message(f"跳过不符合命名规范的文件: {filename}","error")
            continue

        try:
            file_date = datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            logger.log_message(f"无效日期格式，跳过: {filename}")
            continue
        
        # 判断是否在起始日期后
        if start_date <= file_date:
            valid_files[file_name].append(filepath)
            logger.log_message(f"添加文件 {filename} 到 {file_name} 中。。。")

        elif start_date > file_date:
            # 删除过期文件
            logger.log_message(f"删除日期为 {date_str} 的文件 {filepath}。。。")
            try:
                os.remove(filepath)
            except OSError as e:
                logger.log_message(f"删除文件失败 {filepath}: {e}","error")
        else:
            logger.log_message(f"文件信息错误，请查看 {filepath}！！！","error")
            logger.log_message(f"当前处理错误的文件信息为 {file_name},{date_str},{file_type},{start_date},{file_date}")
    # print(fff)
    return valid_files


def wxa_data_concat(valid_files:dict):
    AI_data=pd.DataFrame(columns=["id","msisdn","msisdn_region","is_warning","fraud_type","call_time","call_location","communication_method","other_number","other_number_region","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date"])
    HR_data=pd.DataFrame(columns=["id","msisdn","region:str","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id	other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date","gagag"])
    IM_data=pd.DataFrame(columns=["id","msisdn","region","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","is_involved","creator","create_date","involve_issue_date"])
    logger.log_message("正在对分类结果遍历中。。。")
    for filename in valid_files:
        if not valid_files[filename]:
            continue
        elif filename=='dco_ai_warning_clue_extend':
            for filepath in valid_files[filename]:
                logger.log_message(f"正在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}中。。。")
                try:
                    tmp_ai_data=pd.read_csv(filepath,index_col=None)
                    tmp_ai_data.columns=["id","msisdn","msisdn_region","is_warning","fraud_type","call_time","call_location","communication_method","other_number","other_number_region","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date"]
                    AI_data=pd.concat([AI_data,tmp_ai_data],axis=0)
                except Exception as e:
                    logger.log_message(f"在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}时出现：{e}！！！")
            
        elif filename=='dco_high_risk_msisdn_clue_extend': 
            for filepath in valid_files[filename]:
                logger.log_message(f"正在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}中。。。")
                try:
                    tmp_hr_data=pd.read_csv(filepath,index_col=None)
                    tmp_hr_data.columns=["id","msisdn","region:str","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id	other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date","gagag"]
                    HR_data=pd.concat([HR_data,tmp_hr_data],axis=0)
                except Exception as e:
                    logger.log_message(f"在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}时出现：{e}！！！")
            
        elif filename=='dco_involved_msisdn_clue_extend': 
            for filepath in valid_files[filename]:
                logger.log_message(f"正在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}中。。。")
                try:
                    tmp_im_data=pd.read_csv(filepath,index_col=None)
                    tmp_im_data.columns=["id","msisdn","region","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","is_involved","creator","create_date","involve_issue_date"]
                    IM_data=pd.concat([IM_data,tmp_im_data],axis=0)
                except Exception as e:
                    logger.log_message(f"在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}时出现：{e}！！！")
        else:
            logger.log_message(f"在读取时出现数据名称不符合的文件 {filename} ，{valid_files[filename]}！！！")

    return AI_data,HR_data,IM_data


def get_wxa_files(date:str):
    try:
        logger.log_message("正在对网信安数据进行分类中。。。")
        valid_files=classify_files(date)
        logger.log_message(f"部分分类数据为：\n {str(valid_files)[:100]}")
    except Exception as e:
        logger.log_message(f"在对网信安数据进行分类时出现！！！：{e}")

    try:
        logger.log_message("正在对网信安数据进行整合中。。。")
        AI_data,HR_data,IM_data=wxa_data_concat(valid_files)
    except Exception as e:
        logger.log_message(f"在对网信安数据进行读取并整合时出现！！！：{e}")
        AI_data=pd.DataFrame(columns=["id","msisdn","msisdn_region","is_warning","fraud_type","call_time","call_location","communication_method","other_number","other_number_region","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date"])
        HR_data=pd.DataFrame(columns=["id","msisdn","region:str","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id	other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date",""])
        IM_data=pd.DataFrame(columns=["id","msisdn","region","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","is_involved","creator","create_date","involve_issue_date"])
    
    output_AI=AI_data[['msisdn','call_time','other_number']].copy()
    output_HR=HR_data[['msisdn','call_time','other_number']].copy()
    output_IM=IM_data[['msisdn','call_time','other_number']].copy()
    
    logger.log_message(f"部分AI预警数据为：\n {output_AI[:5]}")
    logger.log_message(f"部分高危预警号码数据为：\n {output_HR[:5]}")
    logger.log_message(f"部分案件号码数据为：\n {output_IM[:5]}")



    output_AI['call_time'] = pd.to_datetime(output_AI['call_time'].astype(str),format="%Y-%m-%d %H:%M:%S")
    output_HR['call_time'] = pd.to_datetime(output_HR['call_time'].astype(str),format="%Y-%m-%d %H:%M:%S")
    output_IM['call_time'] = pd.to_datetime(output_IM['call_time'].astype(str),format="%Y-%m-%d %H:%M:%S")

    output_AI['msisdn'] = output_AI['msisdn'].astype(str)
    output_HR['msisdn'] = output_HR['msisdn'].astype(str)
    output_IM['msisdn'] = output_IM['msisdn'].astype(str)

    output_AI['other_number'] = output_AI['other_number'].astype(str)
    output_HR['other_number'] = output_HR['other_number'].astype(str)
    output_IM['other_number'] = output_IM['other_number'].astype(str)

    logger.log_message(f"部分AI预警数据为：\n {output_AI[:5]}")
    logger.log_message(f"部分高危预警号码数据为：\n {output_HR[:5]}")
    logger.log_message(f"部分案件号码数据为：\n {output_IM[:5]}")


    return output_AI,output_HR,output_IM


if __name__=="__main__":
    logger.setup_log('test.log','/data/app/logs')
    a,b,c=get_wxa_files("20251031")