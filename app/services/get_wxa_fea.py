import pandas as pd
import os
from config import LOGPATH,WXA_FILEPATH
from core.global_logger import logger
#from get_wxa_files import get_wxa_files
from collections import Counter
from datetime import datetime,timedelta


def classify_files(date:str):

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
        logger.logger.info(f'当前处理的文件的信息为 {filename}')
        filepath=os.path.join(WXA_FILEPATH,filename)
        parts = filename.split('-') 
        file_name = parts[0]
        date_str,file_type=parts[-1].split('.')
        # 检查日期格式
        if file_name not in valid_files:
            logger.logger.error(f"跳过不符合命名规范的文件: {filename}")
            continue

        try:
            file_date = datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            logger.logger.error(f"无效日期格式，跳过: {filename}")
            continue
        
        # 判断是否在起始日期后
        if start_date <= file_date:
            valid_files[file_name].append(filepath)
            logger.logger.info(f"添加文件 {filename} 到 {file_name} 中。。。")

        elif start_date > file_date:
            # 删除过期文件
            logger.logger.warning(f"删除日期为 {date_str} 的文件 {filepath}。。。")
            try:
                os.remove(filepath)
            except OSError as e:
                logger.logger.error(f"删除文件失败 {filepath}: {e}")
        else:
            logger.logger.error(f"文件信息错误，请查看 {filepath}！！！")
            logger.logger.error(f"当前处理错误的文件信息为 {file_name},{date_str},{file_type},{start_date},{file_date}")

    return valid_files


def wxa_data_concat(valid_files:dict):
    AI_data=pd.DataFrame(columns=["id","msisdn","msisdn_region","is_warning","fraud_type","call_time","call_location","communication_method","other_number","other_number_region","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date"])
    HR_data=pd.DataFrame(columns=["id","msisdn","region:str","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id	other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date","gagag"])
    IM_data=pd.DataFrame(columns=["id","msisdn","region","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","is_involved","creator","create_date","involve_issue_date"])
    logger.logger.info("正在对分类结果遍历中。。。")
    for filename in valid_files:
        if not valid_files[filename]:
            continue
        elif filename=='dco_ai_warning_clue_extend':
            logger.logger.info(f"正在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}中。。。")
            for filepath in valid_files[filename]:
                try:
                    logger.logger.info("正在尝试使用 utf-8 读取文件中。。。")
                    tmp_ai_data = pd.read_csv(filepath, index_col=None, encoding='utf-8')
                    logger.logger.info("使用 utf-8 读取文件成功。。。")
                except UnicodeDecodeError:
                    logger.logger.error(f"使用 utf-8 读取文件失败！！！")
                    
                    try:
                        logger.logger.info("正在尝试使用 GBK 读取文件中。。。")
                        tmp_ai_data = pd.read_csv(filepath, index_col=None, encoding='GBK')
                        logger.logger.info("使用 GBK 读取文件成功。。。")
                    except UnicodeDecodeError:
                        logger.logger.error(f"使用 GBK 读取文件失败")

                        try:
                            logger.logger.info("正在尝试使用 GB18030 读取文件中。。。")
                            tmp_ai_data = pd.read_csv(filepath, index_col=None, encoding='GB18030')
                            logger.logger.info("使用 GB18030 读取文件成功。。。")
                        except UnicodeDecodeError as e:
                            logger.logger.error("无法解码文件，错误信息{e}！！！")
                try:
                    tmp_ai_data.columns=["id","msisdn","msisdn_region","is_warning","fraud_type","call_time","call_location","communication_method","other_number","other_number_region","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date"]
                    AI_data=pd.concat([AI_data,tmp_ai_data],axis=0)
                except Exception as e:
                    logger.logger.error(f"在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}时出现：{e}！！！")
            
        elif filename=='dco_high_risk_msisdn_clue_extend': 
            logger.logger.info(f"正在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}中。。。")
            for filepath in valid_files[filename]:
                try:
                    logger.logger.info("正在尝试使用 utf-8 读取文件中。。。")
                    tmp_hr_data = pd.read_csv(filepath, index_col=None, encoding='utf-8')
                    logger.logger.info("使用 utf-8 读取文件成功。。。")
                except UnicodeDecodeError:
                    logger.logger.error(f"使用 utf-8 读取文件失败！！！")
                    
                    try:
                        logger.logger.info("正在尝试使用 GBK 读取文件中。。。")
                        tmp_hr_data = pd.read_csv(filepath, index_col=None, encoding='GBK')
                        logger.logger.info("使用 GBK 读取文件成功。。。")
                    except UnicodeDecodeError:
                        logger.logger.error(f"使用 GBK 读取文件失败")

                        try:
                            logger.logger.info("正在尝试使用 GB18030 读取文件中。。。")
                            tmp_hr_data = pd.read_csv(filepath, index_col=None, encoding='GB18030')
                            logger.logger.info("使用 GB18030 读取文件成功。。。")
                        except UnicodeDecodeError as e:
                            logger.logger.error("无法解码文件，错误信息{e}！！！")
                try:
                    tmp_hr_data.columns=["id","msisdn","region:str","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id	other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date","gagag"]
                    HR_data=pd.concat([HR_data,tmp_hr_data],axis=0)
                except Exception as e:
                    logger.logger.error(f"在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}时出现：{e}！！！")
            
        elif filename=='dco_involved_msisdn_clue_extend': 
            logger.logger.info(f"正在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}中。。。")
            for filepath in valid_files[filename]:
                try:
                    logger.logger.info("正在尝试使用 utf-8 读取文件中。。。")
                    tmp_im_data = pd.read_csv(filepath, index_col=None, encoding='utf-8')
                    logger.logger.info("使用 utf-8 读取文件成功。。。")
                except UnicodeDecodeError:
                    logger.logger.error(f"使用 utf-8 读取文件失败！！！")
                    
                    try:
                        logger.logger.info("正在尝试使用 GBK 读取文件中。。。")
                        tmp_im_data = pd.read_csv(filepath, index_col=None, encoding='GBK')
                        logger.logger.info("使用 GBK 读取文件成功。。。")
                    except UnicodeDecodeError:
                        logger.logger.error(f"使用 GBK 读取文件失败")

                        try:
                            logger.logger.info("正在尝试使用 GB18030 读取文件中。。。")
                            tmp_im_data = pd.read_csv(filepath, index_col=None, encoding='GB18030')
                            logger.logger.info("使用 GB18030 读取文件成功。。。")
                        except UnicodeDecodeError as e:
                            logger.logger.error("无法解码文件，错误信息{e}！！！")
                try:
                    tmp_im_data.columns=["id","msisdn","region","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","is_involved","creator","create_date","involve_issue_date"]
                    IM_data=pd.concat([IM_data,tmp_im_data],axis=0)
                except Exception as e:
                    logger.logger.error(f"在整合名称为 {filename} 的文件其路径为 {valid_files[filename]}时出现：{e}！！！")
        else:
            logger.logger.error(f"在读取时出现数据名称不符合的文件 {filename} ，{valid_files[filename]}！！！")

    return AI_data,HR_data,IM_data


def get_wxa_files(date:str):
    try:
        logger.logger.info("正在对网信安数据进行分类中。。。")
        valid_files=classify_files(date)
        logger.logger.info(f"部分分类数据为：\n {str(valid_files)[:100]}")
    except Exception as e:
        logger.logger.error(f"在对网信安数据进行分类时出现！！！：{e}")

    try:
        logger.logger.info("正在对网信安数据进行整合中。。。")
        AI_data,HR_data,IM_data=wxa_data_concat(valid_files)
    except Exception as e:
        logger.logger.error(f"在对网信安数据进行读取并整合时出现！！！：{e}")
        AI_data=pd.DataFrame(columns=["id","msisdn","msisdn_region","is_warning","fraud_type","call_time","call_location","communication_method","other_number","other_number_region","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date"])
        HR_data=pd.DataFrame(columns=["id","msisdn","region:str","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id	other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","creator","create_date",""])
        IM_data=pd.DataFrame(columns=["id","msisdn","region","call_time","call_location","communication_method","other_number","call_duration","communication_type","domestic_fee","international_fee","subtotal","package_fee","used_package","package_info","base_station_id","cell_id","other_fee","charge_fee","status_type","receivable_domestic_fee","receivable_international_fee","receivable_other_fee","network_type","user","high_definition","switch_number","base_station_id_supplement","imei","other_cell_id","is_involved","creator","create_date","involve_issue_date"])
    
    output_AI=AI_data[['msisdn','call_time','other_number']].copy()
    output_HR=HR_data[['msisdn','call_time','other_number']].copy()
    output_IM=IM_data[['msisdn','call_time','other_number']].copy()
    
    logger.logger.info(f"部分AI预警数据为：\n {output_AI[:5]}")
    logger.logger.info(f"部分高危预警号码数据为：\n {output_HR[:5]}")
    logger.logger.info(f"部分案件号码数据为：\n {output_IM[:5]}")



    output_AI['call_time'] = pd.to_datetime(output_AI['call_time'].astype(str),format="%Y-%m-%d %H:%M:%S",errors="coerce")
    output_HR['call_time'] = pd.to_datetime(output_HR['call_time'].astype(str),format="%Y-%m-%d %H:%M:%S",errors="coerce")
    output_IM['call_time'] = pd.to_datetime(output_IM['call_time'].astype(str),format="%Y-%m-%d %H:%M:%S",errors="coerce")

    output_AI['msisdn'] = output_AI['msisdn'].astype(str)
    output_HR['msisdn'] = output_HR['msisdn'].astype(str)
    output_IM['msisdn'] = output_IM['msisdn'].astype(str)

    output_AI['other_number'] = output_AI['other_number'].astype(str)
    output_HR['other_number'] = output_HR['other_number'].astype(str)
    output_IM['other_number'] = output_IM['other_number'].astype(str)

    logger.logger.info(f"部分AI预警数据为：\n {output_AI[:5]}")
    logger.logger.info(f"部分高危预警号码数据为：\n {output_HR[:5]}")
    logger.logger.info(f"部分案件号码数据为：\n {output_IM[:5]}")


    return output_AI,output_HR,output_IM


def wxa_feature_main(data:pd.DataFrame ,date:str):


    try:
        start = datetime.strptime(date,"%Y%m%d") - timedelta(days=3)
        end = datetime.strptime(date,"%Y%m%d") + timedelta(days=1)
        AI_data,HR_data,IM_data=get_wxa_files(date)
        logger.logger.info(f"正在拉取从 {start} ----- {end} 时间的访客通联数据。。。")
        AI_phone=AI_data[(AI_data['call_time'] >= start) & (AI_data['call_time'] <= end)]['other_number'].tolist()
        HR_phone=HR_data[(HR_data['call_time'] >= start) & (HR_data['call_time'] <= end)]['other_number'].tolist()
        IM_phone=IM_data[(IM_data['call_time'] >= start) & (IM_data['call_time'] <= end)]['other_number'].tolist()

        
        logger.logger.info(f"正在统计从 {start} ----- {end} 时间的通联数据的对端号码。。。")
        AI_counter = Counter(AI_phone)
        HR_counter = Counter(HR_phone)
        IM_counter = Counter(IM_phone)
    except Exception as e:
        logger.logger.error(f"在拉取从 {start} ----- {end} 时间的通联数据时出现：{e}！！！")

    try:
        logger.logger.info(f"正在处理与AI预警通联特征中。。。")
        data['与AI预警通联']=data['msisdn'].map(AI_counter).fillna(0).astype(int)
        logger.logger.info(f"与AI预警通联特征已经处理完毕！")

        logger.logger.info(f"正在处理与高危号码通联特征中。。。")
        data['与高危预警号码通联']=data['msisdn'].map(HR_counter).fillna(0).astype(int)
        logger.logger.info(f"与高危预警通联号码通联特征已经处理完毕！")

        logger.logger.info(f"正在处理与案件号码通联特征中。。。")
        data['与案件号码通联']=data['msisdn'].map(IM_counter).fillna(0).astype(int)
        logger.logger.info(f"与案件号码通联特征已经处理完毕！")


    except Exception as e:
        logger.logger.error(f"在处理网信安数据特征中出现{e}！！！")

    logger.logger.info(f"日期为--{date}--网信安数据特征已经处理完成。。。")

    return data

if __name__=="__main__":
    logger.setup_log('test.log','/data/app/logs')
    data=pd.read_excel('/data/newnew.xlsx',dtype={'msisdn':str})
    data=wxa_feature_main(data,"20251031")
    data.to_csv('newnew.csv',sep="\t",index=None)