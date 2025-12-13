import geopandas as gpd

import sys
sys.path.append('/data/app')

import pandas as pd
from shapely.geometry import Point
from core.global_logger import logger
from config import LOCATION_MAP_PATH,MAP_NAME
import os


def fix_encoding(text):
    """
    尝试修复由于编码错误导致的乱码字符串。
    """
    if pd.isna(text):
        return text
    try:
        byte_repr = text.encode('latin1', errors='ignore')
        return byte_repr.decode('utf-8')
    except Exception:
        return text

def process_data(data):

    # 修复字符串列的编码
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data[col] = data[col].astype(str).apply(fix_encoding)

    # 修复列名编码
    data.columns = [fix_encoding(col) for col in data.columns]
    logger.log_message(f"原地域数据为的特征为：\n{data.columns}")
    data.columns = ['DWBM', 'DWMC', '所属分局', 'DJD', 'DJDBH', 'geometry']
    if data.crs != "EPSG:4326":
        data=data.to_crs("EPSG:4326")



    logger.log_message("Shapefile 加载和编码错误 已经完成。。。")
    logger.log_message(f"预处理后的部分地域数据为：\n{data[:5]}")


    return 


def match_points(track_data,lng_fea,lat_fea,gd_data):


    # 筛选出凌晨0-6的数据计算其驻留时间，筛选出最大驻留时间的数据
    track_data['start_date'] = pd.to_datetime(track_data['start_date'], format='%Y%m%d%H%M%S', errors='coerce')
    track_data['end_date'] = pd.to_datetime(track_data['end_date'], format='%Y%m%d%H%M%S', errors='coerce')
    track_data['duration'] = (track_data['end_date'] - track_data['start_date']).dt.total_seconds() / 60  # 分钟
    track_data['hour'] = track_data['start_date'].dt.hour
    print(track_data)
    track_data_by_time=track_data[(track_data['hour'] > 0) & (track_data['hour'] < 6)].copy()
    logger.log_message(f"根据时间 0-6 筛选出的访客部分数据为：\n{track_data_by_time}")


    # 匹配出所有点所在的位置是否在指定位置中
    geometry=[Point(xy) for xy in zip(track_data_by_time[lng_fea],track_data_by_time[lat_fea])]

    track_geodata=gpd.GeoDataFrame(track_data_by_time,geometry=geometry,crs="EPSG:4326")
    logger.log_message(f"访客经纬度数据转化为gdf的部分数据为：\n{track_geodata}")

    merged=gpd.sjoin(track_geodata,gd_data,how='left',predicate="within")
    merged=merged.reset_index(drop=True)
    logger.log_message(f"根据访客经纬度匹配出派出所的位置的部分数据为：\n{merged[:5]}")
    # print(result)
    # print(result.reset_index(drop=True))
    
    grouped=merged.groupby('phone')

    # 创建结果数据框
    res=pd.DataFrame(columns=['phone','DWMC'])

    for phone,group in grouped:
        if group['DWMC'].isna().all():
            print("error")
            continue
        
        grouped_location=group.groupby('DWMC')['duration'].sum()

        duration_location_max=grouped_location.idxmax()
        tmp_df = pd.DataFrame({'phone': [phone], 'DWMC': [duration_location_max]})
        res=pd.concat([res,tmp_df],axis=0,ignore_index=True)
    logger.log_message(f"访客常驻匹配出的派出所位置的部分数据为：\n{res[:5]}")

    return res
 
def match_station_main(track_data,location:str):
    location_map_path=os.path.join(LOCATION_MAP_PATH,location)
    location_map_path=os.path.join(location_map_path,MAP_NAME)
    map_data=gpd.read_file(location_map_path,encoding="latin1")
    logger.log_message(f"读取地图路径为：{location_map_path}")
    logger.log_message(f"部分 {location} 派出所地图数据为：\n{map_data[:5]}")
    try:
        logger.log_message("正在对派出所地域数据进行预处理中。。。")
        process_data(map_data)
        logger.log_message("派出所地域数据预处理完成。。。")
    except Exception as e:
        logger.log_message(f"在对派出所地域数据进行预处理时出现：{e}！！！","error")

    try:
        logger.log_message("正在根据访客经纬度判断处于哪一个派出所中。。。")
        res=match_points(track_data,"lng","lat",map_data)
        logger.log_message("判断访客所处派出所已经完成。。。")
    except Exception as e:
        logger.log_message(f"在根据访客经纬度判断处于哪一个派出所时出现：{e}！！！","error")
    
    return res


if __name__=="__main__":
    logger.setup_log("app.log","/data/app/logs")
    track_data=pd.DataFrame({
                            "phone":["1806884823","13452422555"],
                            "lng":[118.97069,118.60369],
                            "lat":[31.86615,31.90747],
                            "start_date":["20251120054444","20251120054444"],
                            "end_date":["20251120055949","20251120055949"],
                            "duration":["60","70"]
                             })

    res=match_station_main(track_data,location="jiangning")
