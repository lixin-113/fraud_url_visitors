
INPUTPATH="/data/risk_guest"

OUTPUTPATH="/data/app/output"
OUTPUTNAME="res.csv"

LOGPATH="/data/app/logs"

# RF_MODEL_PATH="/data/app/model/best_rf.pkl"
RF_MODEL_PATH="/data/app/model/new_best_rf.pkl"

FDFS_USER="bsyftp"
FDFS_PASSWORD="BSYftp123@"
FDFS_API="10.33.28.22"
FDFS_PATH="/data/yd/sahm"
FDFS_CONFIG='client.conf'

# WXA_FILEPATH="/data/app/wxa_data"
WXA_FILEPATH="/data/wxa_data"

#This is for testing
# TESTPATH="/data/app/test_data"

# === SFT 配置信息 ===
SFTP_HOST = "10.33.28.22"
SFTP_PORT = 22
SFTP_USER = "bsyftp"
SFTP_PASSWORD = "BSYftp123@"
REMOTE_DIR = "/data/yd/sahm"

# 设置风险等级分值
MAP_DICT={"紧急":4,"高危":3,"中危":2,"低危":1}

TYPE_SET=set(['色情博彩引流','裸聊诈骗','招嫖诈骗','博彩诈骗','虚假服务诈骗','公检法','冒充电商客服诈骗','虚假购物诈骗','仿冒政府网站诈骗','刷单诈骗','客服系统','理财诈骗','贷款诈骗'])
RISK_TYPE_SET=set(['虚假服务诈骗','公检法','冒充电商客服诈骗','虚假购物诈骗','仿冒政府网站诈骗','刷单诈骗','客服系统','理财诈骗','贷款诈骗'])

# 派出所地图存放路径
LOCATION_MAP_PATH="/data/app/services/data"
MAP_NAME="派出所边界.shp"



