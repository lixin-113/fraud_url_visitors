import pandas as pd 



# data=pd.read_csv("/data/app/output/nantong/20251027/20251027res.csv",sep="\t")
# data['date_time']="20251027"
# data.to_csv("/data/app/output/nantong/20251027/202510271res.csv",sep="\t",index=None)


# data1=pd.read_csv("/data/app/output/nantong/20251031/20251031feature_and_model_data.csv",sep="\t")
# data1=data1[(data1['rule_label_res']=="紧急") | (data1['rule_label_res']=="高危") | (data1['rule_label_res']=="中危")]
# data1.to_csv("20251031feature_and_model_data.csv",index=None)


# data2=pd.read_csv("/data/app/output/nantong/20251031/fff.csv",sep="\t")
# data2=data2[(data2['res']=="紧急") | (data2['res']=="高危") | (data2['res']=="中危")]
# data2.to_csv("20251031res.csv",index=None)

data3=pd.read_csv("/data/risk_guest/nantong/20251031/访客数据.txt",sep="\t")
print(data3)
data3['host_type']="色情博彩引流"
data3.to_csv("/data/risk_guest/nantong/20251031/访客数据2.txt",sep="\t",index=None)
