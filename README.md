# 涉诈网址历史访客风险等级分级模型
这是一个每天定时对访客进行风险等级分级的服务。
## 项目结构

```
app/
├── logs/               # 日志
├── core/               # 日志处理器
├── model/              # 模型文件
├── output/             # 分级结果
├── services/           # 处理核心
├── config.py           # 基本配置信息
├── docker-compose.yaml # 容器启动脚本
├── main.py             # 应用入口
├── run.sh              # 启动脚本
└── test.py             # 测试入口
README.md         
```
## 运行服务
docker exec fraud_url_visitors /bin/bash -c "nohup python /data/app/main.py > scheduler.log 2>&1 &"

## 定时任务
每天 12：00 处理昨天部分地域的访客数据

