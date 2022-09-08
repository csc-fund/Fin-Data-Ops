# Fin-Data-Ops
Airflow数据运维项目
完成的脚本在finished_pys文件中
## 运维项目1期需求
<img width="720" alt="image" src="https://user-images.githubusercontent.com/49713856/189041722-385c1c52-2b98-4ad5-8dbc-f62dc0d6fb8c.png">
1. 数据下载
2. 多源数据合并


## 多进程任务调度
<img width="538" alt="image" src="https://user-images.githubusercontent.com/49713856/188827621-19ea99cd-7110-430f-b6de-599f823b2ce9.png">
拆分任务流为ETL过程，在DAG任务流上的每个节点设置运行失败和成功方法，方便排查错误
