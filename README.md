# Fin-Data-Ops
Airflow数据运维项目
完成的脚本在finished_pys文件中

## 多进程任务调度
<img width="538" alt="image" src="https://user-images.githubusercontent.com/49713856/188827621-19ea99cd-7110-430f-b6de-599f823b2ce9.png">
拆分任务流为ETL过程，在DAG任务流上的每个节点设置运行失败和成功方法，方便排查错误
