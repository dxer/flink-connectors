# flink-connector-phoenix



## 创建 Phoenix5 结果表

|   参数    |  说明    | 是否必填     |  备注     |
| ---- | ---- | ---- | ---- |
|  connector    |      |   是   |  固定值为PHOENIX5    |
|  server-url    | Phoenix5的Query Server地址：     |  是     |  http://host:port，其中：<br> - host：Phoenix5服务的域名。<br> - port：Phoenix5服务的端口号，固定值为8765。     |
|  table-name    | 读取Phoenix5表名     |  是     | Phoenix5表名格式为SchemaName.TableName，其中：<br> SchemaName：模式名，可以为空，即不写模式名，仅写表名，表示使用数据库的默认模式。<br> TableName：表名。     |



```sql
create table stu_source (
  `id` varchar,
  `name` varchar,
  `age` varchar,
  `birthday` varchar 
) WITH (
   type = 'random'
);

create table stu_sink (
  `id` varchar,
  `name` varchar,
  `age` varchar,
  `birthday` varchar,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'PHOENIX5',
  'server-url' = 'http://127.0.0.1:8765',
  'table-name' = 'stu_sink'
);

INSERT INTO stu_sink
SELECT  `id`, `name`, `age`, `birthday` 
FROM `stu_source`;
```

## 创建Phoenix5维表

|   参数    |  说明    | 是否必填     |  备注     |
| ---- | ---- | ---- | ---- |
|  connector    |      |   是   |  固定值为PHOENIX5    |
|  server-url    | Phoenix5的Query Server地址：     |  是     |  http://host:port，其中：<br> - host：Phoenix5服务的域名。<br> - port：Phoenix5服务的端口号，固定值为8765。     |
|  table-name    | 读取Phoenix5表名     |  是     | Phoenix5表名格式为SchemaName.TableName，其中：<br> SchemaName：模式名，可以为空，即不写模式名，仅写表名，表示使用数据库的默认模式。<br> TableName：表名。     |




```sql
CREATE TABLE datahub_input (
  id  BIGINT,
  name  VARCHAR,
  age   BIGINT
) WITH (
  type='datahub'
);

create table phoneNumber(
  name VARCHAR,
  phoneNumber BIGINT,
  primary key(name),
  proc_time AS PROCTIME()
)with(
  'connector' = 'PHOENIX5',
  'server-url' = 'http://127.0.0.1:8765',
  'table-name' = 'result_info'
);

CREATE table result_info(
  id BIGINT,
  phoneNumber BIGINT,
  name VARCHAR
)with(
  type='rds'
);

INSERT INTO result_info
SELECT
  t.id,
  w.phoneNumber,
  t.name
FROM datahub_input as t
JOIN phoneNumber FOR SYSTEM_TIME AS OF a.proc_time w --维表JOIN时必须指定该声明
ON t.name = w.name;
```