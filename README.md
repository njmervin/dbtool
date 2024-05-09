# dbtool

关系型数据库数据导入导出工具

## 导出

```sh
      export
           --type oracle                       # 数据库类型：oracle / mysql / postgresql
           --jdbc jdbc                         # <可选> JDBC连接串
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称或Oracle服务名
           --sid SID                           # Oracle数据库SID
           --user test                         # 用户名
           --pass test                         # 密码
           
           --table "table"                     # <可选> 表名
           --sql SQL                           # <可选> 完整的SQL语句           
           --fields "field1,...,fieldN"        # <可选> 要导出的字段列表，如果未设置，则导出所有列
           --where "where clause"              # <可选> 导出数据的条件和排序，如果未设置，则导出所有行
           --limit N                           # <可选> 限制导出的行数，如果未设置，则导出所有行
           --feedback N                        # <可选> 每多少行显示进度提示，默认为 10000
           --output destfile                   # 目标数据文件路径
           --log logfile                       # <可选> 日志JSON文件路径
           --timestamp TS                      # <可选> 日志JSON文件中操作文本型时间戳
           
java -jar dbtool-1.0.0.jar export --type oracle --host ***:*** --db orcl --user *** --pass *** --table *** --output "D:\data\***.dat"
```
### 日志文件
```json
{
  "code": 0,
  "timestamp": "abc",
  "start": "2024-05-07 20:41:58",
  "end": "2024-05-07 20:41:58",
  "cost": 25, //总耗时（毫秒）
  "data": {
    "size": 3081, //原始文件大小
    "rows": 1 //导出总行数
  }
}
```

## 导入

```sh
     import
           --type oracle                       # 数据库类型：oracle / mysql / postgresql
           --jdbc jdbc                         # <可选> JDBC连接串
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称或Oracle服务名
           --sid SID                           # Oracle数据库SID
           --user test                         # 用户名
           --pass test                         # 密码
           
           --table "table"                     # 表名
           --fields "name->newname,name3"      # <可选> 字段映射。
                                               #     如果未设置，则按照原始列名导入所有原始列
                                               #     如果设置"*"，则尝试导入和目标表同名的原始所有列
                                               #     如果设置"name"，则导入指定的原始列
                                               #     如果设置"name->newname"，则按照新名称newname导入原始列name
           --limit N                           # <可选> 限制导入的行数，如果未设置，则导入所有行
           --feedback N                        # <可选> 每多少行显示进度提示，默认为 10000
           --batch N                           # <可选> 批量提交的行数，默认为 10000
           --input destfile                    # 要导入的数据文件路径
		   --start N                           # <可选> 从第N行开始导入，默认从第一行开始导入
		   --upset "primary key"               # <可选> 根据唯一约束进行更新，需设置约束字段
           --log logfile                       # <可选> 日志JSON文件路径
           --timestamp TS                      # <可选> 日志JSON文件中操作文本型时间戳
           
java -jar dbtool-1.0.0.jar import --type oracle --host ***:*** --db orcl --user *** --pass *** --table *** --input "D:\data\***.dat"
```
### 日志文件
```json
{
  "code": 0,
  "timestamp": "abc",
  "start": "2024-05-07 20:41:58",
  "end": "2024-05-07 20:41:58",
  "cost": 25, //总耗时（毫秒）
  "data": {
    "rows": 1 //导入总行数
  }
}
```

## 更新

```sh
     exec
           --type oracle                       # 数据库类型：oracle / mysql / postgresql
           --jdbc jdbc                         # <可选> JDBC连接串
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称或Oracle服务名
           --sid SID                           # Oracle数据库SID
           --user test                         # 用户名
           --pass test                         # 密码
           
           --sql "execute sql"                 # 要执行的SQL语句
           --log logfile                       # <可选> 日志JSON文件路径
           --timestamp TS                      # <可选> 日志JSON文件中操作文本型时间戳
           
java -jar dbtool-1.0.0.jar exec --type oracle --host ***:*** --db orcl --user *** --pass *** --sql "update ..."
```
### 日志文件
```json
{
  "code": 0,
  "timestamp": "abc",
  "start": "2024-05-07 20:41:58",
  "end": "2024-05-07 20:41:58",
  "cost": 25, //总耗时（毫秒）
  "data": {
    "rows": 1 //影响的总行数
  }
}
```

## 导出主键
```sh
    export_pk
           --type oracle                       # 数据库类型：oracle / mysql / postgresql
           --jdbc jdbc                         # <可选> JDBC连接串
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称或Oracle服务名
           --sid SID                           # Oracle数据库SID
           --user test                         # 用户名
           --pass test                         # 密码
               
           --table S                           # <可选>表名
           --sql SQL                           # <可选>完整的SQL语句，不能包含排序
           --field S                           # <可选>要导出的主键字段
           --where S                           # <可选>导出数据的条件和排序，如果未设置，则导出所有行
           --feedback N                        # <可选>每多少行显示进度提示，默认为10000行
           --output destfile                   # 目标数据文件路径
```

## 比较主键文件
```sh
    compare_pk
           --src S                             # 源主键文件
           --dest S                            # 目标主键文件
           --feedback N                        # <可选>每多少行显示进度提示，默认为10000行
           --output S                          # 输出数据文件路径
```

## 通过比较主键进行删除多余记录
```sh
    export_pk
           --type oracle                       # 数据库类型：oracle / mysql / postgresql
           --jdbc jdbc                         # <可选> JDBC连接串
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称或Oracle服务名
           --sid SID                           # Oracle数据库SID
           --user test                         # 用户名
           --pass test                         # 密码
               
           --table S                           # 表名
           --field S                           # 主键字段
           --feedback N                        # <可选>每多少行显示进度提示，默认为10000行
           --batch N                           # <可选> 批量提交的行数，默认为 10000
           --output destfile                   # 目标数据文件路径
```

## 查看
```sh
     show
           --input destfile                    # 要查看的数据文件路径
           --row N                             # <可选>查看第N行数据
           --feedback N                        # <可选>每多少行显示进度提示，默认为10000行
java -jar dbtool-1.6.1.jar show --input '/data/data/raw/shuku/company_staff.20220726'

```
