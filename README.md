# dbtool

关系型数据库数据导入导出工具

## 导出

```sh
      export
           --type oracle                       # 数据库类型：oracle / mysql
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称
           --user test                         # 用户名
           --pass test                         # 密码
           --table "table"                     # 表名
           --fields "field1,...,fieldN"        # <可选> 要导出的字段列表，如果未设置，则导出所有列
           --where "where clause"              # <可选> 导出数据的条件和排序，如果未设置，则导出所有行
           --limit N                           # <可选> 限制导出的行数，如果未设置，则导出所有行
           --feedback N                        # <可选> 每多少行显示进度提示，默认为 10000
           --output destfile                   # 目标数据文件路径

java -jar dbtool-1.0.0.jar export --type oracle --host ***:*** --db orcl --user *** --pass *** --table *** --output "D:\data\***.dat"
```

## 导入

```sh
     import
           --type oracle                       # 数据库类型：oracle / mysql
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称
           --user test                         # 用户名
           --pass test                         # 密码
           --table "table"                     # 表名
           --fields "name->newname,name3"      # <可选> 字段映射。如果未设置，则按照原始列名导入所有原始列；
                                               #        如果设置"name"，则导入指定的原始列；
                                               #        如果设置"name->newname"，则按照新名称newname导入原始列name
           --limit N                           # <可选> 限制导入的行数，如果未设置，则导入所有行;如果为0，则仅显示原始列名
           --feedback N                        # <可选> 每多少行显示进度提示，默认为 10000
           --batch N                           # <可选> 批量提交的行数，默认为 1000
           --debugrow N                        # <可选> 设置输出第N行每列的详细数据
           --input destfile                    # 要导入的数据文件路径
		   --start N                           # <可选> 从第N行开始导入
		   --upset "primary key"               # <可选> 根据主键更新，需设置主键字段

java -jar dbtool-1.0.0.jar import --type oracle --host ***:*** --db orcl --user *** --pass *** --table *** --batch 1000 --input "D:\data\***.dat"
```

## 更新

```sh
     exec
           --type oracle                       # 数据库类型：oracle / mysql
           --host 127.0.0.1:1521               # 数据库地址
           --db orcl                           # 数据库名称
           --user test                         # 用户名
           --pass test                         # 密码
           --sql "execute sql"                 # 要执行的SQL语句

java -jar dbtool-1.0.0.jar exec --type oracle --host ***:*** --db orcl --user *** --pass *** --sql "update ..."
```

## 查看
```sh
     show
           --input destfile                    # 要导入的数据文件路径
           --ddl yes                           # <可选> 是否生成表创建语句
java -jar dbtool-1.6.1.jar show --input '/data/data/raw/shuku/company_staff.20220726' --ddl yes
```
