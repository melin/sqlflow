解析sql 获取表和列血缘关系，以及算子信息，代码是从 trino 中抽取的，trino sql语法支持比较全。实现其它数据库sql血缘解析，适当调整antlr4 语法文件，比较容易实现。
目前支持spark、hive、flink、trino 等sql

## 主要功能:
1. 表与表血缘
2. 列与列血缘
3. sql 代码格式


## 相关项目
1. https://gitee.com/melin/bee
2. https://github.com/melin/superior-sql-parser
3. https://github.com/melin/superior-sql-formatter
4. https://github.com/melin/superior-sql-autocomplete
5. https://github.com/melin/datatunnel
6. https://github.com/melin/flink-jobserver
6. https://github.com/melin/spark-jobserver
