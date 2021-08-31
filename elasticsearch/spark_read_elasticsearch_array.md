# spark读elasticsearch array
### elasticsearch数组
在Elasticsearch中，没有专用的数组类型。默认情况下，任何字段都可以包含零个或多个值(数组中的所有值必须具有相同的数据类型)。  
所以，我们在写数据的时候，可以忽略数组和单个值得区别。例如：
```
PUT my_index/_doc/1
{
  "group" : "fans",
  "user" : [
    {
      "first" : "John",
      "last" :  "Smith"
    },
    {
      "first" : "Alice",
      "last" :  "White"
    }
  ],
  "tag": [
    "read",
    "write"
  ]
}

PUT my_index/_doc/2
{
  "group" : "fans",
  "user" : {
    "first" : "John",
    "last" :  "Smith"
  },
  "tag": "read"
}
```
这样写是没有问题的，查询也能得到结果  
查询：
```
GET my_index/_search
{
  "query": {
    "match": {
      "user.first": "john"
    }
  }
}
```
结果：
```
{
  "took": 0,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 2,
    "max_score": 0.2876821,
    "hits": [
      {
        "_index": "my_index",
        "_type": "_doc",
        "_id": "2",
        "_score": 0.2876821,
        "_source": {
          "group": "fans",
          "user": {
            "first": "John",
            "last": "Smith"
          },
          "tag": "read"
        }
      },
      {
        "_index": "my_index",
        "_type": "_doc",
        "_id": "1",
        "_score": 0.2876821,
        "_source": {
          "group": "fans",
          "user": [
            {
              "first": "John",
              "last": "Smith"
            },
            {
              "first": "Alice",
              "last": "White"
            }
          ],
          "tag": [
            "read",
            "write"
          ]
        }
      }
    ]
  }
}
```
可以发现，文档1的user是数组，文档2的user是对象。 
### spark读elasticsearch array
当我们用spark读数据的时候就有问题了，例如：
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df_test = sqlContext.read.format("org.elasticsearch.spark.sql")\
.option("es.nodes", "172.31.46.24")\
.option("es.nodes.wan.only", "true")\
.option("es.port", "9200")\
.load("my_index/_doc")
df_test.show()
```
错误：
```
org.elasticsearch.hadoop.EsHadoopIllegalStateException: Field 'user.first' not found; typically this occurs with arrays which are not mapped as single value
```
由于Elasticsearch可以将一个或多个值映射到字段，因此elasticsearch-hadoop无法根据映射确定是实例化一个值还是数组类型（取决于库类型）。因此我们需要显式声明数组字段，可以通过es.read.field.as.array.include设置。
再次查询：
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df_test = sqlContext.read.format("org.elasticsearch.spark.sql")\
.option("es.nodes", "172.31.46.24")\
.option("es.nodes.wan.only", "true")\
.option("es.port", "9200")\
.option("es.read.field.as.array.include", "user, tag")\
.load("my_index/_doc")
df_test.show()
```
错误如下：
```
scala.MatchError: [John,Smith] (of class org.elasticsearch.spark.sql.ScalaEsRow
```
猜测是因为user字段类型不一致导致。
更新文档2：
```
PUT my_index/_doc/2
{
  "group" : "fans",
  "user" : [{
    "first" : "John",
    "last" :  "Smith"
  }],
  "tag": "read"
}
```
文档更新之后的到结果：
```
+-----+-------------+--------------------+
|group|          tag|                user|
+-----+-------------+--------------------+
| fans|       [read]|      [[John,Smith]]|
| fans|[read, write]|[[John,Smith], [A...|
+-----+-------------+--------------------+
```
可以看出，基本类型的数组，单个值写入时是值本身还是数组，对读取没有影响；对于对象数组，最好以数组方式写入。  
### spark读nested array
如果对象数组声明为nested，结果如何呢？
```
PUT my_nested_index
{
    "mappings": {
      "doc": {
        "properties": {
          "group": {
            "type": "text"
          },
          "tag": {
            "type": "text"
          },
          "user": {
            "type": "nested",
            "properties": {
              "first": {
                "type": "text"
              },
              "last": {
                "type": "text"
              }
            }
          }
        }
      }
    }
}

PUT my_nested_index/doc/1
{
  "group" : "fans",
  "user" : [
    {
      "first" : "John",
      "last" :  "Smith"
    },
    {
      "first" : "Alice",
      "last" :  "White"
    }
  ],
  "tag": [
    "read",
    "write"
  ]
}

PUT my_nested_index/doc/2
{
  "group" : "fans",
  "user" : [{
    "last" :  "Smith",
    "first" : "John"
  }],
  "tag": "read"
}
```
查询：
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df_nested = sqlContext.read.format("org.elasticsearch.spark.sql")\
.option("es.nodes", "172.31.46.24")\
.option("es.nodes.wan.only", "true")\
.option("es.port", "9200")\
.option("es.read.field.as.array.include", "user, tag")\
.load("my_nested_index/doc")

df_nested.show()
```
报错：
```
scala.MatchError: [John,Smith] (of class org.elasticsearch.spark.sql.ScalaEsRow
```
将user从es.read.field.as.array.include中去掉，再查询：
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df_nested = sqlContext.read.format("org.elasticsearch.spark.sql")\
.option("es.nodes", "172.31.46.24")\
.option("es.nodes.wan.only", "true")\
.option("es.port", "9200")\
.option("es.read.field.as.array.include", "tag")\
.load("my_nested_index/doc")

df_nested.show()
```
结果：
```
+-----+-------------+--------------------+
|group|          tag|                user|
+-----+-------------+--------------------+
| fans|       [read]|      [[John,Smith]]|
| fans|[read, write]|[[John,Smith], [A...|
+-----+-------------+--------------------+
```
如果在mapping中已经声明user为nested，就不必在es.read.field.as.array.include中包含。

### 结论
1. 数组最好全部以数组形式写入es，方便解析也方便spark读取。
2. 如果类型是数组，读取时设置es.read.field.as.array.include，如果数组定义为nested，则不必设置es.read.field.as.array.include。
3. 合理设置mapping，如果可以，避免复杂的索引结构。
