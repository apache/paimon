---
title: "Kafka CDC"
weight: 3
type: docs
aliases:
- /cdc-ingestion/kafka-cdc.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kafka CDC

## Prepare Kafka Bundled Jar

```
flink-sql-connector-kafka-*.jar
```

## Supported Formats
Flink provides several Kafka CDC formats: Canal Json, Debezium Json, Debezium Avro, Ogg Json, Maxwell Json and Normal Json.
If a message in a Kafka topic is a change event captured from another database using the Change Data Capture (CDC) tool, then you can use the Paimon Kafka CDC. Write the INSERT, UPDATE, DELETE messages parsed into the paimon table.
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Formats</th>
        <th class="text-left">Supported</th>
      </tr>
    </thead>
    <tbody>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/canal/">Canal CDC</a></td>
          <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/debezium/">Debezium CDC</a></td>
         <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/maxwell/">Maxwell CDC</a></td>
        <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/ogg/">OGG CDC</a></td>
        <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/json/">JSON</a></td>
        <td>True</td>
        </tr>
        <tr>
         <td><a href="https://docs.aws.amazon.com/dms/latest/userguide/Welcome.html">aws-dms-json</a></td>
        <td>True</td>
        </tr>
        <tr>
         <td>debezium-bson</td>
        <td>True</td>
        </tr>
    </tbody>
</table>

{{< hint info >}}
The JSON sources possibly missing some information. For example, Ogg and Maxwell format standards don't contain field 
types; When you write JSON sources into Flink Kafka sink, it will only reserve data and row type and drop other information. 
The synchronization job will try best to handle the problem as follows:
1. Usually, debezium-json contains 'schema' field, from which Paimon will retrieve data types. Make sure your debezium 
json has this field, or Paimon will use 'STRING' type.
2. If missing field types, Paimon will use 'STRING' type as default. 
3. If missing database name or table name, you cannot do database synchronization, but you can still do table synchronization.
4. If missing primary keys, the job might create non primary key table. You can set primary keys when submit job in table 
synchronization.
{{< /hint >}}

## Synchronizing Tables

By using [KafkaSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from Kafka's one topic into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--type_mapping to-string] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--metadata_column <metadata-column>] \
    [--kafka_conf <kafka-source-conf> [--kafka_conf <kafka-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/kafka_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified Kafka topic's tables,it gets the earliest non-DDL data parsing schema from topic. If the Paimon table already exists, its schema will be compared against the schema of all specified Kafka topic's tables.

Example 1:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

If the kafka topic doesn't contain message when you start the synchronization job, you must manually create the table
before submitting the job. You can define the partition keys and primary keys only, and the left columns will be added
by the synchronization job.

NOTE: In this case you shouldn't use --partition_keys or --primary_keys, because those keys are defined when creating
the table and can not be modified. Additionally, if you specified computed columns, you should also define all the argument
columns used for computed columns.

Example 2:
If you want to synchronize a table which has primary key 'id INT', and you want to compute a partition key 'part=date_format(create_time,yyyy-MM-dd)',
you can create a such table first (the other columns can be omitted):

```sql
CREATE TABLE test_db.test_table (
    id INT,                 -- primary key
    create_time TIMESTAMP,  -- the argument of computed column part
    part STRING,            -- partition key
    PRIMARY KEY (id, part) NOT ENFORCED
) PARTITIONED BY (part);
```

Then you can submit synchronization job:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --computed_column 'part=date_format(create_time,yyyy-MM-dd)' \
    ... (other conf)
```

Example 3: 
For some append data (such as log data), it can be treated as special CDC data with only INSERT operation type, so you can use 'format=json' to synchronize such data to the Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --computed_column 'pt=date_format(event_tm, yyyyMMdd)' \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=test_log \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf sink.parallelism=4
```

## Synchronizing Databases

By using [KafkaSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the multi topic or one topic into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_database \
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table_mapping <table-name>=<paimon-table-name1> [--table_mapping <table-name2>=<paimon-table-name2> ...]] \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--table_prefix_db <db-name1>=<table-prefix1> [--table_prefix_db <db-name2>=<table-prefix2> ...]] \
    [--table_suffix_db <db-name1>=<table-suffix1> [--table_suffix_db <db-name2>=<table-suffix2> ...]] \
    [--including_tables <table-name|name-regular-expr>] \
    [--excluding_tables <table-name|name-regular-expr>] \
    [--including_dbs <database-name|name-regular-expr>] \
    [--excluding_dbs <database-name|name-regular-expr>] \
    [--type_mapping to-string] \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--metadata_column <metadata-column>] \
    [--kafka_conf <kafka-source-conf> [--kafka_conf <kafka-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/kafka_sync_database >}}

This action will build a single combined sink for all tables. For each Kafka topic's table to be synchronized, if the
corresponding Paimon table does not exist, this action will automatically create the table, and its schema will be derived
from all specified Kafka topic's tables. If the Paimon table already exists and its schema is different from that parsed
from Kafka record, this action will try to preform schema evolution.

Example

Synchronization from one Kafka topic to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --computed_column 'pt=date_format(event_tm, yyyyMMdd)'
```

Synchronization from multiple Kafka topics to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order\;logistic_order\;user \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

## Additional kafka_config

There are some useful options to build Flink Kafka Source, but they are not provided by flink-kafka-connector document. They are:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Key</th>
        <th class="text-left">Default</th>
        <th class="text-left">Type</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
        <tr>
          <td>schema.registry.url</td>
          <td>(none)</td>
          <td>String</td>
          <td>When configuring "value.format=debezium-avro" which requires using the Confluence schema registry model for Apache Avro serialization, you need to provide the schema registry URL.</td>
        </tr>
    </tbody>
</table>

## Debezium-bson

The debezium-bson format is one of the formats supported by <a href="{{< ref "/cdc-ingestion/kafka-cdc" >}}">Kafka CDC</a>.
It is the format obtained by collecting mongodb through debezium, which is similar to
<a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/debezium/">debezium-json</a> format.
However, MongoDB does not have a fixed schema, and the field types of each document may be different, so the before/after fields
in JSON are all string types, while the debezium-json format requires a JSON object type.

MongoDB BSON Jar can be downloaded from the [Maven repository](https://mvnrepository.com/artifact/org.mongodb/bson)

```
bson-*.jar
```

{{< hint info >}}
The debezium bson format requires insert/update/delete event messages include the full document, and include a field that represents the state of the document before the change.
This requires setting debezium's capture.mode to change_streams_update_full_with_pre_image and [capture.mode.full.update.type](https://debezium.io/documentation/reference/stable/connectors/mongodb.html#mongodb-property-capture-mode-full-update-type) to post_image.
Before version 6.0 of MongoDB, it was not possible to obtain 'Update Before' information. Therefore, using the id field in the Kafka Key as 'Update before' information
{{< /hint >}}

Here is a simple example for an update operation captured from a Mongodb customers collection in JSON format:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "string",
        "optional": true,
        "name": "io.debezium.data.Json",
        "version": 1,
        "field": "before"
      },
      {
        "type": "string",
        "optional": true,
        "name": "io.debezium.data.Json",
        "version": 1,
        "field": "after"
      },
      ...
    ]
  },
  "payload": {
    "before": "{\"_id\": {\"$oid\" : \"596e275826f08b2730779e1f\"}, \"name\" : \"Anne\", \"create_time\" : {\"$numberLong\" : \"1558965506000\"}, \"tags\":[\"success\"]}",
    "after": "{\"_id\": {\"$oid\" : \"596e275826f08b2730779e1f\"}, \"name\" : \"Anne\", \"create_time\" : {\"$numberLong\" : \"1558965506000\"}, \"tags\":[\"passion\",\"success\"]}",
    "source": {
      "db": "inventory",
      "rs": "rs0",
      "collection": "customers",
      ...
    },
    "op": "u",
    "ts_ms": 1558965515240,
    "ts_us": 1558965515240142,
    "ts_ns": 1558965515240142879
  }
}
```

This document from the MongoDB collection customers has 4 columns, the _id is a BSON ObjectID, name is a string,
create_time is a long, tags is an array of string. The following is the processing result in debezium-bson format:

Document Schema:

| Field Name | Field Type | Key         |
|------------|------------|-------------|
| _id        | STRING     | Primary Key |
| name       | STRING     |             |
| create_time| STRING     |             |
| tags       | STRING     |             |

Records:

| RowKind | _id                      | name  | create_time                | tags                  |
|---------|--------------------------|-------|----------------------------|-----------------------|
|   -U    | 596e275826f08b2730779e1f | Anne  | 1558965506000              | ["success"]           |
|   +U    | 596e275826f08b2730779e1f | Anne  | 1558965506000              | ["passion","success"] |

Because the schema field of the event message does not have the field information of the document, the debezium-bson format does not require event messages to have schema information. The specific operations are as follows:

- Parse the before/after fields of the event message into BSONDocument.
- Recursive traversal all fields of BSONDocument and convert BsonValue to Java Object.
- All top-level fields of before/after are converted to string type, and _id is fixed to primary key
- If the top-level fields of before/after is a basic type(such as Integer/Long, etc.), it is directly converted to a string, if not, it is converted to a JSON string

Below is a list of top-level field BsonValue conversion examples:

<table class="configuration table table-bordered">
    <thead>
    <tr>
        <th class="text-left" style="width: 20%">BsonValue Type</th>
        <th class="text-left" style="width: 40%">Json Value</th>
        <th class="text-left" style="width: 40%">Conversion Result String</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td><h5>BsonString</h5></td>
        <td>"hello"</td>
        <td>"hello"</td>
    </tr>
    <tr>
        <td><h5>BsonInt32</h5></td>
        <td>123</td>
        <td>"123"</td>
    </tr>
    <tr>
        <td><h5>BsonInt64</h5></td>
        <td>
            <ul>
                <li>1735934393769</li>
                <li>{"$numberLong": "1735934393769"}</li>
            </ul>
        </td>
        <td>"1735934393769"</td>
    </tr>
    <tr>
        <td><h5>BsonDouble</h5></td>
        <td>
            <ul>
                <li>{"$numberDouble": "3.14"}</li>
                <li>{"$numberDouble": "NaN"}</li>
                <li>{"$numberDouble": "Infinity"}</li>
                <li>{"$numberDouble": "-Infinity"}</li>
            </ul>
        </td>
        <td>
            <ul>
                <li>"3.14"</li>
                <li>"NaN"</li>
                <li>"Infinity"</li>
                <li>"-Infinity"</li>
            </ul>
        </td>
    </tr>
    <tr>
        <td><h5>BsonBoolean</h5></td>
        <td>
            <ul>
                <li>true</li>
                <li>false</li>
            </ul>
        </td>
        <td>
            <ul>
                <li>"true"</li>
                <li>"false"</li>
            </ul>
        </td>
    </tr>
    <tr>
        <td><h5>BsonArray</h5></td>
        <td>[1,2,{"$numberLong": "1735934393769"}]</td>
        <td>"[1,2,1735934393769]"</td>
    </tr>
    <tr>
        <td><h5>BsonObjectId</h5></td>
        <td>{"$oid": "596e275826f08b2730779e1f"}</td>
        <td>"596e275826f08b2730779e1f"</td>
    </tr>
    <tr>
        <td><h5>BsonDateTime</h5></td>
        <td>{"$date": 1735934393769 }</td>
        <td>"1735934393769"</td>
    </tr>
    <tr>
        <td><h5>BsonNull</h5></td>
        <td>null</td>
        <td>null</td>
    </tr>
    <tr>
        <td><h5>BsonUndefined</h5></td>
        <td>{"$undefined": true}</td>
        <td>null</td>
    </tr>
    <tr>
        <td><h5>BsonBinary</h5></td>
        <td>{"$binary": "uE2/4v5MSVOiJZkOo3APKQ==", "$type": "0"}</td>
        <td>"uE2/4v5MSVOiJZkOo3APKQ=="</td>
    </tr>
    <tr>
        <td><h5>BsonBinary(type=UUID)</h5></td>
        <td>{"$binary": "uE2/4v5MSVOiJZkOo3APKQ==", "$type": "4"}</td>
        <td>"b84dbfe2-fe4c-4953-a225-990ea3700f29"</td>
    </tr>
    <tr>
        <td><h5>BsonDecimal128</h5></td>
        <td>
            <ul>
                <li>{"$numberDecimal": "3.14"}</li>
                <li>{"$numberDecimal": "NaN"}</li>
            </ul>
        </td>
        <td>
            <ul>
                <li>"3.14"</li>
                <li>"NaN"</li>
            </ul>
        </td>
    </tr>
    <tr>
        <td><h5>BsonRegularExpression</h5></td>
        <td>{"$regularExpression": {"pattern": "^pass$", "options": "i"}}</td>
        <td>"/^pass$/i"</td>
    </tr>
    <tr>
        <td><h5>BsonSymbol</h5></td>
        <td>{"$symbol": "symbol"}</td>
        <td>"symbol"</td>
    </tr>
    <tr>
        <td><h5>BsonTimestamp</h5></td>
        <td>{"$timestamp": {"t": 1736997330, "i": 2}}</td>
        <td>"1736997330"</td>
    </tr>
    <tr>
        <td><h5>BsonMinKey</h5></td>
        <td>{"$minKey": 1}</td>
        <td>"BsonMinKey"</td>
    </tr>
    <tr>
        <td><h5>BsonMaxKey</h5></td>
        <td>{"$maxKey": 1}</td>
        <td>"BsonMaxKey"</td>
    </tr>
    <tr>
        <td><h5>BsonJavaScript</h5></td>
        <td>{"$code": "function(){}"}</td>
        <td>"function(){}"</td>
    </tr>
    <tr>
        <td><h5>BsonJavaScriptWithScope</h5></td>
        <td>{"$code": "function(){}", "$scope": {"name": "Anne"}}</td>
        <td>'{"$code": "function(){}", "$scope": {"name": "Anne"}}'</td>
    </tr>
    <tr>
        <td><h5>BsonDocument</h5></td>
        <td>
<pre>
{
  "decimalPi": {"$numberDecimal": "3.14"},
  "doublePi": {"$numberDouble": "3.14"},
  "doubleNaN": {"$numberDouble": "NaN"},
  "decimalNaN": {"$numberDecimal": "NaN"},
  "long": {"$numberLong": "100"},
  "bool": true,
  "array": [
    {"$numberInt": "1"},
    {"$numberLong": "2"}
  ]
}
</pre>
        </td>
        <td>
<pre>
'{
  "decimalPi":3.14,
  "doublePi":3.14,
  "doubleNaN":"NaN",
  "decimalNaN":"NaN",
  "long":100,
  "bool":true,
  "array":[1,2]
}'
</pre>
        </td>
    </tr>
    </tbody>
</table>
