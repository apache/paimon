---
title: "Debezium BSON"
weight: 6
type: docs
aliases:
- /cdc-ingestion/debezium-bson.html
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

# Debezium BSON Format


The debezium-bson format is one of the formats supported by <a href="{{< ref "/cdc-ingestion/kafka-cdc" >}}">Kafka CDC</a>.
It is the format obtained by collecting mongodb through debezium, which is similar to 
<a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/debezium/">debezium-json</a> format.
However, MongoDB does not have a fixed schema, and the field types of each document may be different, so the before/after fields
in JSON are all string types, while the debezium-json format requires a JSON object type.


## Prepare MongoDB BSON Jar

Can be downloaded from the [Maven repository](https://mvnrepository.com/artifact/org.mongodb/bson)

```
bson-*.jar
```

## Introduction

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


### How it works
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


### How to use
Use debezium-bson by adding the kafka_conf parameter **value.format=debezium-bson**. Let’s take table synchronization as an example:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table ods_mongodb_customers \
    --primary_keys _id \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=customers \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=debezium-bson \
    --catalog_conf metastore=filesystem \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```


