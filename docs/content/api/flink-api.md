---
title: "Flink API"
weight: 2
type: docs
aliases:
- /api/flink-api.html
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

# Flink API

## Dependency

Maven dependency:

```xml
<dependency>
  <groupId>org.apache.paimon</groupId>
  <artifactId>paimon-flink-1.17</artifactId>
  <version>{{< version >}}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge_2.12</artifactId>
  <version>1.17.0</version>
  <scope>provided</scope>
</dependency>
```

Or download the jar file:
{{< stable >}}[Paimon Flink](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.17/{{< version >}}/paimon-flink-1.17-{{< version >}}.jar).{{< /stable >}}
{{< unstable >}}[Paimon Flink](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/{{< version >}}/).{{< /unstable >}}

Please choose your Flink version.

Paimon relies on Hadoop environment, you should add hadoop classpath or bundled jar.

Paimon does not provide a DataStream API, but you can read or write to Paimon tables by the conversion between DataStream and Table in Flink.
See [DataStream API Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/data_stream_api/).

## Write to Table 

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class WriteToTable {

    public static void writeTo() {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a changelog DataStream
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100))
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"name", "age"},
                                        Types.STRING, Types.INT));

        // interpret the DataStream as a Table
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();
        Table table = tableEnv.fromChangelogStream(dataStream, schema);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='...')");
        tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);

        // insert into paimon table from your data stream table
        tableEnv.executeSql("INSERT INTO sink_paimon_table SELECT * FROM InputTable");
    }
}
```

## Read from Table

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ReadFromTable {

    public static void readFrom() throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='...')");
        tableEnv.executeSql("USE CATALOG paimon");

        // convert to DataStream
        Table table = tableEnv.sqlQuery("SELECT * FROM my_paimon_table");
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

        // use this datastream
        dataStream.executeAndCollect().forEachRemaining(System.out::println);

        // prints:
        // +I[Bob, 12]
        // +I[Alice, 12]
        // -U[Alice, 12]
        // +U[Alice, 14]
    }
}
```
