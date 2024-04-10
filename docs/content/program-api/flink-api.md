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

{{< hint warning >}}
We do not recommend using programming API. Paimon is designed for SQL first, unless you are a professional Flink developer, even if you do, it can be very difficult.

We strongly recommend that you use Flink SQL or Spark SQL, or simply use SQL APIs in programs.

The following documents are not detailed and are for reference only.
{{< /hint >}}

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
  <artifactId>flink-table-api-java-bridge</artifactId>
  <version>1.17.0</version>
  <scope>provided</scope>
</dependency>
```

Or download the jar file:
{{< stable >}}[Paimon Flink](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.17/{{< version >}}/paimon-flink-1.17-{{< version >}}.jar).{{< /stable >}}
{{< unstable >}}[Paimon Flink](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/{{< version >}}/).{{< /unstable >}}

Please choose your Flink version.

Paimon relies on Hadoop environment, you should add hadoop classpath or bundled jar.

Not only DataStream API, you can also read or write to Paimon tables by the conversion between DataStream and Table in Flink.
See [DataStream API Integration](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/data_stream_api/).

## Write to Table 

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class WriteToTable {

    public static void writeTo() throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        // env.enableCheckpointing(60_000);

        // create a changelog DataStream
        DataStream<Row> input =
                env.fromElements(
                                Row.ofKind(RowKind.INSERT, "Alice", 12),
                                Row.ofKind(RowKind.INSERT, "Bob", 5),
                                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100))
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"name", "age"}, Types.STRING, Types.INT));

        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "/path/to/warehouse");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalog.getTable(Identifier.create("my_db", "T"));

        DataType inputType =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()));
        FlinkSinkBuilder builder = new FlinkSinkBuilder(table).forRow(input, inputType);

        // set sink parallelism
        // builder.parallelism(_your_parallelism)

        // set overwrite mode
        // builder.overwrite(...)

        builder.build();
        env.execute();
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

## Cdc ingestion Table

Paimon supports ingest data into Paimon tables with schema evolution.
- You can use Java API to write cdc records into Paimon Tables.
- You can write records to Paimon's partial-update table with adding columns dynamically.

Here is an example to use `RichCdcSinkBuilder` API:

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcSinkBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.paimon.types.RowKind.INSERT;

public class WriteCdcToTable {

    public static void writeTo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        // env.enableCheckpointing(60_000);

        DataStream<RichCdcRecord> dataStream =
                env.fromElements(
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "123")
                                .field("price", DataTypes.DOUBLE(), "62.2")
                                .build(),
                        // dt field will be added with schema evolution
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "245")
                                .field("price", DataTypes.DOUBLE(), "82.1")
                                .field("dt", DataTypes.TIMESTAMP(), "2023-06-12 20:21:12")
                                .build());

        Identifier identifier = Identifier.create("my_db", "T");
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "/path/to/warehouse");
        Catalog.Loader catalogLoader = 
                () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        
        new RichCdcSinkBuilder()
                .withInput(dataStream)
                .withTable(createTableIfNotExists(catalogLoader.load(), identifier))
                .withIdentifier(identifier)
                .withCatalogLoader(catalogLoader)
                .build();

        env.execute();
    }

    private static Table createTableIfNotExists(Catalog catalog, Identifier identifier) throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("order_id");
        schemaBuilder.column("order_id", DataTypes.BIGINT());
        schemaBuilder.column("price", DataTypes.DOUBLE());
        Schema schema = schemaBuilder.build();
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        }
        return catalog.getTable(identifier);
    }
}
```