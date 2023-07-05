---
title: "External Log Systems"
weight: 8
type: docs
aliases:
- /概念/external-log-systems.html
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

# External Log Systems

Aside from [underlying table files]({{< ref "概念/primary-key-table#changelog-producers" >}}), changelog of Paimon can also be stored into or consumed from an external log system, such as Kafka. By specifying `log.system` table property, users can choose which external log system to use.

If an external log system is used, all records written into table files will also be written into the log system. Changes produced by the streaming queries will thus come from the log system instead of table files.

## Consistency Guarantees

By default, changes in the log systems are visible to consumers only after a snapshot, just like table files. This behavior guarantees the exactly-once semantics. That is, each record is seen by the consumers exactly once.

However, users can also specify the table property `'log.consistency' = 'eventual'` so that changelog written into the log system can be immediately consumed by the consumers, without waiting for the next snapshot. This behavior decreases the latency of changelog, but it can only guarantee the at-least-once semantics (that is, consumers might see duplicated records) due to possible failures.

If `'log.consistency' = 'eventual'` is set, in order to achieve correct results, Paimon source in Flink will automatically adds a "normalize" operator for deduplication. This operator persists the values of each key in states. As one can easily tell, this operator will be very costly and should be avoided.

## Supported Log Systems

### Kafka

#### Preparing flink-sql-connector-kafka Jar File

Paimon currently supports Flink 1.17, 1.16, 1.15 and 1.14. We recommend the latest Flink version for a better experience.

Download the flink-sql-connector-kafka jar file with corresponding version.

| Version    | Jar                                                                                                                                                                                |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.17 | [flink-sql-connector-kafka-1.17.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar)                |
| Flink 1.16 | [flink-sql-connector-kafka-1.16.1.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.1/flink-sql-connector-kafka-1.16.1.jar)                |
| Flink 1.15 | [flink-sql-connector-kafka-1.15.4.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.4/flink-sql-connector-kafka-1.15.4.jar)                |
| Flink 1.14 | [flink-sql-connector-kafka_2.11-1.14.4.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.14.4/flink-sql-connector-kafka_2.11-1.14.4.jar) |

By specifying `'log.system' = 'kafka'`, users can write changes into Kafka along with table files.

{{< tabs "kafka-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE T (...)
WITH (
    'log.system' = 'kafka',
    'kafka.bootstrap.servers' = '...',
    'kafka.topic' = '...'
);
```

{{< /tab >}}

{{< /tabs >}}

Table Properties for Kafka are listed as follows.

{{< generated/kafka_log_configuration >}}
