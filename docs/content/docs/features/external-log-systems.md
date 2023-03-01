---
title: "External Log Systems"
weight: 4
type: docs
aliases:
- /features/external-log-systems.html
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

Aside from [underlying table files]({{< ref "docs/concepts/primary-key-table#changelog-producers" >}}), changelog of Table Store can also be stored into or consumed from an external log system, such as Kafka. By specifying `log.system` table property, users can choose which external log system to use.

If an external log system is used, all records written into table files will also be written into the log system. Changes produced by the streaming queries will thus come from the log system instead of table files.

## Consistency Guarantees

By default, changes in the log systems are visible to consumers only after a snapshot, just like table files. This behavior guarantees the exactly-once semantics. That is, each record is seen by the consumers exactly once.

However, users can also specify the table property `'log.consistency' = 'eventual'` so that changelog written into the log system can be immediately consumed by the consumers, without waiting for the next snapshot. This behavior decreases the latency of changelog, but it can only guarantee the at-least-once semantics (that is, consumers might see duplicated records) due to possible failures.

If `'log.consistency' = 'eventual'` is set, in order to achieve correct results, Table Store source in Flink will automatically adds a "normalize" operator for deduplication. This operator persists the values of each key in states. As one can easily tell, this operator will be very costly and should be avoided.

## Supported Log Systems

### Kafka

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
