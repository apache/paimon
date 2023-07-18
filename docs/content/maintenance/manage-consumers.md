---
title: "Manage Consumers"
weight: 9
type: docs
aliases:
- /maintenance/manage-consumers.html
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

# Manage Consumers

## Record Consumer

You can add or reset a consumer with a given consumer ID and next snapshot ID.

{{< hint info >}}
First, you need to stop the streaming task using this consumer ID, and then execute the record consumer action job.
{{< /hint >}}

{{< tabs "record-consumer" >}}

{{< tab "Flink" >}}

 Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    record-consumer \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --consumer-id <consumer-id> \
    --snapshot <next-snapshot-id> \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]
```

{{< /tab >}}

{{< tab "Java API" >}}

```java
import org.apache.paimon.table.Table;

public class RecordConsumer {

    public static void main(String[] args) {
        Table table = ...;
        table.recordConsumer("myid", 1);
    }
}
```

{{< /tab >}}

{{< /tabs >}}
