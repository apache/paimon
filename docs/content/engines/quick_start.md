---
title: "Quick Start"
weight: 5
type: docs
aliases:
- /engines/quick_start.html
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


# Qucik Start
This document explains how to use Docker to quickly build a 
practice environment for Paimon and big data engines (Flink,
Spark, Hive, etc.).

The following steps have been tested on a Windows 11 laptop 
with docker desktop installed. 

## Prerequisites
windows 11

docker desktop for windows

## Use paimon in flink
flink version: 1.17.x

### Step 1: Prepare the dependent jar files

[Download](https://flink.apache.org/downloads.html) Pre-bundled Hadoop jar and copy the jar file to the lib directory of your Flink path.

For example, ./incubator-paimon/docker/custom_jars

The file I downloaded here is flink-shaded-hadoop-2-uber-2.8.3-10.0.jar and paimon-flink-1.17-0.5-20230815.001819-123.jar

### Step 2: Start the flink cluster with docker compose
Open a CMD window and run the commands shown below.
```shell
# Switch to the specified path
cd ./incubator-paimon/docker/

# Start the Flink cluster
PS .\incubator-paimon\docker> docker-compose -f flink_docker_compose.yml up -d jobmanager taskmanager
[+] Running 3/3
 - Network docker_flink-network    Created                                                                                                                                                                  0.1s
 - Container docker-jobmanager-1   Started                                                                                                                                                                  1.1s
 - Container docker-taskmanager-1  Started                                                                                                                                                                  1.5s
 
# Use interactive mode to start an SQL-client for executing SQL                                                                                                                                                 
PS .\incubator-paimon\docker> docker-compose -f flink_docker_compose.yml run sql-client
[+] Running 1/0
 - Container docker-jobmanager-1  Running                                                                                                                                                                   0.0s
[0.019s][warning][perf,memops] Cannot use file /tmp/hsperfdata_flink/1 because it is locked by another process (errno = 11)

                                   ▒▓██▓██▒
                               ▓████▒▒█▓▒▓███▓▒
                            ▓███▓░░        ▒▒▒▓██▒  ▒
                          ░██▒   ▒▒▓▓█▓▓▒░      ▒████
                          ██▒         ░▒▓███▒    ▒█▒█▒
                            ░▓█            ███   ▓░▒██
                              ▓█       ▒▒▒▒▒▓██▓░▒░▓▓█
                            █░ █   ▒▒░       ███▓▓█ ▒█▒▒▒
                            ████░   ▒▓█▓      ██▒▒▒ ▓███▒
                         ░▒█▓▓██       ▓█▒    ▓█▒▓██▓ ░█░
                   ▓░▒▓████▒ ██         ▒█    █▓░▒█▒░▒█▒
                  ███▓░██▓  ▓█           █   █▓ ▒▓█▓▓█▒
                ░██▓  ░█░            █  █▒ ▒█████▓▒ ██▓░▒
               ███░ ░ █░          ▓ ░█ █████▒░░    ░█░▓  ▓░
              ██▓█ ▒▒▓▒          ▓███████▓░       ▒█▒ ▒▓ ▓██▓
           ▒██▓ ▓█ █▓█       ░▒█████▓▓▒░         ██▒▒  █ ▒  ▓█▒
           ▓█▓  ▓█ ██▓ ░▓▓▓▓▓▓▓▒              ▒██▓           ░█▒
           ▓█    █ ▓███▓▒░              ░▓▓▓███▓          ░▒░ ▓█
           ██▓    ██▒    ░▒▓▓███▓▓▓▓▓██████▓▒            ▓███  █
          ▓███▒ ███   ░▓▓▒░░   ░▓████▓░                  ░▒▓▒  █▓
          █▓▒▒▓▓██  ░▒▒░░░▒▒▒▒▓██▓░                            █▓
          ██ ▓░▒█   ▓▓▓▓▒░░  ▒█▓       ▒▓▓██▓    ▓▒          ▒▒▓
          ▓█▓ ▓▒█  █▓░  ░▒▓▓██▒            ░▓█▒   ▒▒▒░▒▒▓█████▒
           ██░ ▓█▒█▒  ▒▓▓▒  ▓█                █░      ░░░░   ░█▒
           ▓█   ▒█▓   ░     █░                ▒█              █▓
            █▓   ██         █░                 ▓▓        ▒█▓▓▓▒█░
             █▓ ░▓██░       ▓▒                  ▓█▓▒░░░▒▓█░    ▒█
              ██   ▓█▓░      ▒                    ░▒█▒██▒      ▓▓
               ▓█▒   ▒█▓▒░                         ▒▒ █▒█▓▒▒░░▒██
                ░██▒    ▒▓▓▒                     ▓██▓▒█▒ ░▓▓▓▓▒█▓
                  ░▓██▒                          ▓░  ▒█▓█  ░░▒▒▒
                      ▒▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░▓▓  ▓░▒█░

    ______ _ _       _       _____  ____  _         _____ _ _            _  BETA
   |  ____| (_)     | |     / ____|/ __ \| |       / ____| (_)          | |
   | |__  | |_ _ __ | | __ | (___ | |  | | |      | |    | |_  ___ _ __ | |_
   |  __| | | | '_ \| |/ /  \___ \| |  | | |      | |    | | |/ _ \ '_ \| __|
   | |    | | | | | |   <   ____) | |__| | |____  | |____| | |  __/ | | | |_
   |_|    |_|_|_| |_|_|\_\ |_____/ \___\_\______|  \_____|_|_|\___|_| |_|\__|

        Welcome! Enter 'HELP;' to list all available commands. 'QUIT;' to exit.

Command history file path: /opt/flink/.flink-sql-history

```
### Step 3: Create the paimon catalog and write and query the data
```shell
Flink SQL> CREATE CATALOG my_catalog WITH (
>     'type'='paimon',
>     'warehouse'='file:/tmp/paimon'
> );
[INFO] Execute statement succeed.

Flink SQL> USE CATALOG my_catalog;
[INFO] Execute statement succeed.

Flink SQL> CREATE TABLE word_count (
>     word STRING PRIMARY KEY NOT ENFORCED,
>     cnt BIGINT
> );
[INFO] Execute statement succeed.

Flink SQL> CREATE TEMPORARY TABLE word_table (
>     word STRING
> ) WITH (
>     'connector' = 'datagen',
>     'fields.word.length' = '1'
> );
[INFO] Execute statement succeed.

Flink SQL> SET 'execution.checkpointing.interval' = '10 s';
[INFO] Execute statement succeed.

Flink SQL> INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word;
[INFO] Submitting SQL update statement to the cluster...
Flink SQL> INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word;[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 19bc54af8dbbf44a90aae79e8534af2e


Flink SQL>
> SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.

Flink SQL> RESET 'execution.checkpointing.interval';
[INFO] Execute statement succeed.

Flink SQL> SET 'execution.runtime-mode' = 'batch';
[INFO] Execute statement succeed.

Flink SQL> SELECT * FROM word_count;
+------+-------+
| word |   cnt |
+------+-------+
|    0 | 42632 |
|    1 | 42572 |
|    2 | 42603 |
|    3 | 42560 |
|    4 | 42318 |
|    5 | 42846 |
|    6 | 42606 |
|    7 | 42252 |
|    8 | 42404 |
|    9 | 42460 |
|    a | 42164 |
|    b | 42173 |
|    c | 42466 |
|    d | 42590 |
|    e | 42757 |
|    f | 42597 |
+------+-------+
16 rows in set

Flink SQL>
> SET 'execution.runtime-mode' = 'streaming';
[INFO] Execute statement succeed.

Flink SQL> SELECT `interval`, COUNT(*) AS interval_cnt FROM
Flink SQL> SELECT `interval`, COUNT(*) AS interval_cnt FROMOUP BY `interval`;
>     (SELECT cnt / 10000 AS `interval` FROM word_count) GROUP BY `interval`;+----+----------------------+----------------------+
| op |             interval |         interval_cnt |
+----+----------------------+----------------------+
| +I |                    7 |                    1 |
| -U |                    7 |                    1 |
| +U |                    7 |                    2 |
| -U |                    7 |                    2 |
| +U |                    7 |                    3 |
...
| -U |                    7 |                    9 |
| +U |                    7 |                    8 |
| -U |                    8 |                    7 |
| +U |                    8 |                    8 |
^CQuery terminated, received a total of 94 rows

Flink SQL>
```

