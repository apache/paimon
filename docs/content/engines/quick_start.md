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


# Quick Start
This document describes how to use Docker and Docker Compose 
to quickly build a big data environment such as Paimon and 
the big data engine Flink/Spark. 
You can see more runtime information between Paimon and 
Flink/Spark by looking at the corresponding section in the 
documentation.

The following steps have been tested on a Windows 11 laptop 
with docker desktop and docker compose installed.

## Prerequisites
windows/Linux/MacOS

docker desktop for windows/Applicable to the docker of the corresponding system

docker compose

## Quick start paimon and flink

Look at the version mapping between paimon and Flink 
in the [Engines/Flink](https://paimon.apache.org/docs/master/engines/flink/) section of the document and select 
the version you want.

### Docker compose

docker compose is a convenient way to manage docker containers and quickly build the application environment 
you need. The following docker compose file is composed of three services, jobmanager, taskmanager and sql-client
, using the flink:1.17.1-scala_2.12 image.

Copy the following into a ```flink_docker_compose.yml``` file.

> Notice:
> 
> The /tmp path in the container is mapped to the local./tmp path so that multiple containers can share a file system path. The reason is because the 'warehouse'='file:/tmp/paimon' option is configured when the catalog is created. If you use another path as the warehouse address, modify the volumes option in the docker compose file simultaneously.

```shell

version: "2.2"
services:
  jobmanager:
    image: flink:1.17.1-scala_2.12
    ports:
      - "8081:8081"
    command: jobmanager
    user: flink
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    networks:
      - flink-network
    volumes:
      - ./custom_jars/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar:/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
      - ./custom_jars/paimon-flink-1.17-0.5-20230815.001819-123.jar:/opt/flink/lib/paimon-flink-1.17-0.5-20230815.001819-123.jar
      - ./tmp:/tmp

  taskmanager:
    image: flink:1.17.1-scala_2.12
    depends_on:
      - jobmanager
    command: taskmanager
    user: flink
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
    networks:
      - flink-network
    volumes:
      - ./custom_jars/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar:/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
      - ./custom_jars/paimon-flink-1.17-0.5-20230815.001819-123.jar:/opt/flink/lib/paimon-flink-1.17-0.5-20230815.001819-123.jar
      - ./tmp:/tmp

  sql-client:
    image: flink:1.17.1-scala_2.12
    command: bin/sql-client.sh
    user: flink
    depends_on:
      - jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager
    networks:
      - flink-network
    volumes:
      - ./custom_jars/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar:/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
      - ./custom_jars/paimon-flink-1.17-0.5-20230815.001819-123.jar:/opt/flink/lib/paimon-flink-1.17-0.5-20230815.001819-123.jar
      - ./tmp:/tmp

networks:
  flink-network:
    driver: bridge

```

### Step 1: Prepare the dependent jar files

Reading [Engines/Flink](https://paimon.apache.org/docs/master/engines/flink/) chapters, Download the appropriate versions of the paimon-flink-*.jar and flink-shaded-hadoop-2-uber-*.jar files to the specified path

For example, ./incubator-paimon/docker/custom_jars.

After creating a new custom_jars folder under the path./incubator-paimon/docker/, place the required jar files into it.

The file I downloaded here is flink-shaded-hadoop-2-uber-2.8.3-10.0.jar and paimon-flink-1.17-0.5-20230815.001819-123.jar

With the jar files ready, you next need to place the ```flink_docker_compose.yml``` docker compose file in the ```./incubator-paimon/docker/``` path.

We can then quickly start the cluster through docker compose.

### Step 2: Start the flink cluster with docker compose
Open a CMD window and run the commands shown below.

#### 2.1 Switch to the specified path
```shell
cd ./incubator-paimon/docker/
```

#### 2.2 Start the Flink cluster
```shell
PS .\incubator-paimon\docker> docker-compose -f flink_docker_compose.yml up -d jobmanager taskmanager
[+] Running 3/3
 - Network docker_flink-network    Created                                                                                                                                                                  0.1s
 - Container docker-jobmanager-1   Started                                                                                                                                                                  1.1s
 - Container docker-taskmanager-1  Started                                                                                                                                                                  1.5s
```
You can now access the Flink Web UI at ```http://localhost:8081/``` .

#### 2.3 Use interactive mode to start an SQL-client for executing SQL
```shell
                                                                                                                                   
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

Flink SQL>
```
Now you have an sql client that can interact.

Here you can continue to execute a variety of SQL code.

### Step 3: Create the paimon catalog and write and query the data

#### 3.1 Create Catalog

Use CREATE CATALOG my_catalog WITH (...) Statement creates a catalog named my_catalog.

> Note the 'warehouse'='file:/tmp/paimon' configuration here, which should be mentioned above in docker compose.
```shell
Flink SQL> CREATE CATALOG my_catalog WITH (
>     'type'='paimon',
>     'warehouse'='file:/tmp/paimon'
> );
[INFO] Execute statement succeed.

Flink SQL> USE CATALOG my_catalog;
[INFO] Execute statement succeed.

```

#### 3.2 Create Table

```shell
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

```

#### 3.3 Insert Data To a Table

```shell
Flink SQL> INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word;
[INFO] Submitting SQL update statement to the cluster...
Flink SQL> INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word;[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 19bc54af8dbbf44a90aae79e8534af2e

```

You should see a running job in the Flink Web UI. In the Flink Web UI you can view the detailed runtime information of the job.

#### 3.4 Query data from a table

```shell

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

Use ctrl+c to exit.
