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

Create a file named flink_docker_compose.yml and copy the following into it.

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

Reading [Engines/Flink](https://paimon.apache.org/docs/master/engines/flink/) chapters, Download the appropriate versions of the paimon-flink-*.jar and flink-shaded-hadoop-2-uber-*.jar files to the specified path.

For example, ```./incubator-paimon/docker/custom_jars```.

After creating a new custom_jars folder under the path```./incubator-paimon/docker/```, place the required jar files into it.

The file I downloaded here is ```flink-shaded-hadoop-2-uber-2.8.3-10.0.jar``` and ```paimon-flink-1.17-0.5-20230815.001819-123.jar```

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

## Quick start paimon and spark3

Look at the version mapping between paimon and Spark in the [Engines/Spark](https://paimon.apache.org/docs/master/engines/spark3/) section of the document and select the version you want


### Docker compose

docker compose is a convenient way to manage docker containers and quickly build the application environment you need. The following docker compose file is composed of three services,
spark-master、spark-worker-1、spark-sql,using the bitnami/spark:3.3.2 image.

Copy the following into a ```spark3_docker_compose.yml``` file.
> Notice:
>
> The /tmp path in the container is mapped to the local./tmp path so that multiple containers can share a file system path. The reason is because the 'warehouse'='file:/tmp/paimon' option is configured when the catalog is created. If you use another path as the warehouse address, modify the volumes option in the docker compose file simultaneously.

```shell
version: "2.2"
services:
  spark-master:
    image: docker.io/bitnami/spark:3.3.2
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark
    ports:
      - "8080:8080"
      - "7077:7077"
    networks:
      - spark-network
    volumes:
      - ./custom_jars/paimon-spark-3.3-0.5-20230816.001807-122.jar:/opt/bitnami/spark/lib/paimon-spark-3.3-0.5-20230816.001807-122.jar
      - ./tmp:/tmp

  spark-worker-1:
    image: bitnami/spark:3.3.2
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=6G
      - SPARK_WORKER_CORES=6
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark
    networks:
      - spark-network
    volumes:
      - ./custom_jars/paimon-spark-3.3-0.5-20230816.001807-122.jar:/opt/bitnami/spark/lib/paimon-spark-3.3-0.5-20230816.001807-122.jar
      - ./tmp:/tmp

  spark-sql:
    image: bitnami/spark:3.3.2
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark
    command: [ "./bin/spark-sql",
               "--jars", "/opt/bitnami/spark/lib/paimon-spark-3.3-0.5-20230816.001807-122.jar",
               "--conf", "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog",
               "--conf", "spark.sql.catalog.paimon.warehouse=file:/tmp/paimo" ]
    working_dir: /opt/bitnami/spark
    networks:
      - spark-network
    volumes:
      - ./custom_jars/paimon-spark-3.3-0.5-20230816.001807-122.jar:/opt/bitnami/spark/lib/paimon-spark-3.3-0.5-20230816.001807-122.jar
      - ./tmp:/tmp

networks:
  spark-network:
    driver: bridge

```

### Step 1: Prepare the dependent jar files

[https://paimon.apache.org/docs/master/engines/spark3/](https://paimon.apache.org/docs/master/engines/spark3/)

Reading Engines/Flink chapters, Download the appropriate versions of the ```paimon-spark-{{< version >}}.jar``` files to the specified path.

For example,```./incubator-paimon/docker/custom_jars```

The file I downloaded here is ```paimon-spark-3.3-0.5-20230816.001807-122.jar``` .

With the jar files ready, you next need to place the ```spark3_docker_compose.yml``` docker compose file in the ```./incubator-paimon/docker/``` path.

We can then quickly start the cluster through docker compose.

### Step 2: Start the spark3 cluster with docker compose
Open a CMD window and run the commands shown below.

#### 2.1 Switch to the specified path
```shell
cd ./incubator-paimon/docker/
```

#### 2.2 Start the Spark cluster
Start a spark master and a spark worker container.

```shell
PS .\incubator-paimon\docker> docker-compose -f spark3_docker_compose.yml up -d spark-master spark-worker-1
time="2023-08-16T16:36:26+08:00" level=warning msg="Found orphan containers ([docker_sql-client_run_5b32db156b76 docker-taskmanager-1 docker-jobmanager-1]) for this
project. If you removed or renamed this service in your compose file, you can run this command with the --remove-orphans flag to clean it up."
[+] Running 2/2
 - Container docker-spark-master-1    Started                                                                                                                   1.2s
 - Container docker-spark-worker-1-1  Started                                                                                                                   1.7s

```

#### 2.3 Use interactive mode to start an SQL-client for executing SQL
Start a Spark-SQL container connected to the spark cluster.

```shell
PS .\incubator-paimon\docker> docker-compose -f spark3_docker_compose.yml run spark-sql
time="2023-08-16T16:37:34+08:00" level=warning msg="Found orphan containers ([docker_sql-client_run_5b32db156b76 docker-taskmanager-1 docker-jobmanager-1]) for this
project. If you removed or renamed this service in your compose file, you can run this command with the --remove-orphans flag to clean it up."
[+] Running 1/0
 - Container docker-spark-master-1  Running                                                                                                                     0.0s
spark 08:37:35.46 
spark 08:37:35.46 Welcome to the Bitnami spark container
spark 08:37:35.46 Subscribe to project updates by watching https://github.com/bitnami/containers
spark 08:37:35.46 Submit issues and feature requests at https://github.com/bitnami/containers/issues
spark 08:37:35.47 
spark 08:37:35.47 INFO  ==> ** Starting Spark setup **
spark 08:37:35.48 ERROR ==> Invalid mode sql. Supported types are 'master/worker'
PS .\incubator-paimon\docker> docker-compose -f spark3_docker_compose.yml up -d spark-master spark-worker-1
time="2023-08-16T16:40:13+08:00" level=warning msg="Found otime="2023-08-16T16:40:13+08:00" level=warning msg="Found orphan containers ([docker_sql-client_run_5b32db156b76 docker-taskmanager-1 docker-jobmanager-1]) for this project. If you removed or renamed this serv
ice in your compose file, you can run this command with the --remove-orphans flag to clean it up."
[+] Running 2/2
 - Container docker-spark-master-1    Started                                                                                                                   2.5s
 - Container docker-spark-worker-1-1  Started                                                                                                                   2.9s
PS .\incubator-paimon\docker> docker-compose -f spark3_docker_compose.yml run spark-sql
time="2023-08-16T20:22:18+08:00" level=warning msg="Found orphan containers ([docker_sql-client_run_5b32db156b76 docker-taskmanager-1 docker-jobmanager-1]) for this project. If you removed or renamed this serv
ice in your compose file, you can run this command with the --remove-orphans flag to clean it up."
[+] Running 1/0
 - Container docker-spark-master-1  Running                                                                                                                                                                 0.0s
spark 12:22:19.68 
spark 12:22:19.68 Welcome to the Bitnami spark container
spark 12:22:19.68 Subscribe to project updates by watching https://github.com/bitnami/containers
spark 12:22:19.68 Submit issues and feature requests at https://github.com/bitnami/containers/issues
spark 12:22:19.68 

23/08/16 12:22:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/08/16 12:22:24 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
23/08/16 12:22:24 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
23/08/16 12:22:29 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
23/08/16 12:22:29 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore UNKNOWN@192.168.112.4
23/08/16 12:22:29 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
Spark master: local[*], Application Id: local-1692188542905
spark-sql> 
```
Now you have an sql client that can interact.

Here you can continue to execute a variety of SQL code.

### Step 3: Create the paimon catalog and write and query the data

#### 3.1 Run the following SQL to create and switch to database default.
```shell
spark-sql> USE paimon;
23/08/16 12:23:09 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Time taken: 1.425 seconds
spark-sql> USE default;
Time taken: 0.075 seconds

```

#### 3.2 Create a table and Write Some Records

```shell
spark-sql> create table my_table (
         >     k int,
         >     v string
         > ) tblproperties (
         >     'primary-key' = 'k'
         > );
Time taken: 0.156 seconds
spark-sql> INSERT INTO my_table VALUES (1, 'Hi'), (2, 'Hello');
Time taken: 6.633 seconds

```

#### 3.3 Query Table with SQL
```shell
spark-sql> SELECT * FROM my_table;
1       Hi
2       Hello
Time taken: 1.324 seconds, Fetched 2 row(s)
spark-sql> INSERT INTO my_table VALUES (1, 'Hi Again'), (3, 'Test');
Time taken: 3.681 seconds
```

#### 3.4 Update the Records
```shell
spark-sql> SELECT * FROM my_table;
1       Hi Again
2       Hello
3       Test
Time taken: 1.074 seconds, Fetched 3 row(s)
spark-sql> exit;
```
