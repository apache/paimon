---
title: "HDFS"
weight: 2
type: docs
aliases:
- /filesystems/hdfs.html
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

# HDFS

You don't need any additional dependencies to access HDFS because you have already taken care of the Hadoop dependencies.

## HDFS Configuration

For HDFS, the most important thing is to be able to read your HDFS configuration. 

{{< tabs "hdfs conf" >}}

{{< tab "Flink/Trino/JavaAPI" >}}

You may not have to do anything, if you are in a hadoop environment. Otherwise pick one of the following ways to
configure your HDFS:

1. Set environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR`.
2. Configure `'fs.hdfs.hadoopconf'` in the paimon catalog.

The first approach is recommended.

{{< /tab >}}

{{< tab "Hive/Spark" >}}

HDFS Configuration is available directly through the computation cluster, see cluster configuration of Hive and Spark for details.

{{< /tab >}}

{{< /tabs >}}

## Hadoop-compatible file systems (HCFS)

All Hadoop file systems are automatically available when the Hadoop libraries are on the classpath.

This way, Paimon seamlessly supports all of Hadoop file systems implementing the `org.apache.hadoop.fs.FileSystem`
interface, and all Hadoop-compatible file systems (HCFS).

- HDFS
- Alluxio (see configuration specifics below)
- XtreemFS
- …

The Hadoop configuration has to have an entry for the required file system implementation in the `core-site.xml` file.

For Alluxio support add the following entry into the core-site.xml file:

```shell
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
```

## HDFS HA

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [HA configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html).

## HDFS ViewFS

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [ViewFs configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html).

## Kerberos

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [Kerberos configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html).
