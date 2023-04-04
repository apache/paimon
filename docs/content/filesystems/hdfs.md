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

For HDFS, the most important thing is to be able to read your HDFS configuration. Choose one of the following ways to configure your HDFS:

- Set environment variable `HADOOP_HOME` (if in a hadoop environment, it is already set up).
- Configure `'fs.hdfs.hadoopconf'` in the catalog.
- Set environment variable `HADOOP_CONF_DIR`.
- Configure `'hadoop.'` prefix options in the catalog.

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

## Kerberos

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [Kerberos configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html).
