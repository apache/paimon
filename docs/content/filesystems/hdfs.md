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
2. Configure `'hadoop-conf-dir'` in the paimon catalog.
3. Configure Hadoop options through prefix `'hadoop.'` in the paimon catalog.

The first approach is recommended.

If you do not want to include the value of the environment variable, you can configure `hadoop-conf-loader` to `option`.

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

## Kerberos

{{< tabs "Kerberos" >}}

{{< tab "Flink" >}}

It is recommended to use [Flink Kerberos Keytab](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/security/security-kerberos/).

{{< /tab >}}

{{< tab "Spark" >}}

It is recommended to use [Spark Kerberos Keytab](https://spark.apache.org/docs/latest/security.html#using-a-keytab).

{{< /tab >}}

{{< tab "Hive" >}}

An intuitive approach is to configure Hive's kerberos authentication.

{{< /tab >}}

{{< tab "Trino/JavaAPI" >}}

Configure the following three options in your catalog configuration:

- security.kerberos.login.keytab: Absolute path to a Kerberos keytab file that contains the user credentials.
  Please make sure it is copied to each machine.
- security.kerberos.login.principal: Kerberos principal name associated with the keytab.
- security.kerberos.login.use-ticket-cache: True or false, indicates whether to read from your Kerberos ticket cache.

For JavaAPI:
```
SecurityContext.install(catalogOptions);
```

{{< /tab >}}

{{< /tabs >}}

## HDFS HA

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [HA configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html).

## HDFS ViewFS

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [ViewFs configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html).
