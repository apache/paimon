---
title: "Hive Catalogs"
weight: 5
type: docs
aliases:
- /iceberg/hive-catalogs.html
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

# Hive Catalog

When creating Paimon table, set `'metadata.iceberg.storage' = 'hive-catalog'`.
This option value not only store Iceberg metadata like hadoop-catalog, but also create Iceberg external table in Hive.
This Paimon table can be accessed from Iceberg Hive catalog later.

To provide information about Hive metastore,
you also need to set some (or all) of the following table options when creating Paimon table.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>metadata.iceberg.uri</h5></td>
      <td style="word-wrap: break-word;"></td>
      <td>String</td>
      <td>Hive metastore uri for Iceberg Hive catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hive-conf-dir</h5></td>
      <td style="word-wrap: break-word;"></td>
      <td>String</td>
      <td>hive-conf-dir for Iceberg Hive catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hadoop-conf-dir</h5></td>
      <td style="word-wrap: break-word;"></td>
      <td>String</td>
      <td>hadoop-conf-dir for Iceberg Hive catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.manifest-compression</h5></td>
      <td style="word-wrap: break-word;">snappy</td>
      <td>String</td>
      <td>Compression for Iceberg manifest files.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.manifest-legacy-version</h5></td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Should use the legacy manifest version to generate Iceberg's 1.4 manifest files.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hive-client-class</h5></td>
      <td style="word-wrap: break-word;">org.apache.hadoop.hive.metastore.HiveMetaStoreClient</td>
      <td>String</td>
      <td>Hive client class name for Iceberg Hive Catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.glue.skip-archive</h5></td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip archive for AWS Glue catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hive-skip-update-stats</h5></td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip updating Hive stats.</td>
    </tr>
    </tbody>
</table>

## AWS Glue Catalog

You can use Hive Catalog to connect AWS Glue metastore, you can use set `'metadata.iceberg.hive-client-class'` to
`'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'`.

> **Note:** You can use this [repo](https://github.com/promotedai/aws-glue-data-catalog-client-for-apache-hive-metastore) to build the required jar, include it in your path and configure the AWSCatalogMetastoreClient.
