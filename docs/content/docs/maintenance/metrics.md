---
title: "Metrics"
weight: 4
type: docs
aliases:
- /maintenance/metrics.html
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

## Metrics

We add the metrics for flink table store so that users could collect them and send to external systems by flink metrics reporter.


<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Name</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>newDataFiles</h5></td>
            <td>The num of the new data files.</td>
        </tr>
        <tr>
            <td><h5>changeLogFiles</h5></td>
            <td>The num of the changelog files.</td>
        </tr>
        <tr>
            <td><h5>compactBeforeFiles</h5></td>
            <td>The num of files before compaction.</td>
        </tr>
        <tr>
            <td><h5>compactAfterFiles</h5></td>
            <td>The num of files after compaction.</td>
        </tr>
        <tr>
            <td><h5>compactChangelogFiles</h5></td>
            <td>The num of changelog files on compaction.</td>
        </tr>
    </tbody>
</table>
