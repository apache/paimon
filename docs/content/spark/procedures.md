---
title: "Procedures"
weight: 99
type: docs
aliases:
- /spark/procedures.html
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

# Procedures

This section introduce all available spark procedures about paimon.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 4%">Procedure Name</th>
      <th class="text-left" style="width: 20%">Explaination</th>
      <th class="text-left" style="width: 4%">Example</th>
    </tr>
    </thead>
    <tbody style="font-size: 12px; ">
    <tr>
      <td>compact</td>
      <td>
         To compact files. Argument:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>partitions: partition filter. "," means "AND"<br>";" means "OR".If you want to compact one partition with date=01 and day=01, you need to write 'date=01,day=01'. Left empty for all partitions. (Can't be used together with "where")</li>
            <li>where: partition predicate. Left empty for all partitions. (Can't be used together with "partitions")</li>          
            <li>order_strategy: 'order' or 'zorder' or 'hilbert' or 'none'. Left empty for 'none'.</li>
            <li>order_columns: the columns need to be sort. Left empty if 'order_strategy' is 'none'.</li>
      </td>
      <td>
         SET spark.sql.shuffle.partitions=10; --set the compact parallelism <br/>
         CALL sys.compact(table => 'T', partitions => 'p=0;p=1',  order_strategy => 'zorder', order_by => 'a,b') <br/>
         CALL sys.compact(table => 'T', where => 'p>0 and p<3', order_strategy => 'zorder', order_by => 'a,b')
      </td>
    </tr>
    <tr>
      <td>expire_snapshots</td>
      <td>
         To expire snapshots. Argument:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>retain_max: the maximum number of completed snapshots to retain.</li>
            <li>retain_min: the minimum number of completed snapshots to retain.</li>
            <li>older_than: timestamp before which snapshots will be removed.</li>
            <li>max_deletes: the maximum number of snapshots that can be deleted at once.</li>
      </td>
      <td>CALL sys.expire_snapshots(table => 'default.T', retain_max => 10)</td>
    </tr>
    <tr>
      <td>create_tag</td>
      <td>
         To create a tag based on given snapshot. Arguments:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>tag: name of the new tag. Cannot be empty.</li>
            <li>snapshot(Long):  id of the snapshot which the new tag is based on.</li>
      </td>
      <td>
         -- based on snapshot 10 <br/>
         CALL sys.create_tag(table => 'default.T', tag => 'my_tag', snapshot => 10) <br/>
         -- based on the latest snapshot <br/>
         CALL sys.create_tag(table => 'default.T', tag => 'my_tag')
      </td>
    </tr>
    <tr>
      <td>delete_tag</td>
      <td>
         To delete a tag. Arguments:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>tag: name of the tag to be deleted.</li>
      </td>
      <td>CALL sys.delete_tag(table => 'default.T', tag => 'my_tag')</td>
    </tr>
    <tr>
      <td>rollback</td>
      <td>
         To rollback to a specific version of target table. Argument:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>version: id of the snapshot or name of tag that will roll back to.</li>
      </td>
      <td>
          CALL sys.rollback(table => 'default.T', version => 'my_tag')<br/>
          CALL sys.rollback(table => 'default.T', version => 10)
      </td>
    </tr>
    <tr>
      <td>remove_orphan_files</td>
      <td>
         To remove the orphan data files and metadata files. Arguments:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>older_than: to avoid deleting newly written files, this procedure only deletes orphan files older than 1 day by default. This argument can modify the interval.</li>
      </td>
      <td>
          CALL sys.remove_orphan_files(table => 'default.T', older_than => '2023-10-31 12:00:00')
      </td>
    </tr>
    </tbody>
</table>
