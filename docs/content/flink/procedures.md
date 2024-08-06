---
title: "Procedures"
weight: 97
type: docs
aliases:
- /flink/procedures.html
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

Flink 1.18 and later versions support [Call Statements](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/call/),
which make it easier to manipulate data and metadata of Paimon table by writing SQLs instead of submitting Flink jobs.

In 1.18, the procedure only supports passing arguments by position. You must pass all arguments in order, and if you
don't want to pass some arguments, you must use `''` as placeholder. For example, if you want to compact table `default.t`
with parallelism 4, but you don't want to specify partitions and sort strategy, the call statement should be \
`CALL sys.compact('default.t', '', '', '', 'sink.parallelism=4')`.

In higher versions, the procedure supports passing arguments by name. You can pass arguments in any order and any optional
argument can be omitted. For the above example, the call statement is \
``CALL sys.compact(`table` => 'default.t', options => 'sink.parallelism=4')``.

Specify partitions: we use string to represent partition filter. "," means "AND" and ";" means "OR". For example, if you want
to specify two partitions date=01 and date=02, you need to write 'date=01;date=02'; If you want to specify one partition
with date=01 and day=01, you need to write 'date=01,day=01'.

Table options syntax: we use string to represent table options. The format is 'key1=value1,key2=value2...'.

All available procedures are listed below.

<table class="table table-bordered">
   <thead>
   <tr>
      <th class="text-left" style="width: 4%">Procedure Name</th>
      <th class="text-left" style="width: 4%">Usage</th>
      <th class="text-left" style="width: 20%">Explanation</th>
      <th class="text-left" style="width: 4%">Example</th>
   </tr>
   </thead>
   <tbody style="font-size: 11px; ">
   <tr>
      <td>compact</td>
      <td>
      </td>
      <td>
         To compact a table. Arguments:
            <li>table(required): the target table identifier.</li>
            <li>partitions(optional): partition filter.</li>
            <li>order_strategy(optional): 'order' or 'zorder' or 'hilbert' or 'none'.</li>
            <li>order_by(optional): the columns need to be sort. Left empty if 'order_strategy' is 'none'.</li>
            <li>options(optional): additional dynamic options of the table.</li>
            <li>where(optional): partition predicate(Can't be used together with "partitions"). Note: as where is a keyword,a pair of backticks need to add around like `where`.</li>
      </td>
      <td>
         -- use partition filter <br/>
         CALL sys.compact(`table` => 'default.T', partitions => 'p=0', order_strategy => 'zorder', order_by => 'a,b', options => 'sink.parallelism=4') <br/>
         -- use partition predicate <br/>
         CALL sys.compact(`table` => 'default.T', `where` => 'dt>10 and h<20', order_strategy => 'zorder', order_by => 'a,b', options => 'sink.parallelism=4')
      </td>
   </tr>
   <tr>
      <td>compact_database</td>
      <td>
         CALL [catalog.]sys.compact_database() <br/><br/>
         CALL [catalog.]sys.compact_database('includingDatabases') <br/><br/> 
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode') <br/><br/> 
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables') <br/><br/> 
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables') <br/><br/>
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables', 'tableOptions')
      </td>
      <td>
         To compact databases. Arguments:
            <li>includingDatabases: to specify databases. You can use regular expression.</li>
            <li>mode: compact mode. "divided": start a sink for each table, detecting the new table requires restarting the job;
               "combined" (default): start a single combined sink for all tables, the new table will be automatically detected.
            </li>
            <li>includingTables: to specify tables. You can use regular expression.</li>
            <li>excludingTables: to specify tables that are not compacted. You can use regular expression.</li>
            <li>tableOptions: additional dynamic options of the table.</li>
      </td>
      <td>
         CALL sys.compact_database('db1|db2', 'combined', 'table_.*', 'ignore', 'sink.parallelism=4')
      </td>
   </tr>
   <tr>
      <td>create_tag</td>
      <td>
         -- based on the specified snapshot <br/>
         CALL [catalog.]sys.create_tag('identifier', 'tagName', snapshotId) <br/>
         -- based on the latest snapshot <br/>
         CALL [catalog.]sys.create_tag('identifier', 'tagName')
      </td>
      <td>
         To create a tag based on given snapshot. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>tagName: name of the new tag.</li>
            <li>snapshotId (Long): id of the snapshot which the new tag is based on.</li>
            <li>time_retained: The maximum time retained for newly created tags.</li>
      </td>
      <td>
         CALL sys.create_tag('default.T', 'my_tag', 10, '1 d')
      </td>
   </tr>
   <tr>
      <td>delete_tag</td>
      <td>
         CALL [catalog.]sys.delete_tag('identifier', 'tagName')
      </td>
      <td>
         To delete a tag. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>tagName: name of the tag to be deleted. If you specify multiple tags, delimiter is ','.</li>
      </td>
      <td>
         CALL sys.delete_tag('default.T', 'my_tag')
      </td>
   </tr>
   <tr>
      <td>merge_into</td>
      <td>
         -- when matched then upsert<br/>
         CALL [catalog.]sys.merge_into('identifier','targetAlias',<br/>
            'sourceSqls','sourceTable','mergeCondition',<br/>
            'matchedUpsertCondition','matchedUpsertSetting')<br/><br/>
         -- when matched then upsert; when not matched then insert<br/>
         CALL [catalog.]sys.merge_into('identifier','targetAlias',<br/>
            'sourceSqls','sourceTable','mergeCondition',<br/>
            'matchedUpsertCondition','matchedUpsertSetting',<br/>
            'notMatchedInsertCondition','notMatchedInsertValues')<br/><br/>
         -- when matched then delete<br/>
         CALL [catalog].sys.merge_into('identifier','targetAlias',<br/>
            'sourceSqls','sourceTable','mergeCondition',<br/>
            'matchedDeleteCondition')<br/><br/>
         -- when matched then upsert + delete;<br/> 
         -- when not matched then insert<br/>
         CALL [catalog].sys.merge_into('identifier','targetAlias',<br/>
            'sourceSqls','sourceTable','mergeCondition',<br/>
            'matchedUpsertCondition','matchedUpsertSetting',<br/>
            'notMatchedInsertCondition','notMatchedInsertValues',<br/>
            'matchedDeleteCondition')<br/><br/>
      </td>
      <td>
         To perform "MERGE INTO" syntax. See <a href="/how-to/writing-tables#merging-into-table">merge_into action</a> for
         details of arguments.
      </td>
      <td>
         -- for matched order rows,<br/>
         -- increase the price,<br/>
         -- and if there is no match,<br/> 
         -- insert the order from<br/>
         -- the source table<br/>
         CALL sys.merge_into('default.T', '', '', 'default.S', 'T.id=S.order_id', '', 'price=T.price+20', '', '*')
      </td>
   </tr>
   <tr>
      <td>remove_orphan_files</td>
      <td>
         CALL [catalog.]sys.remove_orphan_files('identifier')<br/><br/>
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan')<br/><br/>
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun')
      </td>
      <td>
         To remove the orphan data files and metadata files. Arguments:
            <li>identifier: the target table identifier. Cannot be empty, you can use database_name.* to clean whole database.</li>
            <li>olderThan: to avoid deleting newly written files, this procedure only 
               deletes orphan files older than 1 day by default. This argument can modify the interval.
            </li>
            <li>dryRun: when true, view only orphan files, don't actually remove files. Default is false.</li>
      </td>
      <td>CALL remove_orphan_files('default.T', '2023-10-31 12:00:00')<br/><br/>
          CALL remove_orphan_files('default.*', '2023-10-31 12:00:00')<br/><br/>
          CALL remove_orphan_files('default.T', '2023-10-31 12:00:00', true)
      </td>
   </tr>
   <tr>
      <td>reset_consumer</td>
      <td>
         -- reset the new next snapshot id in the consumer<br/>
         CALL [catalog.]sys.reset_consumer('identifier', 'consumerId', nextSnapshotId)<br/><br/>
         -- delete consumer<br/>
         CALL [catalog.]sys.reset_consumer('identifier', 'consumerId')
      </td>
      <td>
         To reset or delete consumer. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>consumerId: consumer to be reset or deleted.</li>
            <li>nextSnapshotId (Long): the new next snapshot id of the consumer.</li>
      </td>
      <td>CALL sys.reset_consumer('default.T', 'myid', 10)</td>
   </tr>
   <tr>
      <td>rollback_to</td>
      <td>
         -- rollback to a snapshot<br/>
         CALL sys.rollback_to('identifier', snapshotId)<br/><br/>
         -- rollback to a tag<br/>
         CALL sys.rollback_to('identifier', 'tagName')
      </td>
      <td>
         To rollback to a specific version of target table. Argument:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>snapshotId (Long): id of the snapshot that will roll back to.</li>
            <li>tagName: name of the tag that will roll back to.</li>
      </td>
      <td>CALL sys.rollback_to('default.T', 10)</td>
   </tr>
   <tr>
      <td>expire_snapshots</td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.expire_snapshots(table, retain_max)<br/><br/>
         -- for Flink 1.19 and later<br/>
         CALL sys.expire_snapshots(table, retain_max, retain_min, older_than, max_deletes)<br/><br/>
      </td>
      <td>
         To expire snapshots. Argument:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>retain_max: the maximum number of completed snapshots to retain.</li>
            <li>retain_min: the minimum number of completed snapshots to retain.</li>
            <li>order_than: timestamp before which snapshots will be removed.</li>
            <li>max_deletes: the maximum number of snapshots that can be deleted at once.</li>
      </td>
      <td>
         -- for Flink 1.18<br/><br/>
         CALL sys.expire_snapshots('default.T', 2)<br/><br/>
         -- for Flink 1.19 and later<br/><br/>
         CALL sys.expire_snapshots(`table` => 'default.T', retain_max => 2)<br/><br/>
         CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00')<br/><br/>
         CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00', retain_min => 10)<br/><br/>
         CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00', max_deletes => 10)<br/><br/>
      </td>
   </tr>
<tr>
      <td>expire_partitions</td>
      <td>
         CALL sys.expire_partitions(table, expiration_time, timestamp_formatter, expire_strategy)<br/><br/>
      </td>
      <td>
         To expire partitions. Argument:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>expiration_time: the expiration interval of a partition. A partition will be expired if it‘s lifetime is over this value. Partition time is extracted from the partition value.</li>
            <li>timestamp_formatter: the formatter to format timestamp from string.</li>
            <li>timestamp_pattern: the pattern to get a timestamp from partitions.</li>
            <li>expire_strategy: specifies the expiration strategy for partition expiration, possible values: 'values-time' or 'update-time' , 'values-time' as default.</li>
      </td>
      <td>
         -- for Flink 1.18<br/><br/>
         CALL sys.expire_partitions('default.T', '1 d', 'yyyy-MM-dd', 'values-time')<br/><br/>
         -- for Flink 1.19 and later<br/><br/>
         CALL sys.expire_partitions(`table` => 'default.T', expiration_time => '1 d', timestamp_formatter => 'yyyy-MM-dd', expire_strategy => 'values-time')<br/>
         CALL sys.expire_partitions(`table` => 'default.T', expiration_time => '1 d', timestamp_formatter => 'yyyy-MM-dd HH:mm', timestamp_pattern => '$dt $hm', expire_strategy => 'values-time')<br/><br/>
      </td>
   </tr>
    <tr>
      <td>repair</td>
      <td>
         -- repair all databases and tables in catalog<br/>
         CALL sys.repair()<br/><br/>
         -- repair all tables in a specific database<br/>
         CALL sys.repair('databaseName')<br/><br/>
         -- repair a table<br/>
         CALL sys.repair('databaseName.tableName')<br/><br/>
         -- repair database and table in a string if you specify multiple tags, delimiter is ','<br/>
         CALL sys.repair('databaseName01,database02.tableName01,database03')
      </td>
      <td>
         Synchronize information from the file system to Metastore. Argument:
            <li>empty: all databases and tables in catalog.</li>
            <li>databaseName : the target database name.</li>
            <li>tableName: the target table identifier.</li>
      </td>
      <td>CALL sys.repair('test_db.T')</td>
   </tr>
    <tr>
      <td>rewrite_file_index</td>
      <td>
         CALL sys.rewrite_file_index(&ltidentifier&gt [, &ltpartitions&gt])<br/><br/>
      </td>
      <td>
         Rewrite the file index for the table. Argument:
            <li>identifier: &ltdatabaseName&gt.&lttableName&gt.</li>
            <li>partitions : specific partitions.</li>
      </td>
      <td>
         -- rewrite the file index for the whole table<br/>
         CALL sys.rewrite_file_index('test_db.T')<br/><br/>
         -- repair all tables in a specific partition<br/>
         CALL sys.rewrite_file_index('test_db.T', 'pt=a')<br/><br/>
     </td>
   <tr>
      <td>create_branch</td>
      <td>
         -- based on the specified snapshot <br/>
         CALL [catalog.]sys.create_branch('identifier', 'branchName', snapshotId) <br/>
         -- based on the specified tag <br/>
         CALL [catalog.]sys.create_branch('identifier', 'branchName', 'tagName')
         -- create empty branch <br/>
         CALL [catalog.]sys.create_branch('identifier', 'branchName')
      </td>
      <td>
         To create a branch based on given snapshot / tag, or just create empty branch. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>branchName: name of the new branch.</li>
            <li>snapshotId (Long): id of the snapshot which the new branch is based on.</li>
            <li>tagName: name of the tag which the new branch is based on.</li>
      </td>
      <td>
         CALL sys.create_branch('default.T', 'branch1', 10)<br/><br/>
         CALL sys.create_branch('default.T', 'branch1', 'tag1')<br/><br/>
         CALL sys.create_branch('default.T', 'branch1')<br/><br/>
      </td>
   </tr>
   <tr>
      <td>delete_branch</td>
      <td>
         CALL [catalog.]sys.delete_branch('identifier', 'branchName')
      </td>
      <td>
         To delete a branch. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>branchName: name of the branch to be deleted. If you specify multiple branches, delimiter is ','.</li>
      </td>
      <td>
         CALL sys.delete_branch('default.T', 'branch1')
      </td>
   </tr>
   <tr>
      <td>fast_forward</td>
      <td>
         CALL [catalog.]sys.fast_forward('identifier', 'branchName')
      </td>
      <td>
         To fast_forward a branch to main branch. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>branchName: name of the branch to be merged.</li>
      </td>
      <td>
         CALL sys.fast_forward('default.T', 'branch1')
      </td>
   </tr>
   </tbody>
</table>
