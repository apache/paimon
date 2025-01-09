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
         -- Use named argument<br/>
         CALL [catalog.]sys.compact(
            `table` => 'table', 
            partitions => 'partitions', 
            order_strategy => 'order_strategy', 
            order_by => 'order_by', 
            options => 'options', 
            `where` => 'where', 
            partition_idle_time => 'partition_idle_time',
            compact_strategy => 'compact_strategy') <br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.compact('table') <br/><br/>
         CALL [catalog.]sys.compact('table', 'partitions') <br/><br/> 
         CALL [catalog.]sys.compact('table', 'order_strategy', 'order_by') <br/><br/>
         CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by') <br/><br/>
         CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by', 'options') <br/><br/>
         CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by', 'options', 'where') <br/><br/>
         CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by', 'options', 'where', 'partition_idle_time') <br/><br/>
         CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by', 'options', 'where', 'partition_idle_time', 'compact_strategy') <br/><br/>
      </td>
      <td>
         To compact a table. Arguments:
            <li>table(required): the target table identifier.</li>
            <li>partitions(optional): partition filter.</li>
            <li>order_strategy(optional): 'order' or 'zorder' or 'hilbert' or 'none'.</li>
            <li>order_by(optional): the columns need to be sort. Left empty if 'order_strategy' is 'none'.</li>
            <li>options(optional): additional dynamic options of the table.</li>
            <li>where(optional): partition predicate(Can't be used together with "partitions"). Note: as where is a keyword,a pair of backticks need to add around like `where`.</li>
            <li>partition_idle_time(optional): this is used to do a full compaction for partition which had not received any new data for 'partition_idle_time'. And only these partitions will be compacted. This argument can not be used with order compact.</li>
            <li>compact_strategy(optional): this determines how to pick files to be merged, the default is determined by the runtime execution mode. 'full' strategy only supports batch mode. All files will be selected for merging. 'minor' strategy: Pick the set of files that need to be merged based on specified conditions.</li>
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
         -- Use named argument<br/>
         CALL [catalog.]sys.compact_database(
            including_databases => 'includingDatabases', 
            mode => 'mode', 
            including_tables => 'includingTables', 
            excluding_tables => 'excludingTables', 
            table_options => 'tableOptions', 
            partition_idle_time => 'partitionIdleTime',
            compact_strategy => 'compact_strategy') <br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.compact_database() <br/><br/>
         CALL [catalog.]sys.compact_database('includingDatabases') <br/><br/> 
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode') <br/><br/> 
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables') <br/><br/> 
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables') <br/><br/>
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables', 'tableOptions') <br/><br/>
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables', 'tableOptions', 'partitionIdleTime')<br/><br/>
         CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables', 'tableOptions', 'partitionIdleTime', 'compact_strategy')<br/><br/>
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
            <li>partition_idle_time: this is used to do a full compaction for partition which had not received any new data for 'partition_idle_time'. And only these partitions will be compacted.</li>
            <li>compact_strategy(optional): this determines how to pick files to be merged, the default is determined by the runtime execution mode. 'full' strategy only supports batch mode. All files will be selected for merging. 'minor' strategy: Pick the set of files that need to be merged based on specified conditions.</li>
      </td>
      <td>
         CALL sys.compact_database(
            including_databases => 'db1|db2', 
            mode => 'combined', 
            including_tables => 'table_.*', 
            excluding_tables => 'ignore', 
            table_options => 'sink.parallelism=4',
            compat_strategy => 'full')
      </td>
   </tr>
   <tr>
      <td>create_tag</td>
      <td>
         -- Use named argument<br/>
         -- based on the specified snapshot <br/>
         CALL [catalog.]sys.create_tag(`table` => 'identifier', tag => 'tagName', snapshot_id => snapshotId) <br/>
         -- based on the latest snapshot <br/>
         CALL [catalog.]sys.create_tag(`table` => 'identifier', tag => 'tagName') <br/><br/>
         -- Use indexed argument<br/>
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
         CALL sys.create_tag(`table` => 'default.T', tag => 'my_tag', snapshot_id => cast(10 as bigint), time_retained => '1 d')
      </td>
   </tr>
    <tr>
      <td>create_tag_from_timestamp</td>
      <td>
         -- Create a tag from the first snapshot whose commit-time greater than the specified timestamp. <br/>
         -- Use named argument<br/>
         CALL [catalog.]sys.create_tag_from_timestamp(`table` => 'identifier', tag => 'tagName', timestamp => timestamp, time_retained => time_retained) <br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.create_tag_from_timestamp('identifier', 'tagName', timestamp, time_retained)
      </td>
      <td>
         To create a tag based on given timestamp. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>tag: name of the new tag.</li>
            <li>timestamp (Long): Find the first snapshot whose commit-time greater than this timestamp.</li>
            <li>time_retained : The maximum time retained for newly created tags.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.create_tag_from_timestamp('default.T', 'my_tag', 1724404318750, '1 d')
         -- for Flink 1.19 and later<br/>
         CALL sys.create_tag_from_timestamp(`table` => 'default.T', `tag` => 'my_tag', `timestamp` => 1724404318750, time_retained => '1 d')
      </td>
   </tr>
    <tr>
      <td>create_tag_from_watermark</td>
      <td>
         -- Create a tag from the first snapshot whose watermark greater than the specified timestamp.<br/>
         -- Use named argument<br/>
         CALL [catalog.]sys.create_tag_from_watermark(`table` => 'identifier', tag => 'tagName', watermark => watermark, time_retained => time_retained) <br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.create_tag_from_watermark('identifier', 'tagName', watermark, time_retained)
      </td>
      <td>
         To create a tag based on given watermark timestamp. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>tag: name of the new tag.</li>
            <li>watermark (Long): Find the first snapshot whose watermark greater than the specified watermark.</li>
            <li>time_retained : The maximum time retained for newly created tags.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.create_tag_from_watermark('default.T', 'my_tag', 1724404318750, '1 d')
         -- for Flink 1.19 and later<br/>
         CALL sys.create_tag_from_watermark(`table` => 'default.T', `tag` => 'my_tag', `watermark` => 1724404318750, time_retained => '1 d')
      </td>
   </tr>
   <tr>
      <td>delete_tag</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.delete_tag(`table` => 'identifier', tag => 'tagName') <br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.delete_tag('identifier', 'tagName')
      </td>
      <td>
         To delete a tag. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>tagName: name of the tag to be deleted. If you specify multiple tags, delimiter is ','.</li>
      </td>
      <td>
         CALL sys.delete_tag(`table` => 'default.T', tag => 'my_tag')
      </td>
   </tr>
   <tr>
      <td>replace_tag</td>
      <td>
         -- Use named argument<br/>
         -- replace tag with new time retained <br/>
         CALL [catalog.]sys.replace_tag(`table` => 'identifier', tag => 'tagName', time_retained => 'timeRetained') <br/>
         -- replace tag with new snapshot id and time retained <br/>
         CALL [catalog.]sys.replace_tag(`table` => 'identifier', snapshot_id => 'snapshotId') <br/><br/>
         -- Use indexed argument<br/>
         -- replace tag with new snapshot id and time retained <br/>
         CALL [catalog.]sys.replace_tag('identifier', 'tagName', 'snapshotId', 'timeRetained') <br/>
      </td>
      <td>
         To replace an existing tag with new tag info. Arguments:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>tag: name of the existed tag. Cannot be empty.</li>
            <li>snapshot(Long):  id of the snapshot which the tag is based on, it is optional.</li>
            <li>time_retained: The maximum time retained for the existing tag, it is optional.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.replace_tag('default.T', 'my_tag', 5, '1 d')<br/><br/>
         -- for Flink 1.19 and later<br/>
         CALL sys.replace_tag(`table` => 'default.T', tag => 'my_tag', snapshot_id => 5, time_retained => '1 d')<br/><br/>
      </td>
   </tr>
   <tr>
      <td>expire_tags</td>
      <td>
         CALL [catalog.]sys.expire_tags('identifier', 'older_than')
      </td>
      <td>
         To expire tags by time. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>older_than: tagCreateTime before which tags will be removed.</li>
      </td>
      <td>
         CALL sys.expire_tags(table => 'default.T', older_than => '2024-09-06 11:00:00')
      </td>
   </tr>
   <tr>
      <td>merge_into</td>
      <td>
         -- for Flink 1.18<br/>
         CALL [catalog].sys.merge_into('identifier','targetAlias',<br/>
            'sourceSqls','sourceTable','mergeCondition',<br/>
            'matchedUpsertCondition','matchedUpsertSetting',<br/>
            'notMatchedInsertCondition','notMatchedInsertValues',<br/>
            'matchedDeleteCondition')<br/><br/>
         -- for Flink 1.19 and later <br/>
         CALL [catalog].sys.merge_into(<br/>
            target_table => 'identifier',<br/>
            target_alias => 'targetAlias',<br/>
            source_sqls => 'sourceSqls',<br/>
            source_table => 'sourceTable',<br/>
            merge_condition => 'mergeCondition',<br/>
            matched_upsert_condition => 'matchedUpsertCondition',<br/>
            matched_upsert_setting => 'matchedUpsertSetting',<br/>
            not_matched_insert_condition => 'notMatchedInsertCondition',<br/>
            not_matched_insert_values => 'notMatchedInsertValues',<br/>
            matched_delete_condition => 'matchedDeleteCondition',<br/>
            not_matched_by_source_upsert_condition => 'notMatchedBySourceUpsertCondition',<br/>
            not_matched_by_source_upsert_setting => 'notMatchedBySourceUpsertSetting',<br/>
            not_matched_by_source_delete_condition => 'notMatchedBySourceDeleteCondition') <br/><br/>
      </td>
      <td>
         To perform "MERGE INTO" syntax. See <a href="/flink/action-jars#merging-into-table">merge_into action</a> for
         details of arguments.
      </td>
      <td>
         -- for matched order rows,<br/>
         -- increase the price,<br/>
         -- and if there is no match,<br/> 
         -- insert the order from<br/>
         -- the source table<br/>
         -- for Flink 1.18<br/>
         CALL [catalog].sys.merge_into('default.T','','','default.S','T.id=S.order_id','','price=T.price+20','','*','')<br/><br/>
         -- for Flink 1.19 and later <br/>
         CALL sys.merge_into(<br/>
            target_table => 'default.T',<br/>
            source_table => 'default.S',<br/>
            merge_condition => 'T.id=S.order_id',<br/>
            matched_upsert_setting => 'price=T.price+20',<br/>
            not_matched_insert_values => '*')<br/><br/>
      </td>
   </tr>
   <tr>
      <td>remove_orphan_files</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.remove_orphan_files(`table` => 'identifier', older_than => 'olderThan', dry_run => 'dryRun', mode => 'mode') <br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.remove_orphan_files('identifier')<br/><br/>
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan')<br/><br/>
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun')<br/><br/>
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun','parallelism')<br/><br/>
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun','parallelism','mode')
      </td>
      <td>
         To remove the orphan data files and metadata files. Arguments:
            <li>identifier: the target table identifier. Cannot be empty, you can use database_name.* to clean whole database.</li>
            <li>olderThan: to avoid deleting newly written files, this procedure only 
               deletes orphan files older than 1 day by default. This argument can modify the interval.
            </li>
            <li>dryRun: when true, view only orphan files, don't actually remove files. Default is false.</li>
            <li>parallelism: The maximum number of concurrent deleting files. By default is the number of processors available to the Java virtual machine.</li>
            <li>mode: The mode of remove orphan clean procedure (local or distributed) . By default is distributed.</li>
      </td>
      <td>CALL remove_orphan_files(`table` => 'default.T', older_than => '2023-10-31 12:00:00')<br/><br/>
          CALL remove_orphan_files(`table` => 'default.*', older_than => '2023-10-31 12:00:00')<br/><br/>
          CALL remove_orphan_files(`table` => 'default.T', older_than => '2023-10-31 12:00:00', dry_run => true)<br/><br/>
          CALL remove_orphan_files(`table` => 'default.T', older_than => '2023-10-31 12:00:00', dry_run => false, parallelism => '5')<br/><br/>
          CALL remove_orphan_files(`table` => 'default.T', older_than => '2023-10-31 12:00:00', dry_run => false, parallelism => '5', mode => 'local')
      </td>
   </tr>
   <tr>
      <td>reset_consumer</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.reset_consumer(`table` => 'identifier', consumer_id => 'consumerId', next_snapshot_id => 'nextSnapshotId') <br/><br/>
         -- Use indexed argument<br/>
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
      <td>CALL sys.reset_consumer(`table` => 'default.T', consumer_id => 'myid', next_snapshot_id => cast(10 as bigint))</td>
   </tr>
   <tr>
      <td>rollback_to</td>
      <td>
         -- for Flink 1.18<br/>
         -- rollback to a snapshot<br/>
         CALL sys.rollback_to('identifier', snapshotId)<br/><br/>
         -- rollback to a tag<br/>
         CALL sys.rollback_to('identifier', 'tagName')<br/><br/>
         -- for Flink 1.19 and later<br/>
         -- rollback to a snapshot<br/>
         CALL sys.rollback_to(`table` => 'identifier', snapshot_id => snapshotId)<br/><br/>
         -- rollback to a tag<br/>
         CALL sys.rollback_to(`table` => 'identifier', tag => 'tagName')
      </td>
      <td>
         To rollback to a specific version of target table. Argument:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>snapshotId (Long): id of the snapshot that will roll back to.</li>
            <li>tagName: name of the tag that will roll back to.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.rollback_to('default.T', 10)
         -- for Flink 1.19 and later<br/>
         CALL sys.rollback_to(`table` => 'default.T', snapshot_id => 10)
      </td>
   </tr>
   <tr>
      <td>rollback_to_timestamp</td>
      <td>
         -- for Flink 1.18<br/>
         -- rollback to the snapshot which earlier or equal than timestamp.<br/>
         CALL sys.rollback_to_timestamp('identifier', timestamp)<br/><br/>
         -- for Flink 1.19 and later<br/>
         -- rollback to the snapshot which earlier or equal than timestamp.<br/>
         CALL sys.rollback_to_timestamp(`table` => 'default.T', `timestamp` => timestamp)<br/><br/>
      </td>
      <td>
         To rollback to the snapshot which earlier or equal than timestamp. Argument:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>timestamp (Long): Roll back to the snapshot which earlier or equal than timestamp.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.rollback_to_timestamp('default.T', 10)
         -- for Flink 1.19 and later<br/>
         CALL sys.rollback_to_timestamp(`table` => 'default.T', timestamp => 1730292023000)
      </td>
   </tr>
   <tr>
          <td>rollback_to_watermark</td>
      <td>
         -- for Flink 1.18<br/>
         -- rollback to the snapshot which earlier or equal than watermark.<br/>
         CALL sys.rollback_to_watermark('identifier', watermark)<br/><br/>
         -- for Flink 1.19 and later<br/>
         -- rollback to the snapshot which earlier or equal than watermark.<br/>
         CALL sys.rollback_to_watermark(`table` => 'default.T', `watermark` => watermark)<br/><br/>
      </td>
      <td>
         To rollback to the snapshot which earlier or equal than watermark. Argument:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>watermark (Long): Roll back to the snapshot which earlier or equal than watermark.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.rollback_to_watermark('default.T', 1730292023000)
         -- for Flink 1.19 and later<br/>
         CALL sys.rollback_to_watermark(`table` => 'default.T', watermark => 1730292023000)
      </td>
   </tr>
   <tr>
          <td>purge_files</td>
      <td>
         -- for Flink 1.18<br/>
         -- clear table with purge files directly.<br/>
         CALL sys.purge_files('identifier')<br/><br/>
         -- for Flink 1.19 and later<br/>
         -- clear table with purge files directly.<br/>
         CALL sys.purge_files(`table` => 'default.T')<br/><br/>
      </td>
      <td>
         To clear table with purge files directly. Argument:
            <li>identifier: the target table identifier. Cannot be empty.</li>
      </td>
      <td>
         -- for Flink 1.18<br/>
         CALL sys.purge_files('default.T')
         -- for Flink 1.19 and later<br/>
         CALL sys.purge_files(`table` => 'default.T')
      </td>
   </tr>
   <tr>
      <td>expire_snapshots</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.expire_snapshots(<br/>
            `table` => 'identifier', <br/>
            retain_max => 'retain_max', <br/>
            retain_min => 'retain_min', <br/>
            older_than => 'older_than', <br/>
            max_deletes => 'max_deletes') <br/><br/>
         -- Use indexed argument<br/>
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
         CALL sys.expire_partitions('default.T', '1 d', 'yyyy-MM-dd', '$dt', 'values-time')<br/><br/>
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
      <td>CALL sys.repair(`table` => 'test_db.T')</td>
   </tr>
    <tr>
      <td>rewrite_file_index</td>
      <td>
         -- Use named argument<br/>
         CALL sys.rewrite_file_index(&lt`table` => identifier&gt [, &ltpartitions => partitions&gt])<br/><br/>
         -- Use indexed argument<br/>
         CALL sys.rewrite_file_index(&ltidentifier&gt [, &ltpartitions&gt])<br/><br/>
      </td>
      <td>
         Rewrite the file index for the table. Argument:
            <li>identifier: &ltdatabaseName&gt.&lttableName&gt.</li>
            <li>partitions : specific partitions.</li>
      </td>
      <td>
         -- rewrite the file index for the whole table<br/>
         CALL sys.rewrite_file_index(`table` => 'test_db.T')<br/><br/>
         -- repair all tables in a specific partition<br/>
         CALL sys.rewrite_file_index(`table` => 'test_db.T', partitions => 'pt=a')<br/><br/>
     </td>
   <tr>
      <td>create_branch</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.create_branch(`table` => 'identifier', branch => 'branchName', tag => 'tagName')<br/><br/>
         -- Use indexed argument<br/>
         -- based on the specified tag <br/>
         CALL [catalog.]sys.create_branch('identifier', 'branchName', 'tagName')
         -- create empty branch <br/>
         CALL [catalog.]sys.create_branch('identifier', 'branchName')
      </td>
      <td>
         To create a branch based on given tag, or just create empty branch. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>branchName: name of the new branch.</li>
            <li>tagName: name of the tag which the new branch is based on.</li>
      </td>
      <td>
         CALL sys.create_branch(`table` => 'default.T', branch => 'branch1', tag => 'tag1')<br/><br/>
         CALL sys.create_branch(`table` => 'default.T', branch => 'branch1')<br/><br/>
      </td>
   </tr>
   <tr>
      <td>delete_branch</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.delete_branch(`table` => 'identifier', branch => 'branchName')<br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.delete_branch('identifier', 'branchName')
      </td>
      <td>
         To delete a branch. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>branchName: name of the branch to be deleted. If you specify multiple branches, delimiter is ','.</li>
      </td>
      <td>
         CALL sys.delete_branch(`table` => 'default.T', branch => 'branch1')
      </td>
   </tr>
   <tr>
      <td>fast_forward</td>
      <td>
         -- Use named argument<br/>
         CALL [catalog.]sys.fast_forward(`table` => 'identifier', branch => 'branchName')<br/><br/>
         -- Use indexed argument<br/>
         CALL [catalog.]sys.fast_forward('identifier', 'branchName')
      </td>
      <td>
         To fast_forward a branch to main branch. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>branchName: name of the branch to be merged.</li>
      </td>
      <td>
         CALL sys.fast_forward(`table` => 'default.T', branch => 'branch1')
      </td>
   </tr>
   </tbody>
</table>
