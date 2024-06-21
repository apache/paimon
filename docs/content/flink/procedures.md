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
      <th class="text-left" style="width: 20%">Explaination</th>
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
      </td>
      <td>
         CALL sys.compact(`table` => 'default.T', partitions => 'p=0', order_strategy => 'zorder', order_by => 'a,b', options => 'sink.parallelism=4')
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
            <li>tagName: name of the tag to be deleted.</li>
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
         CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan')
      </td>
      <td>
         To remove the orphan data files and metadata files. Arguments:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>olderThan: to avoid deleting newly written files, this procedure only 
               deletes orphan files older than 1 day by default. This argument can modify the interval.
            </li>
      </td>
      <td>CALL remove_orphan_files('default.T', '2023-10-31 12:00:00')</td>
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
         -- expires snapshot<br/>
         CALL sys.expire_snapshots('identifier', retainMax)<br/><br/>
      </td>
      <td>
         To expire snapshots. Argument:
            <li>identifier: the target table identifier. Cannot be empty.</li>
            <li>retainMax: the maximum number of completed snapshots to retain.</li>
      </td>
      <td>CALL sys.expire_snapshots('default.T', 2)</td>
   </tr>
   </tbody>
</table>
