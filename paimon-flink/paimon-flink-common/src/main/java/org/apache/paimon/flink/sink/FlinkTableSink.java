/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.abilities.SupportsTruncate;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

/** Table sink to create sink. */
public class FlinkTableSink extends SupportsRowLevelOperationFlinkTableSink
        implements SupportsTruncate {

    public FlinkTableSink(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        super(tableIdentifier, table, context, logStoreTableFactory);
    }

    @Override
    public void executeTruncation() {
        try (BatchTableCommit batchTableCommit = table.newBatchWriteBuilder().newCommit()) {
            batchTableCommit.truncateTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
