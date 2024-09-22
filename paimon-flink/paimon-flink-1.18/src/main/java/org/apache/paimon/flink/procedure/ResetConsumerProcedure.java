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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Reset consumer procedure. Usage:
 *
 * <pre><code>
 *  -- reset the new next snapshot id in the consumer
 *  CALL sys.reset_consumer('tableId', 'consumerId', nextSnapshotId)
 *
 *  -- delete consumer
 *  CALL sys.reset_consumer('tableId', 'consumerId')
 * </code></pre>
 */
public class ResetConsumerProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "reset_consumer";

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String consumerId,
            long nextSnapshotId)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable =
                (FileStoreTable) catalog.getTable(Identifier.fromString(tableId));
        ConsumerManager consumerManager =
                new ConsumerManager(
                        fileStoreTable.fileIO(),
                        fileStoreTable.location(),
                        fileStoreTable.snapshotManager().branch());
        consumerManager.resetConsumer(consumerId, new Consumer(nextSnapshotId));

        return new String[] {"Success"};
    }

    public String[] call(ProcedureContext procedureContext, String tableId, String consumerId)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable =
                (FileStoreTable) catalog.getTable(Identifier.fromString(tableId));
        ConsumerManager consumerManager =
                new ConsumerManager(
                        fileStoreTable.fileIO(),
                        fileStoreTable.location(),
                        fileStoreTable.snapshotManager().branch());
        consumerManager.deleteConsumer(consumerId);

        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
