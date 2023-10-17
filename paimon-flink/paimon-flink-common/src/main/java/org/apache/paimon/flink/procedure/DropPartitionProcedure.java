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
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.UUID;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Drop partition procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.drop_partition('tableId', 'partition1', 'partition2', ...)
 * </code></pre>
 */
public class DropPartitionProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "drop_partition";

    public String[] call(
            ProcedureContext procedureContext, String tableId, String... partitionStrings)
            throws Catalog.TableNotExistException {
        checkArgument(
                partitionStrings.length > 0, "drop-partition procedure must specify partitions.");

        FileStoreTable fileStoreTable =
                (FileStoreTable) catalog.getTable(Identifier.fromString(tableId));
        FileStoreCommit commit = fileStoreTable.store().newCommit(UUID.randomUUID().toString());
        commit.dropPartitions(getPartitions(partitionStrings), BatchWriteBuilder.COMMIT_IDENTIFIER);

        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
