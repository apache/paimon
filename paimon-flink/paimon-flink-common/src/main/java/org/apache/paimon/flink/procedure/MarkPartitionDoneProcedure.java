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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.partition.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.flink.sink.partition.PartitionMarkDone.markDone;
import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Partition mark done procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.mark_partition_done('tableId', 'partition1', 'partition2', ...)
 * </code></pre>
 */
public class MarkPartitionDoneProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "mark_partition_done";

    public String[] call(
            ProcedureContext procedureContext, String tableId, String... partitionStrings)
            throws Catalog.TableNotExistException, IOException {
        checkArgument(
                partitionStrings.length > 0,
                "mark_partition_done procedure must specify partitions.");

        Identifier identifier = Identifier.fromString(tableId);
        Table table = catalog.getTable(identifier);
        checkArgument(
                table instanceof FileStoreTable,
                "Only FileStoreTable supports mark_partition_done procedure. The table type is '%s'.",
                table.getClass().getName());

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        CoreOptions coreOptions = fileStoreTable.coreOptions();
        List<PartitionMarkDoneAction> actions =
                PartitionMarkDoneAction.createActions(fileStoreTable, coreOptions);

        List<String> partitionPaths =
                PartitionPathUtils.generatePartitionPaths(
                        getPartitions(partitionStrings), fileStoreTable.store().partitionType());

        markDone(partitionPaths, actions);

        IOUtils.closeAllQuietly(actions);

        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
