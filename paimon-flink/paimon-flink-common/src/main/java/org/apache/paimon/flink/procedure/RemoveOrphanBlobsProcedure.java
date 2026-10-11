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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.orphan.FlinkManagedBlobOrphanFilesClean;
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.operation.LocalManagedBlobOrphanFilesClean;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Locale;

import static org.apache.paimon.flink.orphan.FlinkManagedBlobOrphanFilesClean.validateParallelism;
import static org.apache.paimon.operation.OrphanFilesClean.olderThanMillis;

/**
 * Remove orphan managed BLOB packs procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.remove_orphan_blobs('tableId')
 *
 *  CALL sys.remove_orphan_blobs('tableId', '2023-12-31 23:59:59')
 *
 *  CALL sys.remove_orphan_blobs('databaseName.*', '2023-12-31 23:59:59')
 * </code></pre>
 */
public class RemoveOrphanBlobsProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "remove_orphan_blobs";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "older_than",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "dry_run", type = @DataTypeHint("BOOLEAN"), isOptional = true),
                @ArgumentHint(name = "parallelism", type = @DataTypeHint("INT"), isOptional = true),
                @ArgumentHint(name = "mode", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String olderThan,
            Boolean dryRun,
            Integer parallelism,
            String mode)
            throws Exception {
        validateParallelism(parallelism);
        Identifier identifier = Identifier.fromString(tableId);
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();
        if (mode == null) {
            mode = "DISTRIBUTED";
        }
        CleanOrphanFilesResult result;
        try {
            switch (mode.toUpperCase(Locale.ROOT)) {
                case "DISTRIBUTED":
                    result =
                            FlinkManagedBlobOrphanFilesClean.executeDatabase(
                                    procedureContext.getExecutionEnvironment(),
                                    catalog,
                                    olderThanMillis(olderThan),
                                    dryRun != null && dryRun,
                                    parallelism,
                                    databaseName,
                                    tableName);
                    break;
                case "LOCAL":
                    result =
                            LocalManagedBlobOrphanFilesClean.executeDatabase(
                                    catalog,
                                    databaseName,
                                    tableName,
                                    olderThanMillis(olderThan),
                                    parallelism,
                                    dryRun != null && dryRun);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown mode: "
                                    + mode
                                    + ". Only 'DISTRIBUTED' and 'LOCAL' are supported.");
            }
            return new String[] {
                String.valueOf(result.getDeletedFileCount()),
                String.valueOf(result.getDeletedFileTotalLenInBytes())
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
