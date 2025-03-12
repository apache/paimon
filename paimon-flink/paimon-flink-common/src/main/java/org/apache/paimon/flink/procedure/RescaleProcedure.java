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
import org.apache.paimon.flink.action.RescaleAction;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

/** Procedure to rescale one partition of a table. */
public class RescaleProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "rescale";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "bucket_num", type = @DataTypeHint("INT"), isOptional = true),
                @ArgumentHint(
                        name = "partition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "scan_parallelism",
                        type = @DataTypeHint("INT"),
                        isOptional = true),
                @ArgumentHint(
                        name = "sink_parallelism",
                        type = @DataTypeHint("INT"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            @Nullable Integer bucketNum,
            @Nullable String partition,
            @Nullable Integer scanParallelism,
            @Nullable Integer sinkParallelism)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();

        RescaleAction action = new RescaleAction(databaseName, tableName, catalog.options());
        if (bucketNum != null) {
            action.withBucketNum(bucketNum);
        }
        if (partition != null) {
            action.withPartition(ParameterUtils.getPartitions(partition).get(0));
        }
        if (scanParallelism != null) {
            action.withScanParallelism(scanParallelism);
        }
        if (sinkParallelism != null) {
            action.withSinkParallelism(sinkParallelism);
        }

        return execute(
                procedureContext, action, "Rescale Postpone Bucket : " + identifier.getFullName());
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
