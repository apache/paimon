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
import org.apache.paimon.flink.action.CompactPostponeBucketAction;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Procedure to compact postpone bucket tables, which distributes records in {@code bucket = -2}
 * directory into real bucket directories.
 */
public class CompactPostponeBucketProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "compact_postpone_bucket";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "default_bucket_num",
                        type = @DataTypeHint("INT"),
                        isOptional = true),
                @ArgumentHint(name = "parallelism", type = @DataTypeHint("INT"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            @Nullable Integer defaultBucketNum,
            @Nullable Integer parallelism)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();

        CompactPostponeBucketAction action =
                new CompactPostponeBucketAction(databaseName, tableName, catalog.options());
        if (defaultBucketNum != null) {
            action.withDefaultBucketNum(defaultBucketNum);
        }
        if (parallelism != null) {
            action.withParallelism(parallelism);
        }
        action.withStreamExecutionEnvironment(procedureContext.getExecutionEnvironment());

        List<String> result = action.buildImpl();
        if (!result.isEmpty()) {
            procedureContext
                    .getExecutionEnvironment()
                    .execute("Postpone Bucket Compaction : " + identifier.getObjectName());
        }
        return result.toArray(new String[0]);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
