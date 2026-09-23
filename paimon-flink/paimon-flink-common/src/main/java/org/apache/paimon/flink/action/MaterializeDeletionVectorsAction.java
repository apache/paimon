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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.compact.DataEvolutionDeletionVectorMaterialize;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink action which applies deletion vectors to the latest data evolution table state. */
public class MaterializeDeletionVectorsAction extends TableActionBase {

    private List<Map<String, String>> partitions;
    private String whereSql;

    public MaterializeDeletionVectorsAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig);
        this.forceStartFlinkJob = true;
        checkArgument(
                table instanceof FileStoreTable,
                "Materializing deletion vectors only supports FileStoreTable.");
        HashMap<String, String> dynamicOptions = new HashMap<>(tableConf);
        dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        table = table.copy(dynamicOptions);
    }

    public MaterializeDeletionVectorsAction withPartitions(List<Map<String, String>> partitions) {
        this.partitions = partitions;
        return this;
    }

    public MaterializeDeletionVectorsAction withWhereSql(String whereSql) {
        this.whereSql = whereSql;
        return this;
    }

    @Override
    public void build() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        checkArgument(
                fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE
                        && fileStoreTable.coreOptions().dataEvolutionEnabled(),
                "Materializing deletion vectors only supports unaware-bucket data evolution tables.");
        checkArgument(
                fileStoreTable.coreOptions().deletionVectorsEnabled(),
                "Materializing deletion vectors requires deletion vectors to be enabled.");
        checkArgument(
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        != RuntimeExecutionMode.STREAMING,
                "Materializing deletion vectors only supports batch mode yet.");
        DataEvolutionDeletionVectorMaterialize builder =
                new DataEvolutionDeletionVectorMaterialize(
                        env, identifier.getFullName(), fileStoreTable);
        builder.withPartitionPredicate(
                ActionPartitionPredicate.create(
                        fileStoreTable, partitions, whereSql, "deletion vector materialization"));
        builder.build();
    }
}
