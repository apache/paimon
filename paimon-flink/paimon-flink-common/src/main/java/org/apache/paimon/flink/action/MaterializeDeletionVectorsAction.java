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

import org.apache.paimon.flink.compact.DataEvolutionTableCompact;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink action which physically applies deletion vectors for a data evolution table. */
public class MaterializeDeletionVectorsAction extends CompactAction {

    public MaterializeDeletionVectorsAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig, tableConf);
    }

    @Override
    public MaterializeDeletionVectorsAction withPartitions(List<Map<String, String>> partitions) {
        super.withPartitions(partitions);
        return this;
    }

    @Override
    public MaterializeDeletionVectorsAction withWhereSql(String whereSql) {
        super.withWhereSql(whereSql);
        return this;
    }

    @Override
    protected boolean buildImpl() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        checkArgument(
                fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE
                        && fileStoreTable.coreOptions().dataEvolutionEnabled(),
                "Materializing deletion vectors only supports unaware-bucket data evolution tables.");
        checkArgument(
                fileStoreTable.coreOptions().deletionVectorsEnabled(),
                "Materializing deletion vectors requires deletion vectors to be enabled.");
        return super.buildImpl();
    }

    @Override
    protected void buildForDataEvolutionTableCompact(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        checkArgument(!isStreaming, "Materializing deletion vectors only supports batch mode yet.");
        DataEvolutionTableCompact builder =
                new DataEvolutionTableCompact(env, identifier.getFullName(), table, true);
        builder.withPartitionPredicate(getPartitionPredicate());
        builder.build();
    }
}
