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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_ENABLED;

/** {@link FlinkSink} for writing records into fixed bucket Paimon table. */
public class PostponeBucketSink extends FlinkWriteSink<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final boolean writeRealBucket;

    public PostponeBucketSink(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartition,
            boolean writeRealBucket) {
        super(table, overwritePartition);
        this.writeRealBucket = writeRealBucket;
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        if (writeRealBucket) {
            Options options = table.coreOptions().toConfiguration();
            boolean coordinatorEnabled = options.get(SINK_WRITER_COORDINATOR_ENABLED);
            return coordinatorEnabled
                    ? new RowDataStoreWriteOperator.CoordinatedFactory(
                            table, null, writeProvider, commitUser)
                    : new RowDataStoreWriteOperator.Factory(table, null, writeProvider, commitUser);
        } else {
            return createNoStateRowWriteOperatorFactory(table, null, writeProvider, commitUser);
        }
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        if (writeRealBucket) {
            return super.createCommittableStateManager();
        } else {
            return createRestoreOnlyCommittableStateManager(table);
        }
    }
}
