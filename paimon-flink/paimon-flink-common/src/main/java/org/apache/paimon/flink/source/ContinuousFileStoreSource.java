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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.STREAMING_READ_ATOMIC;

/** Unbounded {@link FlinkSource} for reading records. It continuously monitors new snapshots. */
public class ContinuousFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 3L;

    private final Map<String, String> options;

    public ContinuousFileStoreSource(
            ReadBuilder readBuilder, Map<String, String> options, @Nullable Long limit) {
        super(readBuilder, limit);
        this.options = options;
    }

    @Override
    public Boundedness getBoundedness() {
        return isBounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Long nextSnapshotId = null;
        Collection<FileStoreSourceSplit> splits = new ArrayList<>();
        if (checkpoint != null) {
            nextSnapshotId = checkpoint.currentSnapshotId();
            splits = checkpoint.splits();
        }
        CoreOptions coreOptions = CoreOptions.fromMap(options);
        StreamTableScan scan = readBuilder.newStreamScan();
        scan.restore(nextSnapshotId);
        return new ContinuousFileSplitEnumerator(
                context,
                splits,
                nextSnapshotId,
                coreOptions.continuousDiscoveryInterval().toMillis(),
                coreOptions
                        .toConfiguration()
                        .get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                scan);
    }

    @Override
    public FileStoreSourceReader<?> createSourceReader(
            SourceReaderContext context, TableRead read, @Nullable Long limit) {
        return Options.fromMap(options).get(STREAMING_READ_ATOMIC)
                ? new FileStoreSourceReader<>(RecordsFunction.forSingle(), context, read, limit)
                : new FileStoreSourceReader<>(RecordsFunction.forIterate(), context, read, limit);
    }

    private boolean isBounded() {
        return CoreOptions.fromMap(options).scanBoundedWatermark() != null;
    }
}
