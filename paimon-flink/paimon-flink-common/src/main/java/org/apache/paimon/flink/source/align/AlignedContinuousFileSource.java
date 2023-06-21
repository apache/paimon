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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Map;

/**
 * Unbounded {@link Source} for reading records. It continuously monitors new snapshots.
 *
 * <p>The difference with {@link org.apache.paimon.flink.source.ContinuousFileStoreSource} is that
 * it will coordinate flink's checkpoint and paimon snapshot.
 */
public class AlignedContinuousFileSource implements Source<Split, AlignedSourceSplit, Void> {

    private static final long serialVersionUID = 1L;

    protected final ReadBuilder readBuilder;

    private final boolean emitSnapshotWatermark;

    private final Map<String, String> options;

    public AlignedContinuousFileSource(
            ReadBuilder readBuilder, boolean emitSnapshotWatermark, Map<String, String> options) {
        this.readBuilder = readBuilder;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
        this.options = options;
    }

    @Override
    public Boundedness getBoundedness() {
        Long boundedWatermark = CoreOptions.fromMap(options).scanBoundedWatermark();
        return boundedWatermark != null ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<AlignedSourceSplit, Void> createEnumerator(
            SplitEnumeratorContext<AlignedSourceSplit> enumContext) throws Exception {
        return new AlignedEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<AlignedSourceSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<AlignedSourceSplit> enumContext, Void checkpoint)
            throws Exception {
        return new AlignedEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<AlignedSourceSplit> getSplitSerializer() {
        return new AlignedSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }

    @Override
    public SourceReader<Split, AlignedSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        Options conf = Options.fromMap(options);
        return new AlignedSourceReader(
                readBuilder,
                conf.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                emitSnapshotWatermark,
                conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGNED_MODE));
    }
}
