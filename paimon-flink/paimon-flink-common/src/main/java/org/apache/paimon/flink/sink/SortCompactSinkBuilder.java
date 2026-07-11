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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.SortCompactSequenceUtils;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** A special version {@link FlinkSinkBuilder} for sort compact. */
public class SortCompactSinkBuilder extends FlinkSinkBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SortCompactSinkBuilder.class);

    private long baseSnapshotId;
    @Nullable private List<DataSplit> compactInputSplits;
    private boolean sortCompactInputSet = false;

    public SortCompactSinkBuilder(Table table) {
        super(table);
    }

    public SortCompactSinkBuilder forCompact(boolean compactSink) {
        this.compactSink = compactSink;
        return this;
    }

    /**
     * Capture the base snapshot id and the planned compact input splits of the sort compact. The
     * splits (serializable) are carried into the job graph and used at commit time to rewrite the
     * written append files into a compact commit.
     *
     * <p><b>Scale note:</b> each {@link DataSplit} embeds full file metadata and is serialized into
     * the committer factory closure. For tables with very large numbers of input files, this can
     * significantly inflate the Flink job graph. Consider compacting in smaller partition batches
     * when approaching hundreds of thousands of files.
     */
    public SortCompactSinkBuilder withSortCompactInput(
            long baseSnapshotId, List<DataSplit> compactInputSplits) {
        validateSortCompactInput(compactInputSplits);
        this.baseSnapshotId = baseSnapshotId;
        this.compactInputSplits = compactInputSplits;
        this.sortCompactInputSet = true;
        return this;
    }

    private void validateSortCompactInput(List<DataSplit> compactInputSplits) {
        long inputFileCount =
                compactInputSplits.stream().mapToLong(split -> split.dataFiles().size()).sum();
        int warnThreshold = table.coreOptions().sortCompactionWarnInputFiles();
        int maxThreshold = table.coreOptions().sortCompactionMaxInputFiles();
        if (inputFileCount > warnThreshold) {
            LOG.warn(
                    "Sort compact plan contains {} input files across {} splits, which exceeds the "
                            + "warn threshold {}. Each input file is serialized into the Flink job "
                            + "graph and may significantly inflate job submission time and RPC "
                            + "payload. Consider compacting in smaller partition batches or raising "
                            + "'{}' if this is expected.",
                    inputFileCount,
                    compactInputSplits.size(),
                    warnThreshold,
                    CoreOptions.SORT_COMPACTION_WARN_INPUT_FILES.key());
        }
        if (inputFileCount > maxThreshold) {
            throw new IllegalArgumentException(
                    String.format(
                            "Sort compact plan contains %d input files across %d splits, which "
                                    + "exceeds the limit %d. Compact in smaller partition batches "
                                    + "or raise '%s'.",
                            inputFileCount,
                            compactInputSplits.size(),
                            maxThreshold,
                            CoreOptions.SORT_COMPACTION_MAX_INPUT_FILES.key()));
        }
    }

    @Override
    protected RowType sinkInputRowType() {
        if (table.coreOptions().snapshotSequenceOrdering() && !table.primaryKeys().isEmpty()) {
            return SortCompactSequenceUtils.rowTypeWithKeyValueSequenceNumber(table.rowType());
        }
        return table.rowType();
    }

    @Override
    protected RowAppendTableSink createAppendTableSink() {
        if (sortCompactInputSet) {
            return new SortCompactAppendTableSink(
                    table, parallelism, baseSnapshotId, compactInputSplits);
        }
        return super.createAppendTableSink();
    }

    @Override
    protected DataStreamSink<?> buildDynamicBucketSink(
            DataStream<InternalRow> input, boolean globalIndex) {
        if (sortCompactInputSet && compactSink && !globalIndex) {
            return new SortCompactDynamicBucketSink(table, baseSnapshotId, compactInputSplits)
                    .build(input, parallelism);
        }
        return super.buildDynamicBucketSink(input, globalIndex);
    }
}
