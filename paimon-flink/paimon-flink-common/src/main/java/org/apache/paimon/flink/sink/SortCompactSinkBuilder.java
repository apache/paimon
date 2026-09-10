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

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;

import javax.annotation.Nullable;

import java.util.List;

/** A special version {@link FlinkSinkBuilder} for sort compact. */
public class SortCompactSinkBuilder extends FlinkSinkBuilder {

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
        this.baseSnapshotId = baseSnapshotId;
        this.compactInputSplits = compactInputSplits;
        this.sortCompactInputSet = true;
        return this;
    }

    @Override
    protected RowAppendTableSink createAppendTableSink() {
        if (sortCompactInputSet) {
            return new SortCompactAppendTableSink(
                    table, parallelism, baseSnapshotId, compactInputSplits);
        }
        return super.createAppendTableSink();
    }
}
