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

import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * {@link StoreSinkWrite} for sort compact on bucket-unaware append tables.
 *
 * <p>Always uses {@code waitCompaction=false} so sorted output lands in {@code newFilesIncrement}.
 * The committer rewrites those files into {@code compactAfter}.
 */
public class SortCompactAppendSinkWrite extends StoreSinkWriteImpl {

    public SortCompactAppendSinkWrite(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean isStreamingMode,
            MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        super(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                false,
                isStreamingMode,
                memoryPoolFactory,
                metricGroup);
    }

    @Override
    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        // Batch endInput always passes waitCompaction=true, but sort compact write must not wait
        // for inline compaction. The committer rewrites append-style newFiles into compactAfter.
        return super.prepareCommit(false, checkpointId);
    }

    public static StoreSinkWrite.Provider provider() {
        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                new SortCompactAppendSinkWrite(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        true,
                        false,
                        memoryPoolFactory,
                        metricGroup);
    }
}
