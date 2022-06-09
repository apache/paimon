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

package org.apache.flink.table.store.file.writer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactUnit;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A {@link RecordWriter} implementation that only perform compaction on existing records and does
 * not generate new records.
 */
public class CompactWriter implements RecordWriter {

    private final CompactUnit unit;
    private final ExecutorService compactExecutor;
    private final CompactManager compactor;
    private final DataFileWriter dataFileWriter;

    public CompactWriter(
            CompactUnit unit,
            Comparator<RowData> keyComparator,
            long targetFileSize,
            CompactManager.Rewriter rewriter,
            DataFileWriter dataFileWriter) {
        this.unit = unit;
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
        this.compactor =
                new CompactManager(
                        compactExecutor,
                        (numLevels, runs) ->
                                Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs)),
                        keyComparator,
                        targetFileSize,
                        rewriter);
        this.dataFileWriter = dataFileWriter;
    }

    @Override
    public Increment prepareCommit() throws IOException, InterruptedException {
        List<DataFileMeta> compactBefore = new ArrayList<>();
        List<DataFileMeta> compactAfter = new ArrayList<>();
        if (compactor.isCompactionFinished()) {
            compactor.submitCompaction(unit, true);
        }
        while (!compactor.isCompactionFinished()) {
            try {
                compactor
                        .finishCompaction(true)
                        .ifPresent(
                                result -> {
                                    compactBefore.addAll(result.before());
                                    compactAfter.addAll(result.after());
                                });
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        }
        return Increment.forCompact(compactBefore, compactAfter);
    }

    @Override
    public List<DataFileMeta> close() throws Exception {
        compactExecutor.shutdownNow();
        List<DataFileMeta> delete = new ArrayList<>();
        try {
            compactor.finishCompaction(true).ifPresent(result -> delete.addAll(result.after()));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
        // delete generated compact-after files which have no chance to be committed
        for (DataFileMeta file : delete) {
            dataFileWriter.delete(file);
        }
        return delete;
    }

    @Override
    public void write(ValueKind valueKind, RowData key, RowData value) throws Exception {
        // nothing to write
    }

    @Override
    public void sync() throws Exception {}
}
