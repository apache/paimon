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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.FileStoreExpire;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.utils.RecordWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.row;

/** Test {@link FileStore}. */
public class TestFileStore implements FileStore {

    public final Set<ManifestCommittable> committed = new HashSet<>();

    public final Map<BinaryRowData, Map<Integer, List<String>>> committedFiles = new HashMap<>();

    public boolean expired = false;

    @Override
    public FileStoreWrite newWrite() {
        return new FileStoreWrite() {
            @Override
            public RecordWriter createWriter(
                    BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
                TestRecordWriter writer = new TestRecordWriter();
                writer.records.addAll(
                        committedFiles
                                .computeIfAbsent(partition, k -> new HashMap<>())
                                .computeIfAbsent(bucket, k -> new ArrayList<>()));
                committedFiles.get(partition).remove(bucket);
                return writer;
            }

            @Override
            public RecordWriter createEmptyWriter(
                    BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
                return new TestRecordWriter();
            }
        };
    }

    @Override
    public FileStoreRead newRead() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStoreCommit newCommit() {
        return new TestCommit();
    }

    @Override
    public FileStoreExpire newExpire() {
        return () -> expired = true;
    }

    @Override
    public FileStoreScan newScan() {
        throw new UnsupportedOperationException();
    }

    static class TestRecordWriter implements RecordWriter {

        final List<String> records = new ArrayList<>();

        boolean synced = false;

        boolean closed = false;

        private String rowToString(RowData row) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < row.getArity(); i++) {
                if (i != 0) {
                    builder.append("/");
                }
                builder.append(row.getInt(i));
            }
            return builder.toString();
        }

        @Override
        public void write(ValueKind valueKind, RowData key, RowData value) {
            records.add(
                    valueKind.toString()
                            + "-key-"
                            + rowToString(key)
                            + "-value-"
                            + rowToString(value));
        }

        @Override
        public Increment prepareCommit() {
            List<SstFileMeta> newFiles =
                    records.stream()
                            .map(
                                    s ->
                                            new SstFileMeta(
                                                    s,
                                                    0,
                                                    0,
                                                    null,
                                                    null,
                                                    new FieldStats[] {
                                                        new FieldStats(null, null, 0),
                                                        new FieldStats(null, null, 0),
                                                        new FieldStats(null, null, 0)
                                                    },
                                                    0,
                                                    0,
                                                    0))
                            .collect(Collectors.toList());
            return new Increment(newFiles, Collections.emptyList(), Collections.emptyList());
        }

        @Override
        public void sync() {
            synced = true;
        }

        @Override
        public List<SstFileMeta> close() {
            closed = true;
            return Collections.emptyList();
        }
    }

    class TestCommit implements FileStoreCommit {

        Lock lock;

        @Override
        public FileStoreCommit withLock(Lock lock) {
            this.lock = lock;
            return this;
        }

        @Override
        public List<ManifestCommittable> filterCommitted(
                List<ManifestCommittable> committableList) {
            return committableList.stream()
                    .filter(c -> !committed.contains(c))
                    .collect(Collectors.toList());
        }

        @Override
        public void commit(ManifestCommittable committable, Map<String, String> properties) {
            try {
                lock.runWithLock(() -> committed.add(committable));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            committable
                    .newFiles()
                    .forEach(
                            (part, bMap) ->
                                    bMap.forEach(
                                            (bucket, files) -> {
                                                List<String> committed =
                                                        committedFiles
                                                                .computeIfAbsent(
                                                                        part, k -> new HashMap<>())
                                                                .computeIfAbsent(
                                                                        bucket,
                                                                        k -> new ArrayList<>());
                                                files.stream()
                                                        .map(SstFileMeta::fileName)
                                                        .forEach(committed::add);
                                            }));
        }

        @Override
        public void overwrite(
                Map<String, String> partition,
                ManifestCommittable committable,
                Map<String, String> properties) {
            if (partition.isEmpty()) {
                committedFiles.clear();
            } else {
                BinaryRowData partRow = row(Integer.parseInt(partition.get("part")));
                committedFiles.remove(partRow);
            }
            commit(committable, properties);
        }
    }
}
