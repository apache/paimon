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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.store.file.TestFileStore.PAGE_SIZE;
import static org.apache.flink.table.store.file.TestFileStore.WRITE_BUFFER_SIZE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED;

/** Testing {@link Thread}s to perform concurrent commits. */
public class TestCommitThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(TestCommitThread.class);

    private final Map<BinaryRowData, List<KeyValue>> data;
    private final Map<BinaryRowData, List<KeyValue>> result;
    private final Map<BinaryRowData, MergeTreeWriter> writers;

    private final FileStoreWrite<KeyValue> write;
    private final FileStoreCommit commit;

    public TestCommitThread(
            Map<BinaryRowData, List<KeyValue>> data,
            TestFileStore testStore,
            TestFileStore safeStore) {
        this.data = data;
        this.result = new HashMap<>();
        this.writers = new HashMap<>();

        this.write = safeStore.newWrite();
        this.commit = testStore.newCommit();
    }

    public Map<BinaryRowData, List<KeyValue>> getResult() {
        return result;
    }

    @Override
    public void run() {
        while (!data.isEmpty()) {
            try {
                if (ThreadLocalRandom.current().nextInt(5) == 0) {
                    // 20% probability to overwrite
                    doOverwrite();
                } else {
                    doCommit();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        for (MergeTreeWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void doCommit() throws Exception {
        int numWrites = ThreadLocalRandom.current().nextInt(3) + 1;
        for (int i = 0; i < numWrites && !data.isEmpty(); i++) {
            writeData();
        }
        ManifestCommittable committable =
                new ManifestCommittable(String.valueOf(new Random().nextLong()));
        for (Map.Entry<BinaryRowData, MergeTreeWriter> entry : writers.entrySet()) {
            committable.addFileCommittable(entry.getKey(), 0, entry.getValue().prepareCommit());
        }

        runWithRetry(committable, () -> commit.commit(committable, Collections.emptyMap()));
    }

    private void doOverwrite() throws Exception {
        BinaryRowData partition = overwriteData();
        ManifestCommittable committable =
                new ManifestCommittable(String.valueOf(new Random().nextLong()));
        committable.addFileCommittable(partition, 0, writers.get(partition).prepareCommit());

        runWithRetry(
                committable,
                () ->
                        commit.overwrite(
                                TestKeyValueGenerator.toPartitionMap(partition, MULTI_PARTITIONED),
                                committable,
                                Collections.emptyMap()));
    }

    private void runWithRetry(ManifestCommittable committable, Runnable runnable) {
        boolean shouldCheckFilter = false;
        while (true) {
            try {
                if (shouldCheckFilter) {
                    if (commit.filterCommitted(Collections.singletonList(committable)).isEmpty()) {
                        break;
                    }
                }
                runnable.run();
                break;
            } catch (Throwable e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to commit because of exception, try again", e);
                }
                writers.clear();
                shouldCheckFilter = true;
            }
        }
    }

    private void writeData() throws Exception {
        List<KeyValue> changes = new ArrayList<>();
        BinaryRowData partition = pickData(changes);
        result.compute(partition, (p, l) -> l == null ? new ArrayList<>() : l).addAll(changes);

        MergeTreeWriter writer =
                writers.compute(partition, (p, w) -> w == null ? createWriter(p, false) : w);
        for (KeyValue kv : changes) {
            writer.write(kv);
        }
    }

    private BinaryRowData overwriteData() throws Exception {
        List<KeyValue> changes = new ArrayList<>();
        BinaryRowData partition = pickData(changes);
        List<KeyValue> resultOfPartition =
                result.compute(partition, (p, l) -> l == null ? new ArrayList<>() : l);
        resultOfPartition.clear();
        resultOfPartition.addAll(changes);

        if (writers.containsKey(partition)) {
            MergeTreeWriter oldWriter = writers.get(partition);
            oldWriter.close();
        }
        MergeTreeWriter writer = createWriter(partition, true);
        writers.put(partition, writer);
        for (KeyValue kv : changes) {
            writer.write(kv);
        }

        return partition;
    }

    private BinaryRowData pickData(List<KeyValue> changes) {
        List<BinaryRowData> keys = new ArrayList<>(data.keySet());
        BinaryRowData partition = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
        List<KeyValue> remaining = data.get(partition);
        int numChanges = ThreadLocalRandom.current().nextInt(Math.min(100, remaining.size() + 1));
        changes.addAll(remaining.subList(0, numChanges));
        if (numChanges == remaining.size()) {
            data.remove(partition);
        } else {
            remaining.subList(0, numChanges).clear();
        }
        return partition;
    }

    private MergeTreeWriter createWriter(BinaryRowData partition, boolean empty) {
        ExecutorService service =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r);
                            t.setName(Thread.currentThread().getName() + "-writer-service-pool");
                            return t;
                        });
        MergeTreeWriter writer =
                empty
                        ? (MergeTreeWriter) write.createEmptyWriter(partition, 0, service)
                        : (MergeTreeWriter) write.createWriter(partition, 0, service);
        writer.setMemoryPool(
                new HeapMemorySegmentPool(
                        WRITE_BUFFER_SIZE.getBytes(), (int) PAGE_SIZE.getBytes()));
        return writer;
    }
}
