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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/** Testing {@link Thread}s to perform concurrent commits. */
public class TestCommitThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(TestCommitThread.class);

    private final Map<BinaryRowData, List<KeyValue>> data;
    private final Map<BinaryRowData, MergeTreeWriter> writers;

    private final FileStoreWrite write;
    private final FileStoreCommit commit;

    public TestCommitThread(
            Map<BinaryRowData, List<KeyValue>> data,
            FileStorePathFactory testPathFactory,
            FileStorePathFactory safePathFactory) {
        this.data = data;
        this.writers = new HashMap<>();

        FileFormat avro =
                FileFormat.fromIdentifier(
                        FileStoreCommitTestBase.class.getClassLoader(),
                        "avro",
                        new Configuration());
        this.write = OperationTestUtils.createWrite(avro, safePathFactory);
        this.commit = OperationTestUtils.createCommit(avro, testPathFactory);
    }

    @Override
    public void run() {
        while (!data.isEmpty()) {
            ManifestCommittable committable;
            try {
                committable = createCommittable();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            boolean shouldCheckFilter = false;
            while (true) {
                try {
                    if (shouldCheckFilter) {
                        if (commit.filterCommitted(Collections.singletonList(committable))
                                .isEmpty()) {
                            break;
                        }
                    }
                    commit.commit(committable, Collections.emptyMap());
                    break;
                } catch (Throwable e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.warn("Failed to commit because of exception, try again", e);
                    }
                    writers.clear();
                    shouldCheckFilter = true;
                }
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

    private ManifestCommittable createCommittable() throws Exception {
        int numWrites = ThreadLocalRandom.current().nextInt(3) + 1;
        for (int i = 0; i < numWrites && !data.isEmpty(); i++) {
            writeData();
        }

        ManifestCommittable committable = new ManifestCommittable();
        for (Map.Entry<BinaryRowData, MergeTreeWriter> entry : writers.entrySet()) {
            committable.add(entry.getKey(), 0, entry.getValue().prepareCommit());
        }
        return committable;
    }

    private void writeData() throws Exception {
        List<KeyValue> changes = new ArrayList<>();
        BinaryRowData partition = pickData(changes);
        MergeTreeWriter writer =
                writers.compute(partition, (k, v) -> v == null ? createWriter(k) : v);
        for (KeyValue kv : changes) {
            writer.write(kv.valueKind(), kv.key(), kv.value());
        }
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

    private MergeTreeWriter createWriter(BinaryRowData partition) {
        ExecutorService service =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r);
                            t.setName(Thread.currentThread().getName() + "-writer-service-pool");
                            return t;
                        });
        return (MergeTreeWriter) write.createWriter(partition, 0, service);
    }
}
