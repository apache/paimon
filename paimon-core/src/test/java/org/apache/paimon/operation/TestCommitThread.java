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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.TestFileStore.PAGE_SIZE;
import static org.apache.paimon.TestFileStore.WRITE_BUFFER_SIZE;
import static org.apache.paimon.TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED;
import static org.apache.paimon.utils.FailingFileIO.retryArtificialException;

/** Testing {@link Thread}s to perform concurrent commits. */
public class TestCommitThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(TestCommitThread.class);

    private final RowType keyType;
    private final RowType valueType;
    private final boolean enableOverwrite;
    private final Map<BinaryRow, List<KeyValue>> data;
    private final Map<BinaryRow, List<KeyValue>> result;
    private final Map<BinaryRow, MergeTreeWriter> writers;
    private final Set<BinaryRow> writtenPartitions;

    private final AbstractFileStoreWrite<KeyValue> write;
    private final FileStoreCommit commit;

    private long commitIdentifier;

    public TestCommitThread(
            RowType keyType,
            RowType valueType,
            boolean enableOverwrite,
            Map<BinaryRow, List<KeyValue>> data,
            TestFileStore testStore,
            TestFileStore safeStore)
            throws Exception {
        this.keyType = keyType;
        this.valueType = valueType;
        this.enableOverwrite = enableOverwrite;
        this.data = data;
        this.result = new HashMap<>();
        this.writers = new HashMap<>();
        this.writtenPartitions = new HashSet<>();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Test commit thread is given these data:\n"
                            + data.values().stream()
                                    .flatMap(Collection::stream)
                                    .map(kv -> kv.toString(keyType, valueType))
                                    .collect(Collectors.joining("\n")));
        }

        String commitUser = UUID.randomUUID().toString();
        this.write = safeStore.newWrite(commitUser);
        this.commit =
                retryArtificialException(
                        () -> testStore.newCommit(commitUser).ignoreEmptyCommit(false));

        this.commitIdentifier = 0;
    }

    public List<KeyValue> getResult() {
        return result.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public void run() {
        while (!data.isEmpty()) {
            try {
                if (enableOverwrite && ThreadLocalRandom.current().nextInt(5) == 0) {
                    // 20% probability to overwrite
                    doOverwrite();
                } else {
                    doCommit();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        doFinalCompact();

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
        ManifestCommittable committable = new ManifestCommittable(commitIdentifier++);
        for (Map.Entry<BinaryRow, MergeTreeWriter> entry : writers.entrySet()) {
            CommitIncrement inc = entry.getValue().prepareCommit(true);
            committable.addFileCommittable(
                    new CommitMessageImpl(
                            entry.getKey(), 0, inc.newFilesIncrement(), inc.compactIncrement()));
        }

        runWithRetry(committable, () -> commit.commit(committable, Collections.emptyMap()));
    }

    private void doOverwrite() throws Exception {
        BinaryRow partition = overwriteData();
        ManifestCommittable committable = new ManifestCommittable(commitIdentifier++);
        CommitIncrement inc = writers.get(partition).prepareCommit(true);
        committable.addFileCommittable(
                new CommitMessageImpl(
                        partition, 0, inc.newFilesIncrement(), inc.compactIncrement()));

        runWithRetry(
                committable,
                () ->
                        commit.overwrite(
                                TestKeyValueGenerator.toPartitionMap(partition, MULTI_PARTITIONED),
                                committable,
                                Collections.emptyMap()));
    }

    private void doFinalCompact() {
        long identifier = commitIdentifier++;
        while (true) {
            try {
                ManifestCommittable committable = new ManifestCommittable(identifier);
                for (BinaryRow partition : writtenPartitions) {
                    MergeTreeWriter writer =
                            writers.computeIfAbsent(partition, p -> createWriter(p, false));
                    writer.compact(true);
                    CommitIncrement inc = writer.prepareCommit(true);
                    committable.addFileCommittable(
                            new CommitMessageImpl(
                                    partition, 0, inc.newFilesIncrement(), inc.compactIncrement()));
                }
                commit.commit(committable, Collections.emptyMap());
                break;
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to do final compact because of exception, try again", e);
                }
                writers.clear();
            }
        }
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
        BinaryRow partition = pickData(changes);
        result.computeIfAbsent(partition, p -> new ArrayList<>()).addAll(changes);
        writtenPartitions.add(partition);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Test commit thread will write data\n"
                            + changes.stream()
                                    .map(kv -> kv.toString(keyType, valueType))
                                    .collect(Collectors.joining("\n")));
        }

        MergeTreeWriter writer = writers.computeIfAbsent(partition, p -> createWriter(p, false));
        for (KeyValue kv : changes) {
            writer.write(kv);
        }
    }

    private BinaryRow overwriteData() throws Exception {
        List<KeyValue> changes = new ArrayList<>();
        BinaryRow partition = pickData(changes);
        List<KeyValue> resultOfPartition =
                result.computeIfAbsent(partition, p -> new ArrayList<>());
        resultOfPartition.clear();
        resultOfPartition.addAll(changes);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Test commit thread will overwrite data\n"
                            + changes.stream()
                                    .map(kv -> kv.toString(keyType, valueType))
                                    .collect(Collectors.joining("\n")));
        }

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

    private BinaryRow pickData(List<KeyValue> changes) {
        List<BinaryRow> keys = new ArrayList<>(data.keySet());
        BinaryRow partition = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
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

    private MergeTreeWriter createWriter(BinaryRow partition, boolean empty) {
        MergeTreeWriter writer =
                (MergeTreeWriter) write.createWriterContainer(partition, 0, empty).writer;
        writer.setMemoryPool(
                new HeapMemorySegmentPool(
                        WRITE_BUFFER_SIZE.getBytes(), (int) PAGE_SIZE.getBytes()));
        return writer;
    }
}
