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

import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for {@link FileStore} of {@link KeyValue}, such as writing and committing. */
public class FileStoreTestUtils {

    // generate partitioned data
    public static List<KeyValue> partitionedData(
            int num, TestKeyValueGenerator gen, Object... partitionSpec) {
        List<KeyValue> keyValues = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            keyValues.add(gen.nextPartitionedData(RowKind.INSERT, partitionSpec));
        }
        return keyValues;
    }

    public static void assertPathExists(FileIO fileIO, Path path) throws IOException {
        assertThat(fileIO.exists(path)).isTrue();
    }

    public static void assertPathNotExists(FileIO fileIO, Path path) throws IOException {
        assertThat(fileIO.exists(path)).isFalse();
    }

    // --------------------------------------------------------------------------------
    // writeData & commitData are copied from TestFileStore#commitDataImpl and modified
    // --------------------------------------------------------------------------------

    // create a RecordWriter and write data
    public static RecordWriter<KeyValue> writeData(
            TestFileStore store, List<KeyValue> keyValues, BinaryRow partition, int bucket)
            throws Exception {
        AbstractFileStoreWrite<KeyValue> write = store.newWrite();
        RecordWriter<KeyValue> writer =
                write.createWriterContainer(partition, bucket, false).writer;
        ((MemoryOwner) writer)
                .setMemoryPool(
                        new HeapMemorySegmentPool(
                                TestFileStore.WRITE_BUFFER_SIZE.getBytes(),
                                (int) TestFileStore.PAGE_SIZE.getBytes()));
        for (KeyValue kv : keyValues) {
            writer.write(kv);
        }
        return writer;
    }

    // commit data in writers
    public static void commitData(
            TestFileStore store,
            long commitIdentifier,
            Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers)
            throws Exception {
        ManifestCommittable committable = new ManifestCommittable(commitIdentifier, null);
        for (Map.Entry<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter<KeyValue>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                CommitIncrement increment = entryWithBucket.getValue().prepareCommit(false);
                committable.addFileCommittable(
                        new CommitMessageImpl(
                                entryWithPartition.getKey(),
                                entryWithBucket.getKey(),
                                increment.newFilesIncrement(),
                                increment.compactIncrement()));
            }
        }

        try (FileStoreCommit commit = store.newCommit()) {
            commit.commit(committable, Collections.emptyMap());
        }

        writers.values().stream()
                .flatMap(m -> m.values().stream())
                .forEach(
                        w -> {
                            try {
                                // wait for compaction to end, otherwise orphan files may occur
                                // see CompactManager#cancelCompaction for more info
                                w.sync();
                                w.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
