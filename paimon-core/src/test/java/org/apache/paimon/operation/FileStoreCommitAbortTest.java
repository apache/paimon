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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.TestKeyValueGenerator.GeneratorMode;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStoreCommitImpl#abort}. */
public class FileStoreCommitAbortTest {

    @TempDir java.nio.file.Path tempDir;

    private CommitMessageImpl writeDvCommitMessage(TestAppendFileStore store, BinaryRow partition)
            throws Exception {
        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("f1", Arrays.asList(1, 3, 5));
        return store.writeDVIndexFiles(partition, 0, dvs);
    }

    @Test
    public void abortDeletesNewIndexFiles() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        TestKeyValueGenerator generator =
                new TestKeyValueGenerator(GeneratorMode.MULTI_PARTITIONED);
        BinaryRow partition = generator.getPartition(generator.next());
        CommitMessageImpl commitMessage = writeDvCommitMessage(store, partition);

        IndexPathFactory indexPathFactory =
                store.pathFactory().indexFileFactory(commitMessage.partition(), 0);
        FileIO fileIO = store.fileIO();
        assertThat(commitMessage.newFilesIncrement().newIndexFiles()).isNotEmpty();
        for (IndexFileMeta indexFile : commitMessage.newFilesIncrement().newIndexFiles()) {
            assertThat(fileIO.exists(indexPathFactory.toPath(indexFile))).isTrue();
        }

        store.newCommit().abort(Collections.singletonList(commitMessage));

        for (IndexFileMeta indexFile : commitMessage.newFilesIncrement().newIndexFiles()) {
            assertThat(fileIO.exists(indexPathFactory.toPath(indexFile))).isFalse();
        }
    }

    @Test
    public void abortUnderThreadInterruptStillDeletesFiles() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        TestKeyValueGenerator generator =
                new TestKeyValueGenerator(GeneratorMode.MULTI_PARTITIONED);
        BinaryRow partition = generator.getPartition(generator.next());
        CommitMessageImpl commitMessage = writeDvCommitMessage(store, partition);

        IndexPathFactory indexPathFactory =
                store.pathFactory().indexFileFactory(commitMessage.partition(), 0);
        FileIO fileIO = store.fileIO();
        assertThat(commitMessage.newFilesIncrement().newIndexFiles()).isNotEmpty();

        Thread.currentThread().interrupt();
        try {
            store.newCommit().abort(Collections.singletonList(commitMessage));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }

        for (IndexFileMeta indexFile : commitMessage.newFilesIncrement().newIndexFiles()) {
            assertThat(fileIO.exists(indexPathFactory.toPath(indexFile))).isFalse();
        }
    }

    @Test
    public void abortSkipsNullMessages() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        TestKeyValueGenerator generator =
                new TestKeyValueGenerator(GeneratorMode.MULTI_PARTITIONED);
        BinaryRow partition = generator.getPartition(generator.next());
        CommitMessageImpl commitMessage = writeDvCommitMessage(store, partition);

        IndexPathFactory indexPathFactory =
                store.pathFactory().indexFileFactory(commitMessage.partition(), 0);
        FileIO fileIO = store.fileIO();
        assertThat(commitMessage.newFilesIncrement().newIndexFiles()).isNotEmpty();

        store.newCommit().abort(Arrays.asList(null, commitMessage));

        for (IndexFileMeta indexFile : commitMessage.newFilesIncrement().newIndexFiles()) {
            assertThat(fileIO.exists(indexPathFactory.toPath(indexFile))).isFalse();
        }
    }

    @Test
    public void abortDoesNotDeleteDeletedOrCompactBeforeFiles() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        TestKeyValueGenerator generator =
                new TestKeyValueGenerator(GeneratorMode.MULTI_PARTITIONED);
        BinaryRow partition = generator.getPartition(generator.next());
        FileIO fileIO = store.fileIO();
        DataFilePathFactory dataPathFactory =
                store.pathFactory().createDataFilePathFactory(partition, 0);
        IndexPathFactory indexPathFactory = store.pathFactory().indexFileFactory(partition, 0);

        CommitMessageImpl keepData =
                store.writeDataFiles(
                        partition, 0, Arrays.asList("keep-deleted.parquet", "keep-before.parquet"));
        DataFileMeta deletedFile = keepData.newFilesIncrement().newFiles().get(0);
        DataFileMeta compactBeforeFile = keepData.newFilesIncrement().newFiles().get(1);

        CommitMessageImpl abortData =
                store.writeDataFiles(
                        partition, 0, Arrays.asList("abort-new.parquet", "abort-after.parquet"));
        DataFileMeta newFile = abortData.newFilesIncrement().newFiles().get(0);
        DataFileMeta compactAfterFile = abortData.newFilesIncrement().newFiles().get(1);

        CommitMessageImpl keepIndexMsg = writeDvCommitMessage(store, partition);
        CommitMessageImpl abortIndexMsg = writeDvCommitMessage(store, partition);
        IndexFileMeta deletedIndexFile = keepIndexMsg.newFilesIncrement().newIndexFiles().get(0);
        IndexFileMeta newIndexFile = abortIndexMsg.newFilesIncrement().newIndexFiles().get(0);

        assertThat(fileIO.exists(dataPathFactory.toPath(deletedFile))).isTrue();
        assertThat(fileIO.exists(dataPathFactory.toPath(compactBeforeFile))).isTrue();
        assertThat(fileIO.exists(dataPathFactory.toPath(newFile))).isTrue();
        assertThat(fileIO.exists(dataPathFactory.toPath(compactAfterFile))).isTrue();
        assertThat(fileIO.exists(indexPathFactory.toPath(deletedIndexFile))).isTrue();
        assertThat(fileIO.exists(indexPathFactory.toPath(newIndexFile))).isTrue();

        CommitMessageImpl abortMessage =
                new CommitMessageImpl(
                        partition,
                        0,
                        store.options().bucket(),
                        new DataIncrement(
                                Collections.singletonList(newFile),
                                Collections.singletonList(deletedFile),
                                Collections.emptyList(),
                                Collections.singletonList(newIndexFile),
                                Collections.singletonList(deletedIndexFile)),
                        new CompactIncrement(
                                Collections.singletonList(compactBeforeFile),
                                Collections.singletonList(compactAfterFile),
                                Collections.emptyList()));

        store.newCommit().abort(Collections.singletonList(abortMessage));

        assertThat(fileIO.exists(dataPathFactory.toPath(deletedFile))).isTrue();
        assertThat(fileIO.exists(dataPathFactory.toPath(compactBeforeFile))).isTrue();
        assertThat(fileIO.exists(indexPathFactory.toPath(deletedIndexFile))).isTrue();
        assertThat(fileIO.exists(dataPathFactory.toPath(newFile))).isFalse();
        assertThat(fileIO.exists(dataPathFactory.toPath(compactAfterFile))).isFalse();
        assertThat(fileIO.exists(indexPathFactory.toPath(newIndexFile))).isFalse();
    }
}
