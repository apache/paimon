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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FailingFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableCommit}. */
public class TableCommitTest {

    @TempDir java.nio.file.Path tempDir;

    private static final Map<String, Set<Long>> commitCallbackResult = new ConcurrentHashMap<>();

    @Test
    public void testCommitCallbackWithFailureFixedBucket() throws Exception {
        innerTestCommitCallbackWithFailure(1);
    }

    @Test
    public void testCommitCallbackWithFailureDynamicBucket() throws Exception {
        innerTestCommitCallbackWithFailure(-1);
    }

    private void innerTestCommitCallbackWithFailure(int bucket) throws Exception {
        int numIdentifiers = 30;
        String testId = UUID.randomUUID().toString();
        commitCallbackResult.put(testId, new HashSet<>());

        try {
            testCommitCallbackWithFailureImpl(bucket, numIdentifiers, testId);
        } finally {
            commitCallbackResult.remove(testId);
        }
    }

    private void testCommitCallbackWithFailureImpl(int bucket, int numIdentifiers, String testId)
            throws Exception {
        String failingName = UUID.randomUUID().toString();
        // no failure when creating table and writing data
        FailingFileIO.reset(failingName, 0, 1);

        String path = FailingFileIO.getFailingPath(failingName, tempDir.toString());

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options conf = new Options();
        conf.set(CoreOptions.TABLE_SCHEMA_PATH, path);
        conf.set(CoreOptions.BUCKET, bucket);
        conf.set(CoreOptions.COMMIT_CALLBACKS, TestCommitCallback.class.getName());
        conf.set(
                CoreOptions.COMMIT_CALLBACK_PARAM
                        .key()
                        .replace("#", TestCommitCallback.class.getName()),
                testId);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                conf.toMap(),
                                ""));

        FileStoreTable table =
                FileStoreTableFactory.create(
                        new FailingFileIO(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        Map<Long, List<CommitMessage>> commitMessages = new HashMap<>();
        for (int i = 0; i < numIdentifiers; i++) {
            if (bucket == -1) {
                write.write(GenericRow.of(i, i * 1000L), 0);
            } else {
                write.write(GenericRow.of(i, i * 1000L));
            }
            commitMessages.put((long) i, write.prepareCommit(true, i));
        }
        write.close();

        StreamTableCommit commit = table.newCommit(commitUser);
        // enable failure when committing
        FailingFileIO.reset(failingName, 3, 1000);
        while (true) {
            try {
                commit.filterAndCommit(commitMessages);
                break;
            } catch (Throwable t) {
                // artificial exception is intended
                Optional<FailingFileIO.ArtificialException> artificialException =
                        ExceptionUtils.findThrowable(t, FailingFileIO.ArtificialException.class);
                // this test emulates an extremely slow commit procedure,
                // so conflicts may occur due to back pressuring
                Optional<Throwable> conflictException =
                        ExceptionUtils.findThrowableWithMessage(
                                t, "Conflicts during commits are normal");
                if (artificialException.isPresent() || conflictException.isPresent()) {
                    continue;
                }
                throw t;
            }
        }
        commit.close();

        assertThat(commitCallbackResult.get(testId))
                .isEqualTo(LongStream.range(0, numIdentifiers).boxed().collect(Collectors.toSet()));
    }

    /** {@link CommitCallback} for test. */
    public static class TestCommitCallback implements CommitCallback {

        private final String testId;

        public TestCommitCallback(String testId) {
            this.testId = testId;
        }

        @Override
        public void call(List<ManifestEntry> entries, Snapshot snapshot) {
            commitCallbackResult.get(testId).add(snapshot.commitIdentifier());
        }

        @Override
        public void retry(ManifestCommittable committable) {
            commitCallbackResult.get(testId).add(committable.identifier());
        }

        @Override
        public void close() throws Exception {}
    }

    @Test
    public void testRecoverDeletedFiles() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.TABLE_SCHEMA_PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));

        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        write.write(GenericRow.of(0, 0L));
        List<CommitMessage> messages0 = write.prepareCommit(true, 0);

        write.write(GenericRow.of(1, 1L));
        List<CommitMessage> messages1 = write.prepareCommit(true, 1);
        write.close();

        StreamTableCommit commit = table.newCommit(commitUser);
        commit.commit(0, messages0);

        // delete files for commit0 and commit1
        for (CommitMessageImpl message :
                Arrays.asList(
                        (CommitMessageImpl) messages0.get(0),
                        (CommitMessageImpl) messages1.get(0))) {
            DataFilePathFactory pathFactory =
                    createNonPartFactory(new Path(path))
                            .createDataFilePathFactory(message.partition(), message.bucket());
            Path file =
                    message.newFilesIncrement().newFiles().get(0).collectFiles(pathFactory).get(0);
            LocalFileIO.create().delete(file, true);
        }

        // commit 0, fine, it will be filtered
        commit.filterAndCommit(Collections.singletonMap(0L, messages0));

        // commit 1, exception now.
        assertThatThrownBy(() -> commit.filterAndCommit(Collections.singletonMap(1L, messages1)))
                .hasMessageContaining(
                        "Cannot recover from this checkpoint because some files in the"
                                + " snapshot that need to be resubmitted have been deleted");
    }
}
