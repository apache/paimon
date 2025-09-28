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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendCompactTask}. */
public class AppendCompactTaskTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAppendCompactionWithDeletionVectors(boolean compactBeforeAllFiles)
            throws Exception {
        TestAppendFileStore store =
                createAppendStore(
                        tempDir,
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));

        // Create deletion vectors for two files
        // Each file has some deleted rows
        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        dvs.put("data-1.orc", Arrays.asList(2, 4, 6));

        // Write deletion vectors for all files to simulate existing deletion vectors
        CommitMessageImpl commitMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, dvs, true);
        store.commit(commitMessage);

        List<DataFileMeta> allFiles =
                Arrays.asList(
                        newFile("data-0.orc", 0, 0, 100, 100),
                        newFile("data-1.orc", 0, 101, 200, 200));

        List<DataFileMeta> beforeFiles =
                compactBeforeAllFiles ? allFiles : Collections.singletonList(allFiles.get(0));
        AppendCompactTask compactTask = new AppendCompactTask(BinaryRow.EMPTY_ROW, beforeFiles);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        BaseAppendFileStoreWrite write = new NoopAppendWrite(store);

        CommitMessageImpl compactMessage = (CommitMessageImpl) compactTask.doCompact(table, write);

        assertThat(compactMessage.compactIncrement().deletedIndexFiles()).isNotEmpty();
        if (compactBeforeAllFiles) {
            assertThat(compactMessage.compactIncrement().newIndexFiles()).isEmpty();
        } else {
            assertThat(compactMessage.compactIncrement().newIndexFiles()).isNotEmpty();
        }
    }

    private static class NoopAppendWrite extends BaseAppendFileStoreWrite {

        public NoopAppendWrite(TestAppendFileStore store) {
            this(
                    store.fileIO(),
                    store.newRead(),
                    0L,
                    store.schema().logicalRowType(),
                    store.schema().logicalPartitionType(),
                    store.pathFactory(),
                    store.snapshotManager(),
                    store.newScan(),
                    store.options(),
                    BucketedDvMaintainer.factory(store.newIndexFileHandler()),
                    "test");
        }

        private NoopAppendWrite(
                FileIO fileIO,
                RawFileSplitRead readForCompact,
                long schemaId,
                RowType rowType,
                RowType partitionType,
                FileStorePathFactory pathFactory,
                SnapshotManager snapshotManager,
                FileStoreScan scan,
                CoreOptions options,
                @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
                String tableName) {
            super(
                    fileIO,
                    readForCompact,
                    schemaId,
                    rowType,
                    partitionType,
                    pathFactory,
                    snapshotManager,
                    scan,
                    options,
                    dvMaintainerFactory,
                    tableName);
        }

        @Override
        public List<DataFileMeta> compactRewrite(
                BinaryRow partition,
                int bucket,
                @Nullable Function<String, DeletionVector> dvFactory,
                List<DataFileMeta> toCompact)
                throws Exception {
            return Collections.emptyList();
        }

        @Override
        protected CompactManager getCompactManager(
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> restoredFiles,
                ExecutorService compactExecutor,
                @Nullable BucketedDvMaintainer dvMaintainer) {
            return null;
        }

        @Override
        protected Function<WriterContainer<InternalRow>, Boolean> createWriterCleanChecker() {
            return null;
        }
    }

    private TestAppendFileStore createAppendStore(
            java.nio.file.Path tempDir, Map<String, String> dynamicOptions) throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        FileIO fileIO = FileIOFinder.find(new Path(root));
        SchemaManager schemaManage = new SchemaManager(new LocalFileIO(), path);

        Map<String, String> options = new HashMap<>(dynamicOptions);
        options.put(CoreOptions.PATH.key(), root);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        schemaManage,
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options,
                                null));
        return new TestAppendFileStore(
                fileIO,
                schemaManage,
                new CoreOptions(options),
                tableSchema,
                RowType.of(),
                RowType.of(),
                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                (new Path(root)).getName());
    }
}
