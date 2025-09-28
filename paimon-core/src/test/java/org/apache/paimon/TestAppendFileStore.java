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

package org.apache.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.AppendDeletionFileMaintainerHelper;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** Wrapper of AppendOnlyFileStore. */
public class TestAppendFileStore extends AppendOnlyFileStore {

    private final String commitUser;

    private final IndexFileHandler fileHandler;

    private long commitIdentifier;

    private FileIO fileIO;

    public TestAppendFileStore(
            FileIO fileIO,
            SchemaManager schemaManage,
            CoreOptions options,
            TableSchema tableSchema,
            RowType partitionType,
            RowType bucketType,
            RowType rowType,
            String tableName) {
        super(
                fileIO,
                schemaManage,
                tableSchema,
                options,
                partitionType,
                bucketType,
                rowType,
                tableName,
                CatalogEnvironment.empty());

        this.fileIO = fileIO;
        this.commitUser = UUID.randomUUID().toString();
        this.fileHandler = this.newIndexFileHandler();
        this.commitIdentifier = 0L;
    }

    public FileIO fileIO() {
        return this.fileIO;
    }

    public TableSchema schema() {
        return schema;
    }

    public FileStoreCommitImpl newCommit() {
        return super.newCommit(commitUser, null);
    }

    public void commit(CommitMessage... commitMessages) {
        ManifestCommittable committable = new ManifestCommittable(commitIdentifier++);
        for (CommitMessage commitMessage : commitMessages) {
            committable.addFileCommittable(commitMessage);
        }
        newCommit().commit(committable, false);
    }

    public List<IndexFileMeta> scanDVIndexFiles(BinaryRow partition, int bucket) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        return fileHandler.scan(latestSnapshot, DELETION_VECTORS_INDEX, partition, bucket);
    }

    public AppendDeleteFileMaintainer createDVIFMaintainer(
            BinaryRow partition, Map<String, DeletionFile> dataFileToDeletionFiles) {
        return AppendDeletionFileMaintainerHelper.fromDeletionFiles(
                fileHandler, partition, dataFileToDeletionFiles);
    }

    public Map<String, DeletionVector> deletionVectors(BinaryRow partition, int bucket) {
        return fileHandler.readAllDeletionVectors(
                partition, bucket, scanDVIndexFiles(partition, bucket));
    }

    public CommitMessageImpl writeDVIndexFiles(
            BinaryRow partition, Map<String, List<Integer>> dataFileToPositions, boolean bitmap64) {
        AppendDeleteFileMaintainer dvMaintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        fileHandler, snapshotManager().latestSnapshot(), partition);
        for (Map.Entry<String, List<Integer>> entry : dataFileToPositions.entrySet()) {
            DeletionVector dv =
                    bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
            for (Integer i : entry.getValue()) {
                dv.delete(i);
            }
            dvMaintainer.notifyNewDeletionVector(entry.getKey(), dv);
        }
        List<IndexManifestEntry> persist = dvMaintainer.persist();
        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry indexManifestEntry : persist) {
            if (indexManifestEntry.kind() == FileKind.ADD) {
                newIndexFiles.add(indexManifestEntry.indexFile());
            } else {
                deletedIndexFiles.add(indexManifestEntry.indexFile());
            }
        }
        return new CommitMessageImpl(
                partition,
                0,
                options().bucket(),
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        newIndexFiles,
                        deletedIndexFiles),
                CompactIncrement.emptyIncrement());
    }

    public static TestAppendFileStore createAppendStore(
            java.nio.file.Path tempDir, Map<String, String> options) throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        FileIO fileIO = FileIOFinder.find(new Path(root));
        SchemaManager schemaManage = new SchemaManager(new LocalFileIO(), path);

        options.put(CoreOptions.PATH.key(), root);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        schemaManage,
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                Collections.emptyList(),
                                options,
                                null));
        return new TestAppendFileStore(
                fileIO,
                schemaManage,
                new CoreOptions(options),
                tableSchema,
                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                RowType.of(),
                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                (new Path(root)).getName());
    }
}
