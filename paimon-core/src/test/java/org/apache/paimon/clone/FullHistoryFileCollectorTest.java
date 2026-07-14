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

package org.apache.paimon.clone;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FullHistoryFileCollector}. */
public class FullHistoryFileCollectorTest {

    @TempDir private java.nio.file.Path tempDir;
    @TempDir private java.nio.file.Path externalDir;

    private final FileIO fileIO = LocalFileIO.create();
    private int tableId = 0;

    @Test
    public void testCollectReachableFilesFromSnapshotsAndTags() throws Exception {
        FileStoreTable table = createTable(new Options());
        writeRows(table, 0, "A", 1, 2);
        table.createTag("tag1", 1);
        table.createBranch("branch1", "tag1");
        writeRows(table, 1, "B", 3);

        FullHistoryFileSet files = new FullHistoryFileCollector(table).collect();

        assertThat(files.metadataFiles()).anyMatch(path -> path.getName().startsWith("schema-"));
        assertThat(files.metadataFiles()).anyMatch(path -> path.getName().startsWith("snapshot-"));
        assertThat(files.metadataFiles()).anyMatch(path -> path.getName().startsWith("tag-"));
        assertThat(files.metadataFiles())
                .anyMatch(path -> path.getName().startsWith("manifest-list-"));
        assertThat(files.metadataFiles()).anyMatch(path -> path.getName().startsWith("manifest-"));
        assertThat(files.dataFiles()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(files.allFiles()).allMatch(this::exists);
    }

    @Test
    public void testCollectExternalDataFilesAsAbsolutePaths() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, "file://" + externalDir);
        options.set(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, ExternalPathStrategy.ROUND_ROBIN);
        FileStoreTable table = createTable(options);
        writeRows(table, 0, "A", 1);

        Set<Path> dataFiles = new FullHistoryFileCollector(table).collect().dataFiles();

        assertThat(dataFiles).isNotEmpty();
        assertThat(dataFiles).allMatch(path -> path.toString().contains(externalDir.toString()));
        assertThat(dataFiles).allMatch(this::exists);
    }

    @Test
    public void testStreamingPayloadVisitorMatchesReachableFileSet() throws Exception {
        FileStoreTable table = createTable(new Options());
        writeRows(table, 0, "A", 1);
        table.createTag("tag1", 1);
        table.createBranch("branch1", "tag1");
        writeRows(table.switchToBranch("branch1"), 1, "B", 2);
        writeRows(table, 1, "C", 3);

        FullHistoryFileSet expected = new FullHistoryFileCollector(table).collect();
        Set<Path> dataFiles = new LinkedHashSet<>();
        Set<Path> indexFiles = new LinkedHashSet<>();
        new FullHistoryPayloadFileVisitor(table)
                .visit(
                        (path, kind, size) -> {
                            if (kind == FullHistoryCopyPlan.FileKind.DATA) {
                                dataFiles.add(path);
                            } else {
                                indexFiles.add(path);
                            }
                        });

        assertThat(dataFiles).isEqualTo(expected.dataFiles());
        assertThat(indexFiles).isEqualTo(expected.indexFiles());
    }

    private boolean exists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FileStoreTable createTable(Options options) throws Exception {
        Path tablePath = new Path(tempDir.resolve("table-" + tableId++).toString());
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "pt"});
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "id");

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }

    private void writeRows(
            FileStoreTable table, long commitIdentifier, String partition, int... ids)
            throws Exception {
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        try {
            for (int id : ids) {
                write.write(GenericRow.of(id, BinaryString.fromString(partition)));
            }
            commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
        } finally {
            write.close();
            commit.close();
        }
    }
}
