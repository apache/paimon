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

package org.apache.paimon.core;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Iceberg format-version 3 row-lineage metadata fields. */
public class IcebergRowLineageCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testFreshV3MetadataHasRowLineageFields() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        IcebergMetadata metadata = readIcebergMetadata(table, 1);
        assertThat(metadata.formatVersion()).isEqualTo(3);
        assertThat(metadata.nextRowId()).isEqualTo(2L);
        IcebergSnapshot snapshot = metadata.currentSnapshot();
        assertThat(snapshot.firstRowId()).isEqualTo(0L);
        assertThat(snapshot.addedRows()).isEqualTo(2L);

        // the bundled Iceberg parser must still accept the metadata
        TableMetadata parsed = TableMetadataParser.fromJson(readMetadataJson(table, 1));
        assertThat(parsed.formatVersion()).isEqualTo(3);
    }

    @Test
    public void testV2MetadataHasNoRowLineageFields() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(2), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        String json = readMetadataJson(table, 1);
        assertThat(json)
                .doesNotContain("next-row-id")
                .doesNotContain("first-row-id")
                .doesNotContain("added-rows");
        IcebergMetadata metadata = IcebergMetadata.fromJson(json);
        assertThat(metadata.nextRowId()).isNull();
        assertThat(metadata.currentSnapshot().firstRowId()).isNull();
        assertThat(metadata.currentSnapshot().addedRows()).isNull();
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private RowType defaultRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
    }

    private Map<String, String> formatVersionOptions(int formatVersion) {
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.FORMAT_VERSION.key(), String.valueOf(formatVersion));
        return options;
    }

    private FileStoreTable createPaimonTable(
            RowType rowType, Map<String, String> customOptions, String fileFormat)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, -1);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, fileFormat);
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));

        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private Path metadataPath(FileStoreTable table, long snapshotId) {
        return new Path(table.location(), String.format("metadata/v%d.metadata.json", snapshotId));
    }

    private IcebergMetadata readIcebergMetadata(FileStoreTable table, long snapshotId) {
        return IcebergMetadata.fromPath(LocalFileIO.create(), metadataPath(table, snapshotId));
    }

    private String readMetadataJson(FileStoreTable table, long snapshotId) throws Exception {
        return LocalFileIO.create().readFileUtf8(metadataPath(table, snapshotId));
    }
}
