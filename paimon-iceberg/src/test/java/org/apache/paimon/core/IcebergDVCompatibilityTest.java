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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Iceberg DV compatibility. */
public class IcebergDVCompatibilityTest {
    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSyncDVWithoutBase() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        // must set deletion-vectors.bitmap64 = true
        customOptions.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        // must set metadata.iceberg.format-version = 3
        customOptions.put(IcebergOptions.FORMAT_VERSION.key(), "3");

        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        customOptions);

        // disable iceberg metadata storage
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.DISABLED.toString());
        table = table.copy(options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        write.write(GenericRow.ofKind(RowKind.DELETE, 2, 20));
        commit.commit(2, write.prepareCommit(false, 2));

        // produce dv
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        commit.commit(3, write.prepareCommit(true, 3));

        assertThat(
                        table.store()
                                .newIndexFileHandler()
                                .scan(DeletionVectorsIndexFile.DELETION_VECTORS_INDEX)
                                .size())
                .isGreaterThan(0);
        // enable iceberg metadata storage and commit changes to iceberg
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        table = table.copy(options);
        write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        commit.commit(4, write.prepareCommit(false, 4));

        validateIcebergResult(Collections.singletonList(new Object[] {1, 10}));

        write.close();
        commit.close();
    }

    @Test
    public void testSyncDVWithBase() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        // must set deletion-vectors.bitmap64 = true
        customOptions.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        // must set metadata.iceberg.format-version = 3
        customOptions.put(IcebergOptions.FORMAT_VERSION.key(), "3");

        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        customOptions);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 1));
        write.write(GenericRow.of(2, 2));
        write.write(GenericRow.of(3, 3));
        commit.commit(1, write.prepareCommit(false, 1));
        validateIcebergResult(
                Arrays.asList(new Object[] {1, 1}, new Object[] {2, 2}, new Object[] {3, 3}));

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(4, 4));
        // compact to generate dv index
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        commit.commit(2, write.prepareCommit(true, 2));
        validateIcebergResult(
                Arrays.asList(
                        new Object[] {1, 11},
                        new Object[] {2, 2},
                        new Object[] {3, 3},
                        new Object[] {4, 4}));

        // level-0 file will not be added to iceberg
        write.write(GenericRow.of(2, 22));
        write.write(GenericRow.of(5, 5));
        commit.commit(3, write.prepareCommit(false, 3));
        validateIcebergResult(
                Arrays.asList(
                        new Object[] {1, 11},
                        new Object[] {2, 2},
                        new Object[] {3, 3},
                        new Object[] {4, 4}));

        // compact to generate dv index
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        commit.commit(4, write.prepareCommit(true, 4));
        validateIcebergResult(
                Arrays.asList(
                        new Object[] {1, 11},
                        new Object[] {2, 22},
                        new Object[] {3, 3},
                        new Object[] {4, 4},
                        new Object[] {5, 5}));

        write.close();
        commit.close();
    }

    @Test
    public void testUpgradeFormatVersion() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        // must set deletion-vectors.bitmap64 = true
        customOptions.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");

        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        customOptions);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 1));
        write.write(GenericRow.of(2, 2));
        commit.commit(1, write.prepareCommit(false, 1));
        validateIcebergResult(Arrays.asList(new Object[] {1, 1}, new Object[] {2, 2}));

        // upgrade format version to 3
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        table = table.copy(options);
        write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 3));
        // compact to generate dv index
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        commit.commit(2, write.prepareCommit(true, 2));
        validateIcebergResult(
                Arrays.asList(new Object[] {1, 11}, new Object[] {2, 2}, new Object[] {3, 3}));

        write.close();
        commit.close();
    }

    private FileStoreTable createPaimonTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 4);
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private void validateIcebergResult(List<Object[]> expected) throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);

        Types.StructType type = icebergTable.schema().asStruct();

        StructLikeSet actualSet = StructLikeSet.create(type);
        StructLikeSet expectSet = StructLikeSet.create(type);

        try (CloseableIterable<Record> reader = IcebergGenerics.read(icebergTable).build()) {
            reader.forEach(actualSet::add);
        }
        expectSet.addAll(
                expected.stream().map(r -> icebergRecord(type, r)).collect(Collectors.toList()));

        assertThat(actualSet).isEqualTo(expectSet);
    }

    private org.apache.iceberg.data.GenericRecord icebergRecord(
            Types.StructType type, Object[] row) {
        org.apache.iceberg.data.GenericRecord record =
                org.apache.iceberg.data.GenericRecord.create(type);
        for (int i = 0; i < row.length; i++) {
            record.set(i, row[i]);
        }
        return record;
    }
}
