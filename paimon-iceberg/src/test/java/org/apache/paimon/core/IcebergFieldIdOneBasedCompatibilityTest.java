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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergStructType;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.schema.GroupType;
import org.apache.paimon.shade.org.apache.parquet.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CoreOptions#FIELD_ID_ONE_BASED} with Iceberg compatibility: a table created with
 * 1-based field ids must emit strictly positive ids in Iceberg metadata that exactly match the ids
 * embedded in the Parquet data files (readers like Snowflake reject field id 0 and resolve columns
 * by the physical ids).
 */
public class IcebergFieldIdOneBasedCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    private RowType rowType() {
        return new RowType(
                Arrays.asList(
                        new DataField(0, "pt", DataTypes.INT().notNull()),
                        new DataField(1, "k", DataTypes.INT().notNull()),
                        new DataField(2, "v", DataTypes.STRING()),
                        new DataField(
                                3,
                                "nested",
                                new RowType(
                                        Arrays.asList(
                                                new DataField(4, "a", DataTypes.INT()),
                                                new DataField(5, "b", DataTypes.STRING()))))));
    }

    @Test
    public void testStrictModePrimaryKeyDvTable() throws Exception {
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(CoreOptions.FIELD_ID_ONE_BASED.key(), "true");
        customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        customOptions.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        customOptions.put(IcebergOptions.FORMAT_VERSION.key(), "3");

        FileStoreTable table = createPaimonTable(customOptions);

        // the Paimon schema itself is 1-based, top-level and nested alike
        List<DataField> fields = table.schema().fields();
        assertThat(fields.stream().map(DataField::id)).containsExactly(1, 2, 3, 4);
        RowType nested = (RowType) fields.get(3).type();
        assertThat(nested.getFields().stream().map(DataField::id)).containsExactly(5, 6);
        assertThat(table.schema().highestFieldId()).isEqualTo(6);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(row(RowKind.INSERT, 1, 1, "a", 10, "x"));
        write.write(row(RowKind.INSERT, 1, 2, "b", 20, "y"));
        commit.commit(1, write.prepareCommit(false, 1));

        write.write(row(RowKind.DELETE, 1, 2, "b", 20, "y"));
        commit.commit(2, write.prepareCommit(false, 2));

        // produce a deletion vector
        write.compact(partition(1), 0, false);
        commit.commit(3, write.prepareCommit(true, 3));
        write.close();
        commit.close();

        IcebergMetadata metadata = readLatestIcebergMetadata(table);

        // 1) metadata field ids are strictly positive and match the Paimon schema
        assertThat(metadata.formatVersion()).isEqualTo(3);
        for (IcebergSchema schema : metadata.schemas()) {
            assertThat(collectAllFieldIds(schema.fields())).allMatch(id -> id >= 1);
        }
        IcebergSchema currentSchema = metadata.schemas().get(metadata.currentSchemaId());
        assertThat(currentSchema.fields().stream().map(IcebergDataField::id))
                .containsExactly(1, 2, 3, 4);
        IcebergStructType nestedType = (IcebergStructType) currentSchema.fields().get(3).type();
        assertThat(nestedType.fields().stream().map(IcebergDataField::id)).containsExactly(5, 6);
        assertThat(metadata.lastColumnId()).isEqualTo(4);

        // 2) partition source ids reference the shifted ids
        assertThat(metadata.partitionSpecs()).hasSize(1);
        List<IcebergPartitionField> partitionFields = metadata.partitionSpecs().get(0).fields();
        assertThat(partitionFields).hasSize(1);
        assertThat(partitionFields.get(0).sourceId()).isEqualTo(1);
        assertThat(partitionFields.get(0).fieldId())
                .isGreaterThanOrEqualTo(IcebergPartitionField.FIRST_FIELD_ID);

        // 3) v3 data manifest-list entries carry a non-null first_row_id
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        List<IcebergManifestFileMeta> metas =
                manifestList.read(new Path(metadata.currentSnapshot().manifestList()).getName());
        assertThat(metas).isNotEmpty();
        assertThat(metas.stream().filter(m -> m.content() == IcebergManifestFileMeta.Content.DATA))
                .allMatch(m -> m.firstRowId() != null);

        // 4) Parquet footers embed exactly the metadata ids
        List<java.nio.file.Path> parquetFiles = dataParquetFiles(table);
        assertThat(parquetFiles).isNotEmpty();
        for (java.nio.file.Path file : parquetFiles) {
            MessageType parquetSchema = readParquetSchema(file);
            assertFieldId(parquetSchema, "pt", 1);
            assertFieldId(parquetSchema, "k", 2);
            assertFieldId(parquetSchema, "v", 3);
            assertFieldId(parquetSchema, "nested", 4);
            GroupType nestedGroup = parquetSchema.getType("nested").asGroupType();
            assertFieldId(nestedGroup, "a", 5);
            assertFieldId(nestedGroup, "b", 6);
        }

        // 5) Apache Iceberg reads the table (schema ids and data, with the DV applied)
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of("mydb.db", "t"));
        assertThat(icebergTable.schema().findField("pt").fieldId()).isEqualTo(1);
        assertThat(icebergTable.schema().findField("k").fieldId()).isEqualTo(2);
        assertThat(icebergTable.schema().findField("v").fieldId()).isEqualTo(3);
        assertThat(icebergTable.schema().findField("nested").fieldId()).isEqualTo(4);
        assertThat(icebergTable.schema().findField("nested.a").fieldId()).isEqualTo(5);
        assertThat(icebergTable.schema().findField("nested.b").fieldId()).isEqualTo(6);

        Types.StructType structType = icebergTable.schema().asStruct();
        StructLikeSet actual = StructLikeSet.create(structType);
        try (CloseableIterable<Record> reader = IcebergGenerics.read(icebergTable).build()) {
            reader.forEach(actual::add);
        }
        org.apache.iceberg.data.GenericRecord expected =
                org.apache.iceberg.data.GenericRecord.create(structType);
        expected.set(0, 1);
        expected.set(1, 1);
        expected.set(2, "a");
        org.apache.iceberg.data.GenericRecord expectedNested =
                org.apache.iceberg.data.GenericRecord.create(
                        structType.fieldType("nested").asStructType());
        expectedNested.set(0, 10);
        expectedNested.set(1, "x");
        expected.set(3, expectedNested);
        StructLikeSet expectedSet = StructLikeSet.create(structType);
        expectedSet.add(expected);
        assertThat(actual).isEqualTo(expectedSet);

        // 6) Paimon itself reads the strict-mode files
        List<InternalRow> rows = readPaimonRows(table);
        assertThat(rows).hasSize(1);
        InternalRow row = rows.get(0);
        assertThat(row.getInt(0)).isEqualTo(1);
        assertThat(row.getInt(1)).isEqualTo(1);
        assertThat(row.getString(2).toString()).isEqualTo("a");
        assertThat(row.getRow(3, 2).getInt(0)).isEqualTo(10);
        assertThat(row.getRow(3, 2).getString(1).toString()).isEqualTo("x");
    }

    @Test
    public void testDefaultRemainsZeroBased() throws Exception {
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        FileStoreTable table = createPaimonTable(customOptions);

        assertThat(table.schema().fields().stream().map(DataField::id)).containsExactly(0, 1, 2, 3);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(row(RowKind.INSERT, 1, 1, "a", 10, "x"));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        IcebergMetadata metadata = readLatestIcebergMetadata(table);
        IcebergSchema currentSchema = metadata.schemas().get(metadata.currentSchemaId());
        assertThat(currentSchema.fields().stream().map(IcebergDataField::id))
                .containsExactly(0, 1, 2, 3);
    }

    private FileStoreTable createPaimonTable(Map<String, String> customOptions) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, 1);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, "parquet");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));

        Schema schema =
                new Schema(
                        rowType().getFields(),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private static GenericRow row(RowKind kind, int pt, int k, String v, int a, String b) {
        return GenericRow.ofKind(
                kind,
                pt,
                k,
                BinaryString.fromString(v),
                GenericRow.of(a, BinaryString.fromString(b)));
    }

    private static BinaryRow partition(int pt) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, pt);
        writer.complete();
        return partition;
    }

    private IcebergMetadata readLatestIcebergMetadata(FileStoreTable table) throws IOException {
        java.nio.file.Path metadataDir =
                java.nio.file.Paths.get(new Path(table.location(), "metadata").toUri().getPath());
        java.nio.file.Path latest;
        try (Stream<java.nio.file.Path> files = Files.list(metadataDir)) {
            latest =
                    files.filter(f -> f.getFileName().toString().endsWith(".metadata.json"))
                            .max(
                                    java.util.Comparator.comparingLong(
                                            f ->
                                                    Long.parseLong(
                                                            f.getFileName()
                                                                    .toString()
                                                                    .replaceAll("[^0-9]", ""))))
                            .orElseThrow(() -> new IllegalStateException("no metadata.json found"));
        }
        return IcebergMetadata.fromPath(LocalFileIO.create(), new Path(latest.toUri()));
    }

    private List<java.nio.file.Path> dataParquetFiles(FileStoreTable table) throws IOException {
        java.nio.file.Path tableDir = java.nio.file.Paths.get(table.location().toUri().getPath());
        try (Stream<java.nio.file.Path> files = Files.walk(tableDir)) {
            return files.filter(f -> f.getFileName().toString().endsWith(".parquet"))
                    .filter(f -> !f.toString().contains("/metadata/"))
                    .collect(Collectors.toList());
        }
    }

    private MessageType readParquetSchema(java.nio.file.Path file) {
        try (ParquetFileReader reader =
                org.apache.paimon.format.parquet.ParquetUtil.getParquetReader(
                        LocalFileIO.create(),
                        new Path(file.toUri()),
                        Files.size(file),
                        new Options())) {
            return reader.getFooter().getFileMetaData().getSchema();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertFieldId(GroupType parent, String name, int expectedId) {
        org.apache.paimon.shade.org.apache.parquet.schema.Type field = parent.getType(name);
        assertThat(field.getId()).as("field id of '%s'", name).isNotNull();
        assertThat(field.getId().intValue()).as("field id of '%s'", name).isEqualTo(expectedId);
    }

    private static List<Integer> collectAllFieldIds(List<IcebergDataField> fields) {
        List<Integer> ids = new ArrayList<>();
        for (IcebergDataField field : fields) {
            ids.add(field.id());
            if (field.type() instanceof IcebergStructType) {
                ids.addAll(collectAllFieldIds(((IcebergStructType) field.type()).fields()));
            }
        }
        return ids;
    }

    private static List<InternalRow> readPaimonRows(FileStoreTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits)) {
            reader.forEachRemaining(
                    r ->
                            rows.add(
                                    GenericRow.of(
                                            r.getInt(0),
                                            r.getInt(1),
                                            r.getString(2).copy(),
                                            GenericRow.of(
                                                    r.getRow(3, 2).getInt(0),
                                                    r.getRow(3, 2).getString(1).copy()))));
        }
        return rows;
    }
}
