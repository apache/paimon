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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.RowTrackingTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for table with blob. */
public class BlobTableTest extends TableTestBase {

    private final byte[] blobBytes = randomBytes();

    @Test
    public void testBlobConsumer() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        List<BlobDescriptor> blobs = new ArrayList<>();
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.withBlobConsumer(
                    (blobFieldName, blobDescriptor) -> {
                        assertThat(blobFieldName).isEqualTo("f2");
                        blobs.add(blobDescriptor);
                        return true;
                    });
            write.write(dataDefault(0, 0));
            write.write(dataDefault(0, 0));
            write.write(GenericRow.of(1, BinaryString.fromString("nice"), null));
        }
        assertThat(blobs.size()).isEqualTo(3);
        FileIO fileIO = table.fileIO();
        UriReader uriReader = UriReader.fromFile(fileIO);
        for (int i = 0; i < 2; i++) {
            BlobDescriptor blob = blobs.get(i);
            assertThat(Blob.fromDescriptor(uriReader, blob).toData()).isEqualTo(blobBytes);
        }
        for (int i = 0; i < 2; i++) {
            BlobDescriptor blob = blobs.get(i);
            fileIO.deleteQuietly(new Path(blob.uri()));
        }
        assertThat(blobs.get(2)).isNull();
    }

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 1));

        AtomicInteger integer = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                DataEvolutionSplitRead.splitFieldBunches(
                        filesMetas, key -> makeBlobRowType(key.writeCols(), f -> 0));

        assertThat(fieldGroups.size()).isEqualTo(2);
        assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(1).files().size()).isEqualTo(10);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    }
                });

        assertThat(integer.get()).isEqualTo(1000);
    }

    @Test
    public void testUpdateBlobColumn() throws Exception {
        createTableDefault();

        byte[] blob0 = "blob-0".getBytes();
        byte[] blob1 = "blob-1".getBytes();
        byte[] blob2 = "blob-2".getBytes();
        writeDataDefault(
                Arrays.asList(
                        GenericRow.of(0, BinaryString.fromString("row-0"), new BlobData(blob0)),
                        GenericRow.of(1, BinaryString.fromString("row-1"), new BlobData(blob1)),
                        GenericRow.of(2, BinaryString.fromString("row-2"), new BlobData(blob2))));

        byte[] updatedBlob1 = "updated-blob-1".getBytes();
        FileStoreTable table = getTableDefault();
        RowType blobWriteType = table.schema().logicalRowType().project("f2");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(blobWriteType);
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(BlobPlaceholder.INSTANCE));
            write.write(GenericRow.of(new BlobData(updatedBlob1)));
            write.write(GenericRow.of(BlobPlaceholder.INSTANCE));

            List<CommitMessage> commitMessages = write.prepareCommit();
            assignFirstRowId(commitMessages, 0L);
            commit.commit(commitMessages);
        }

        Map<Integer, byte[]> actual = new HashMap<>();
        readDefault(row -> actual.put(row.getInt(0), row.getBlob(2).toData()));

        assertThat(actual.size()).isEqualTo(3);
        assertThat(actual.get(0)).isEqualTo(blob0);
        assertThat(actual.get(1)).isEqualTo(updatedBlob1);
        assertThat(actual.get(2)).isEqualTo(blob2);
    }

    @Test
    public void testCompactUpdatedBlobColumn() throws Exception {
        createTableDefault();

        byte[] blob0 = "blob-0".getBytes();
        byte[] blob1 = "blob-1".getBytes();
        byte[] blob2 = "blob-2".getBytes();
        writeDataDefault(
                Arrays.asList(
                        GenericRow.of(0, BinaryString.fromString("row-0"), new BlobData(blob0)),
                        GenericRow.of(1, BinaryString.fromString("row-1"), new BlobData(blob1)),
                        GenericRow.of(2, BinaryString.fromString("row-2"), new BlobData(blob2))));

        byte[] updatedBlob1 = "updated-blob-1".getBytes();
        FileStoreTable table = getTableDefault();
        RowType blobWriteType = table.schema().logicalRowType().project("f2");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(blobWriteType);
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(BlobPlaceholder.INSTANCE));
            write.write(GenericRow.of(new BlobData(updatedBlob1)));
            write.write(GenericRow.of(BlobPlaceholder.INSTANCE));

            List<CommitMessage> commitMessages = write.prepareCommit();
            assignFirstRowId(commitMessages, 0L);
            commit.commit(commitMessages);
        }

        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, true, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(DataEvolutionCompactTask::isBlobTask)).isTrue();

        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        Map<Integer, byte[]> actual = new HashMap<>();
        readDefault(row -> actual.put(row.getInt(0), row.getBlob(2).toData()));

        assertThat(actual.size()).isEqualTo(3);
        assertThat(actual.get(0)).isEqualTo(blob0);
        assertThat(actual.get(1)).isEqualTo(updatedBlob1);
        assertThat(actual.get(2)).isEqualTo(blob2);
    }

    @Test
    public void testWriteByInputStream() throws Exception {
        createTableDefault();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(blobBytes);

        GenericRow genericRow = new GenericRow(3);
        genericRow.setField(0, 0);
        genericRow.setField(1, BinaryString.fromString("nice"));
        genericRow.setField(
                2, Blob.fromInputStream(() -> SeekableInputStream.wrap(byteArrayInputStream)));

        writeDataDefault(Collections.singletonList(genericRow));

        readDefault(row -> assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes));
    }

    @Test
    public void testMultiBatch() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 2));

        AtomicInteger integer = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(2);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(
                            batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
            assertThat(fieldGroups.size()).isEqualTo(2);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(10);
        }

        readDefault(
                row -> {
                    if (integer.getAndIncrement() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    }
                });
        assertThat(integer.get()).isEqualTo(2000);
    }

    @Test
    public void testRowIdPushDown() throws Exception {
        createTableDefault();
        writeDataDefault(
                new Iterable<InternalRow>() {
                    @Nonnull
                    @Override
                    public Iterator<InternalRow> iterator() {
                        return new Iterator<InternalRow>() {
                            int i = 0;

                            @Override
                            public boolean hasNext() {
                                return i < 200;
                            }

                            @Override
                            public InternalRow next() {
                                i++;
                                return (i - 1) == 100
                                        ? GenericRow.of(
                                                i,
                                                BinaryString.fromString("nice"),
                                                new BlobData(
                                                        "This is the specified message".getBytes()))
                                        : dataDefault(0, 0);
                            }
                        };
                    }
                });

        Table table = getTableDefault();
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withRowRanges(Collections.singletonList(new Range(100L, 100L)));
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    i.getAndIncrement();
                    assertThat(row.getBlob(2).toData())
                            .isEqualTo("This is the specified message".getBytes());
                });
        assertThat(i.get()).isEqualTo(1);
    }

    @Test
    public void testRolling() throws Exception {
        createTableDefault();
        commitDefault(writeDataDefault(1025, 1));
        AtomicInteger integer = new AtomicInteger(0);
        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    }
                });
        assertThat(integer.get()).isEqualTo(1025);
    }

    @Test
    public void testDescriptorStorageModeReuseUpstreamBlobWithoutCopy() throws Exception {
        createDescriptorTable();
        FileStoreTable table = getTableDefault();

        // prepare an "upstream blob file" (any file) and write blob bytes into it
        Path external = new Path(tempPath.resolve("upstream-blob.bin").toString());
        writeFile(table.fileIO(), external, blobBytes);

        BlobDescriptor descriptor = new BlobDescriptor(external.toString(), 0, blobBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(1, BinaryString.fromString("nice"), blobRef)));

        readDefault(
                row -> {
                    assertThat(row.getBlob(2).toDescriptor()).isEqualTo(descriptor);
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                });

        long blobFiles = countFilesWithSuffix(table.fileIO(), table.location(), ".blob");
        assertThat(blobFiles).isEqualTo(0);
    }

    @Test
    public void testDescriptorStorageModeRejectsNonDescriptorInput() throws Exception {
        createDescriptorTable();

        assertThatThrownBy(
                        () ->
                                writeDataDefault(
                                        Collections.singletonList(
                                                GenericRow.of(
                                                        1,
                                                        BinaryString.fromString("bad"),
                                                        new BlobData(blobBytes)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blob-descriptor-field");
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "avro", "orc"})
    public void testMixedBlobStorageModeByFields(String format) throws Exception {
        createMixedModeTable(format);
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-mixed-blob.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("mixed"),
                                new BlobData(blobBytes),
                                blobRef)));

        readDefault(
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("mixed");
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    assertThat(row.getBlob(3).toDescriptor()).isEqualTo(descriptor);
                    assertThat(row.getBlob(3).toData()).isEqualTo(descriptorBytes);
                });

        long blobFiles = countFilesWithSuffix(table.fileIO(), table.location(), ".blob");
        assertThat(blobFiles).isEqualTo(1);
    }

    @Test
    public void testMixedBlobStorageModeRejectsNonDescriptorInput() throws Exception {
        createMixedModeTable();

        assertThatThrownBy(
                        () ->
                                writeDataDefault(
                                        Collections.singletonList(
                                                GenericRow.of(
                                                        1,
                                                        BinaryString.fromString("bad"),
                                                        new BlobData(blobBytes),
                                                        new BlobData(randomBytes())))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blob-descriptor-field");
    }

    @Test
    public void testExternalStorageBlobField() throws Exception {
        createExternalStorageTable();
        FileStoreTable table = getTableDefault();

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1, BinaryString.fromString("copy-test"), new BlobData(blobBytes))));

        readDefault(
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("copy-test");
                    // The blob should be readable via descriptor
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                });

        // Verify the blob file was written to the external storage path
        java.nio.file.Path externalStoragePath = tempPath.resolve("external-storage-blob-path");
        assertThat(Files.exists(externalStoragePath)).isTrue();
        try (Stream<java.nio.file.Path> stream = Files.list(externalStoragePath)) {
            long externalStorageFiles =
                    stream.filter(p -> p.getFileName().toString().endsWith(".blob")).count();
            assertThat(externalStorageFiles).isGreaterThanOrEqualTo(1);
        }

        // Verify no .blob files were created in the table directory
        long blobFiles = countFilesWithSuffix(table.fileIO(), table.location(), ".blob");
        assertThat(blobFiles).isEqualTo(0);
    }

    @Test
    public void testThreeTypeBlobCoexistence() throws Exception {
        createThreeTypeBlobTable();
        FileStoreTable table = getTableDefault();

        // Prepare external blob for the descriptor field
        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-three-type.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        // Prepare data for the descriptor field backed by external storage
        byte[] copyBytes = randomBytes();

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("three-types"),
                                new BlobData(blobBytes), // raw-data blob
                                blobRef, // descriptor blob
                                new BlobData(copyBytes)))); // descriptor blob with external storage

        readDefault(
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("three-types");
                    // Raw-data blob
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    // Descriptor blob
                    assertThat(row.getBlob(3).toDescriptor()).isEqualTo(descriptor);
                    assertThat(row.getBlob(3).toData()).isEqualTo(descriptorBytes);
                    // External-storage descriptor blob
                    assertThat(row.getBlob(4).toData()).isEqualTo(copyBytes);
                });

        // Verify raw-data blob files exist (for f2)
        long blobFiles = countFilesWithSuffix(table.fileIO(), table.location(), ".blob");
        assertThat(blobFiles).isGreaterThanOrEqualTo(1);

        // Verify descriptor files backed by external storage exist in the configured path
        java.nio.file.Path externalStoragePath =
                tempPath.resolve("external-storage-blob-path-3type");
        assertThat(Files.exists(externalStoragePath)).isTrue();
        try (Stream<java.nio.file.Path> stream = Files.list(externalStoragePath)) {
            long externalStorageFiles =
                    stream.filter(p -> p.getFileName().toString().endsWith(".blob")).count();
            assertThat(externalStorageFiles).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    public void testExternalStorageFieldValidationRequiresPath() {
        assertThatThrownBy(
                        () -> {
                            Schema.Builder schemaBuilder = Schema.newBuilder();
                            schemaBuilder.column("f0", DataTypes.INT());
                            schemaBuilder.column("f1", DataTypes.STRING());
                            schemaBuilder.column("f2", DataTypes.BLOB());
                            schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
                            schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                            schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                            schemaBuilder.option(CoreOptions.BLOB_FIELD.key(), "f2");
                            schemaBuilder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "f2");
                            schemaBuilder.option(
                                    CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "f2");
                            // No external storage path set
                            catalog.createTable(identifier(), schemaBuilder.build(), true);
                        })
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "'"
                                + CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key()
                                + "' must be set when '"
                                + CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()
                                + "' is configured.");
    }

    @Test
    public void testExternalStorageFieldMustBeSubsetOfDescriptorField() {
        assertThatThrownBy(
                        () -> {
                            Schema.Builder schemaBuilder = Schema.newBuilder();
                            schemaBuilder.column("f0", DataTypes.INT());
                            schemaBuilder.column("f1", DataTypes.STRING());
                            schemaBuilder.column("f2", DataTypes.BLOB());
                            schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
                            schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                            schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                            // f2 is configured for external storage but not in
                            // blob-descriptor-field
                            schemaBuilder.option(
                                    CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "f2");
                            schemaBuilder.option(
                                    CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(), "/tmp/target");
                            catalog.createTable(identifier(), schemaBuilder.build(), true);
                        })
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Field 'f2' in '"
                                + CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()
                                + "' must also be in '"
                                + CoreOptions.BLOB_DESCRIPTOR_FIELD.key()
                                + "'.");
    }

    @Test
    public void testBlobInlineFieldCanDeclareBlobWithoutBlobField() throws Exception {
        assertCreateBlobInlineFieldWithoutBlobField(
                "blob_descriptor_without_blob_field", CoreOptions.BLOB_DESCRIPTOR_FIELD.key());
        assertCreateBlobInlineFieldWithoutBlobField(
                "blob_view_without_blob_field", CoreOptions.BLOB_VIEW_FIELD.key());
    }

    private void assertCreateBlobInlineFieldWithoutBlobField(String tableName, String optionKey)
            throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(optionKey, "f2");

        catalog.createTable(identifier(tableName), schemaBuilder.build(), true);

        assertThat(
                        catalog.getTable(identifier(tableName))
                                .rowType()
                                .getTypeAt(2)
                                .is(DataTypeRoot.BLOB))
                .isTrue();
    }

    @Test
    public void testReadRowTrackingWithBlobProjection() throws Exception {
        createTableDefault();
        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("test_row_id_projection"),
                                new BlobData(blobBytes))));

        // read from RowTrackingTable which appends _ROW_ID and _SEQUENCE_NUMBER to the schema
        FileStoreTable fileStoreTable = getTableDefault();
        RowTrackingTable rowTrackingTable = new RowTrackingTable(fileStoreTable);

        // read with projection: only _ROW_ID and f2 (blob)
        // row tracking schema indices: 0=f0, 1=f1, 2=f2, 3=_ROW_ID, 4=_SEQUENCE_NUMBER
        ReadBuilder projectedBuilder =
                rowTrackingTable.newReadBuilder().withProjection(new int[] {3, 2});
        RecordReader<InternalRow> projectedReader =
                projectedBuilder.newRead().createReader(projectedBuilder.newScan().plan());
        AtomicInteger projectedCount = new AtomicInteger(0);
        projectedReader.forEachRemaining(
                row -> {
                    projectedCount.incrementAndGet();
                    // field 0 = _ROW_ID
                    assertThat(row.isNullAt(0)).isFalse();
                    // field 1 = f2 (blob)
                    assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes);
                });
        assertThat(projectedCount.get()).isEqualTo(1);
    }

    @Test
    void testReadBlobAfterAlterTableAndCompaction() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "100 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        // Step 1: write data with schemaId=0
        commitDefault(writeDataDefault(100, 1));

        // Step 2: ALTER TABLE SET an unrelated option -> schemaId becomes 1
        catalog.alterTable(
                identifier(), SchemaChange.setOption("snapshot.num-retained.min", "5"), false);

        // Step 3: write more data with schemaId=1
        commitDefault(writeDataDefault(100, 1));

        // Step 4: compact blob table using DataEvolutionCompactCoordinator
        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.size()).isGreaterThan(0);
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        // Step 5: read after compaction
        readDefault(row -> assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes));
    }

    @Test
    void testReadBlobAfterAddColumnAndCompaction() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "100 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        {
            FileStoreTable t = getTableDefault();
            StreamWriteBuilder b = t.newStreamWriteBuilder().withCommitUser(commitUser);
            try (StreamTableWrite w = b.newWrite()) {
                for (int j = 0; j < 100; j++) {
                    w.write(
                            GenericRow.of(
                                    1, BinaryString.fromString("batch1"), new BlobData(blobBytes)));
                }
                commitDefault(w.prepareCommit(false, Long.MAX_VALUE));
            }
        }

        catalog.alterTable(identifier(), SchemaChange.addColumn("f3", DataTypes.STRING()), false);

        {
            FileStoreTable t = getTableDefault();
            StreamWriteBuilder b = t.newStreamWriteBuilder().withCommitUser(commitUser);
            try (StreamTableWrite w = b.newWrite()) {
                for (int j = 0; j < 100; j++) {
                    w.write(
                            GenericRow.of(
                                    2,
                                    BinaryString.fromString("batch2"),
                                    new BlobData(blobBytes),
                                    BinaryString.fromString("after-add")));
                }
                commitDefault(w.prepareCommit(false, Long.MAX_VALUE));
            }
        }

        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.size()).isGreaterThan(0);
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        AtomicInteger batch1Count = new AtomicInteger(0);
        AtomicInteger batch2Count = new AtomicInteger(0);
        readDefault(
                row -> {
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    if (row.getInt(0) == 1) {
                        batch1Count.incrementAndGet();
                    } else if (row.getInt(0) == 2) {
                        batch2Count.incrementAndGet();
                    }
                });
        assertThat(batch1Count.get()).isEqualTo(100);
        assertThat(batch2Count.get()).isEqualTo(100);
    }

    @Test
    void testReadBlobAfterDropColumnAndCompaction() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "100 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        {
            FileStoreTable t = getTableDefault();
            StreamWriteBuilder b = t.newStreamWriteBuilder().withCommitUser(commitUser);
            try (StreamTableWrite w = b.newWrite()) {
                for (int j = 0; j < 100; j++) {
                    w.write(
                            GenericRow.of(
                                    1,
                                    BinaryString.fromString("batch1"),
                                    new BlobData(blobBytes),
                                    BinaryString.fromString("before-drop")));
                }
                commitDefault(w.prepareCommit(false, Long.MAX_VALUE));
            }
        }

        catalog.alterTable(identifier(), SchemaChange.dropColumn("f3"), false);

        {
            FileStoreTable t = getTableDefault();
            StreamWriteBuilder b = t.newStreamWriteBuilder().withCommitUser(commitUser);
            try (StreamTableWrite w = b.newWrite()) {
                for (int j = 0; j < 100; j++) {
                    w.write(
                            GenericRow.of(
                                    2, BinaryString.fromString("batch2"), new BlobData(blobBytes)));
                }
                commitDefault(w.prepareCommit(false, Long.MAX_VALUE));
            }
        }

        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.size()).isGreaterThan(0);
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        AtomicInteger batch1Count = new AtomicInteger(0);
        AtomicInteger batch2Count = new AtomicInteger(0);
        readDefault(
                row -> {
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    if (row.getInt(0) == 1) {
                        batch1Count.incrementAndGet();
                    } else if (row.getInt(0) == 2) {
                        batch2Count.incrementAndGet();
                    }
                });
        assertThat(batch1Count.get()).isEqualTo(100);
        assertThat(batch2Count.get()).isEqualTo(100);
    }

    @Disabled("Reproduce: rename blob column causes read failure after compaction")
    @Test
    void testRenameBlobColumnReadFailure() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "100 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        // Step 1: write blob data — blob files record writeCols=["f2"]
        commitDefault(writeDataDefault(100, 1));

        // Step 2: rename blob column f2 -> f2_renamed
        catalog.alterTable(identifier(), SchemaChange.renameColumn("f2", "f2_renamed"), false);

        // Step 3: write more data — new blob files have writeCols=["f2_renamed"]
        commitDefault(writeDataDefault(100, 1));

        // Step 4: compact merges files into the same split
        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.size()).isGreaterThan(0);
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        // Step 5: read fails —
        assertThatThrownBy(() -> readDefault(row -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("All files in this bunch should have the same write columns");
    }

    @Test
    void testRenameBlobColumnShouldFail() throws Exception {
        createTableDefault();
        commitDefault(writeDataDefault(10, 1));

        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.renameColumn("f2", "f2_renamed"),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot rename BLOB column");
    }

    @Test
    public void testBlobViewE2E() throws Exception {
        String upstreamTableName = "UpstreamBlob";
        Schema.Builder upstreamSchema = Schema.newBuilder();
        upstreamSchema.column("id", DataTypes.INT());
        upstreamSchema.column("name", DataTypes.STRING());
        upstreamSchema.column("image", DataTypes.BLOB());
        upstreamSchema.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        upstreamSchema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        upstreamSchema.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(upstreamTableName), upstreamSchema.build(), true);

        FileStoreTable upstreamTable = getTable(identifier(upstreamTableName));
        byte[] imageBytes1 = randomBytes();
        byte[] imageBytes2 = randomBytes();
        writeRows(
                upstreamTable,
                Arrays.asList(
                        GenericRow.of(
                                1, BinaryString.fromString("row1"), new BlobData(imageBytes1)),
                        GenericRow.of(
                                2, BinaryString.fromString("row2"), new BlobData(imageBytes2)),
                        GenericRow.of(3, BinaryString.fromString("row3"), null)));

        int imageFieldId =
                upstreamTable.rowType().getFields().stream()
                        .filter(f -> f.name().equals("image"))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("image field not found"))
                        .id();

        RowTrackingTable upstreamRowTracking = new RowTrackingTable(upstreamTable);
        ReadBuilder rowIdReader =
                upstreamRowTracking.newReadBuilder().withProjection(new int[] {0, 3});
        Map<Integer, Long> idToRowId = new HashMap<>();
        Map<Integer, byte[]> idToBlob = new HashMap<>();
        idToBlob.put(1, imageBytes1);
        idToBlob.put(2, imageBytes2);
        idToBlob.put(3, null);
        rowIdReader
                .newRead()
                .createReader(rowIdReader.newScan().plan())
                .forEachRemaining(
                        row -> {
                            int id = row.getInt(0);
                            idToRowId.put(id, row.getLong(1));
                        });
        assertThat(idToRowId.size()).isEqualTo(3);

        String downstreamTableName = "DownstreamView";
        Schema.Builder downstreamSchema = Schema.newBuilder();
        downstreamSchema.column("id", DataTypes.INT());
        downstreamSchema.column("label", DataTypes.STRING());
        downstreamSchema.column("image", DataTypes.BLOB());
        downstreamSchema.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        downstreamSchema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        downstreamSchema.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        downstreamSchema.option(CoreOptions.BLOB_FIELD.key(), "image");
        downstreamSchema.option(CoreOptions.BLOB_VIEW_FIELD.key(), "image");
        catalog.createTable(identifier(downstreamTableName), downstreamSchema.build(), true);

        FileStoreTable downstreamTable = getTable(identifier(downstreamTableName));
        String upstreamFullName = database + "." + upstreamTableName;
        writeRows(
                downstreamTable,
                Arrays.asList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("label1"),
                                Blob.fromView(
                                        new BlobViewStruct(
                                                Identifier.fromString(upstreamFullName),
                                                imageFieldId,
                                                idToRowId.get(1)))),
                        GenericRow.of(
                                2,
                                BinaryString.fromString("label2"),
                                Blob.fromView(
                                        new BlobViewStruct(
                                                Identifier.fromString(upstreamFullName),
                                                imageFieldId,
                                                idToRowId.get(2)))),
                        GenericRow.of(
                                3,
                                BinaryString.fromString("label3"),
                                Blob.fromView(
                                        new BlobViewStruct(
                                                Identifier.fromString(upstreamFullName),
                                                imageFieldId,
                                                idToRowId.get(3))))));

        ReadBuilder downstreamReadBuilder = downstreamTable.newReadBuilder();
        downstreamReadBuilder
                .newRead()
                .createReader(downstreamReadBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            int id = row.getInt(0);
                            if (idToBlob.get(id) == null) {
                                assertThat(row.isNullAt(2)).isTrue();
                                assertThat(row.getBlob(2)).isNull();
                                return;
                            }
                            Blob blob = row.getBlob(2);
                            assertThat(blob).isInstanceOf(BlobView.class);
                            assertThat(((BlobView) blob).isResolved()).isTrue();
                            assertThat(blob.toData()).isEqualTo(idToBlob.get(id));
                        });
    }

    @Test
    public void testForwardBlobViewReference() throws Exception {
        String upstreamTableName = "UpstreamBlobForward";
        Schema.Builder upstreamSchema = Schema.newBuilder();
        upstreamSchema.column("id", DataTypes.INT());
        upstreamSchema.column("image", DataTypes.BLOB());
        upstreamSchema.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        upstreamSchema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        upstreamSchema.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(upstreamTableName), upstreamSchema.build(), true);

        FileStoreTable upstreamTable = getTable(identifier(upstreamTableName));
        byte[] imageBytes1 = randomBytes();
        byte[] imageBytes2 = randomBytes();
        writeRows(
                upstreamTable,
                Arrays.asList(
                        GenericRow.of(1, new BlobData(imageBytes1)),
                        GenericRow.of(2, new BlobData(imageBytes2))));

        int imageFieldId = upstreamTable.rowType().getField("image").id();
        RowTrackingTable upstreamRowTracking = new RowTrackingTable(upstreamTable);
        ReadBuilder rowIdReader =
                upstreamRowTracking.newReadBuilder().withProjection(new int[] {0, 2});
        Map<Integer, Long> idToRowId = new HashMap<>();
        rowIdReader
                .newRead()
                .createReader(rowIdReader.newScan().plan())
                .forEachRemaining(row -> idToRowId.put(row.getInt(0), row.getLong(1)));
        assertThat(idToRowId.size()).isEqualTo(2);

        String firstViewTableName = "FirstBlobView";
        Schema.Builder firstViewSchema = Schema.newBuilder();
        firstViewSchema.column("id", DataTypes.INT());
        firstViewSchema.column("image", DataTypes.BLOB());
        firstViewSchema.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        firstViewSchema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        firstViewSchema.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        firstViewSchema.option(CoreOptions.BLOB_FIELD.key(), "image");
        firstViewSchema.option(CoreOptions.BLOB_VIEW_FIELD.key(), "image");
        catalog.createTable(identifier(firstViewTableName), firstViewSchema.build(), true);

        String upstreamFullName = database + "." + upstreamTableName;
        BlobViewStruct viewStruct1 =
                new BlobViewStruct(
                        Identifier.fromString(upstreamFullName), imageFieldId, idToRowId.get(1));
        BlobViewStruct viewStruct2 =
                new BlobViewStruct(
                        Identifier.fromString(upstreamFullName), imageFieldId, idToRowId.get(2));
        FileStoreTable firstViewTable = getTable(identifier(firstViewTableName));
        writeRows(
                firstViewTable,
                Arrays.asList(
                        GenericRow.of(1, Blob.fromView(viewStruct1)),
                        GenericRow.of(2, Blob.fromView(viewStruct2))));

        String secondViewTableName = "SecondBlobView";
        Schema.Builder secondViewSchema = Schema.newBuilder();
        secondViewSchema.column("id", DataTypes.INT());
        secondViewSchema.column("image", DataTypes.BLOB());
        secondViewSchema.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        secondViewSchema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        secondViewSchema.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        secondViewSchema.option(CoreOptions.BLOB_FIELD.key(), "image");
        secondViewSchema.option(CoreOptions.BLOB_VIEW_FIELD.key(), "image");
        catalog.createTable(identifier(secondViewTableName), secondViewSchema.build(), true);

        Map<String, String> preserveBlobViewOptions = new HashMap<>();
        preserveBlobViewOptions.put(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key(), "false");
        FileStoreTable firstViewWithoutResolve = firstViewTable.copy(preserveBlobViewOptions);
        ReadBuilder preserveReadBuilder = firstViewWithoutResolve.newReadBuilder();
        RecordReader<InternalRow> preserveReader =
                preserveReadBuilder.newRead().createReader(preserveReadBuilder.newScan().plan());
        List<InternalRow> rowsToForward = new ArrayList<>();
        InternalRowSerializer firstViewSerializer =
                new InternalRowSerializer(firstViewWithoutResolve.rowType());
        preserveReader.forEachRemaining(row -> rowsToForward.add(firstViewSerializer.copy(row)));
        preserveReader.close();

        rowsToForward.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        Blob preservedBlob1 = rowsToForward.get(0).getBlob(1);
        Blob preservedBlob2 = rowsToForward.get(1).getBlob(1);
        assertThat(preservedBlob1).isInstanceOf(BlobView.class);
        assertThat(preservedBlob2).isInstanceOf(BlobView.class);
        assertThat(((BlobView) preservedBlob1).isResolved()).isFalse();
        assertThat(((BlobView) preservedBlob2).isResolved()).isFalse();
        assertThat(((BlobView) preservedBlob1).viewStruct()).isEqualTo(viewStruct1);
        assertThat(((BlobView) preservedBlob2).viewStruct()).isEqualTo(viewStruct2);

        FileStoreTable secondViewTable = getTable(identifier(secondViewTableName));
        writeRows(secondViewTable, rowsToForward);

        FileStoreTable secondViewWithoutResolve = secondViewTable.copy(preserveBlobViewOptions);
        ReadBuilder verifyReferenceBuilder = secondViewWithoutResolve.newReadBuilder();
        RecordReader<InternalRow> verifyReferenceReader =
                verifyReferenceBuilder
                        .newRead()
                        .createReader(verifyReferenceBuilder.newScan().plan());
        List<InternalRow> secondViewRawRows = new ArrayList<>();
        InternalRowSerializer secondViewSerializer =
                new InternalRowSerializer(secondViewWithoutResolve.rowType());
        verifyReferenceReader.forEachRemaining(
                row -> secondViewRawRows.add(secondViewSerializer.copy(row)));
        verifyReferenceReader.close();

        secondViewRawRows.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        assertThat(((BlobView) secondViewRawRows.get(0).getBlob(1)).viewStruct())
                .isEqualTo(viewStruct1);
        assertThat(((BlobView) secondViewRawRows.get(1).getBlob(1)).viewStruct())
                .isEqualTo(viewStruct2);

        Map<Integer, byte[]> idToBlob = new HashMap<>();
        idToBlob.put(1, imageBytes1);
        idToBlob.put(2, imageBytes2);
        ReadBuilder secondViewReadBuilder = secondViewTable.newReadBuilder();
        secondViewReadBuilder
                .newRead()
                .createReader(secondViewReadBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            int id = row.getInt(0);
                            Blob blob = row.getBlob(1);
                            assertThat(blob).isInstanceOf(BlobView.class);
                            assertThat(((BlobView) blob).isResolved()).isTrue();
                            assertThat(blob.toData()).isEqualTo(idToBlob.get(id));
                        });
    }

    @Test
    public void testBlobProjectionExcludesBlobColumn() throws Exception {
        createTableDefault();
        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                42, BinaryString.fromString("hello"), new BlobData(blobBytes))));

        Table table = getTableDefault();
        // Project only f0 and f1 (indices 0, 1), excluding blob column f2
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1});
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getInt(0)).isEqualTo(42);
                    assertThat(row.getString(1).toString()).isEqualTo("hello");
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testBlobProjectionOnlyBlobColumn() throws Exception {
        createTableDefault();
        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                42, BinaryString.fromString("hello"), new BlobData(blobBytes))));

        Table table = getTableDefault();
        // Project only blob column f2 (index 2)
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {2});
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testNullBlobValues() throws Exception {
        createTableDefault();
        writeDataDefault(
                Arrays.asList(
                        GenericRow.of(0, BinaryString.fromString("a"), new BlobData(blobBytes)),
                        GenericRow.of(1, BinaryString.fromString("b"), null),
                        GenericRow.of(2, BinaryString.fromString("c"), new BlobData(blobBytes)),
                        GenericRow.of(3, BinaryString.fromString("d"), null),
                        GenericRow.of(4, BinaryString.fromString("e"), new BlobData(blobBytes))));

        Table table = getTableDefault();
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(serializer.copy(r)));
        assertThat(rows.size()).isEqualTo(5);

        rows.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        // Non-null blobs
        assertThat(rows.get(0).getBlob(2).toData()).isEqualTo(blobBytes);
        assertThat(rows.get(2).getBlob(2).toData()).isEqualTo(blobBytes);
        assertThat(rows.get(4).getBlob(2).toData()).isEqualTo(blobBytes);
        // Null blobs
        assertThat(rows.get(1).isNullAt(2)).isTrue();
        assertThat(rows.get(3).isNullAt(2)).isTrue();
    }

    @Test
    public void testBlobAsDescriptorReadMode() throws Exception {
        createTableDefault();
        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1, BinaryString.fromString("test"), new BlobData(blobBytes))));

        FileStoreTable table = getTableDefault();
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        Table tableWithDescriptorMode = table.copy(options);

        ReadBuilder readBuilder = tableWithDescriptorMode.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    Blob blob = row.getBlob(2);
                    // In descriptor mode, toDescriptor() should work
                    BlobDescriptor descriptor = blob.toDescriptor();
                    assertThat(descriptor).isNotNull();
                    assertThat(descriptor.uri()).isNotNull();
                    assertThat(descriptor.length()).isGreaterThan(0);
                    // The data should still be accessible via toData()
                    assertThat(blob.toData()).isEqualTo(blobBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testBlobCompactionSingleField() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "1 GB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        // Write multiple batches to create multiple blob files
        commitDefault(writeDataDefault(50, 20));

        FileStoreTable table = getTableDefault();
        List<DataFileMeta> before =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        long beforeBlobCount =
                before.stream()
                        .filter(
                                file ->
                                        org.apache.paimon.format.blob.BlobFileFormat.isBlobFile(
                                                file.fileName()))
                        .count();
        assertThat(beforeBlobCount).isGreaterThan(1);

        // Run blob compaction
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, true, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(DataEvolutionCompactTask::isBlobTask)).isTrue();

        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        // Read and verify data correctness
        AtomicInteger readCount = new AtomicInteger(0);
        readDefault(
                row -> {
                    readCount.incrementAndGet();
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                });
        assertThat(readCount.get()).isEqualTo(1000);

        // Verify no more blob compaction tasks needed
        table = getTableDefault();
        coordinator = new DataEvolutionCompactCoordinator(table, true, false);
        List<DataEvolutionCompactTask> tasks2;
        try {
            tasks2 = coordinator.plan();
        } catch (EndOfScanException e) {
            tasks2 = Collections.emptyList();
        }
        assertThat(tasks2.stream().anyMatch(DataEvolutionCompactTask::isBlobTask)).isFalse();
    }

    @Test
    public void testPartitionedTableWithBlob() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pt", DataTypes.STRING());
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.BLOB());
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        FileStoreTable table = getTableDefault();
        byte[] blobP1 = randomBytes();
        byte[] blobP2 = randomBytes();

        writeRows(
                table,
                Arrays.asList(
                        GenericRow.of(BinaryString.fromString("p1"), 1, new BlobData(blobP1)),
                        GenericRow.of(BinaryString.fromString("p1"), 2, new BlobData(blobP1))));
        writeRows(
                table,
                Collections.singletonList(
                        GenericRow.of(BinaryString.fromString("p2"), 3, new BlobData(blobP2))));

        // Read all partitions
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(serializer.copy(r)));
        assertThat(rows.size()).isEqualTo(3);

        rows.sort((a, b) -> Integer.compare(a.getInt(1), b.getInt(1)));
        // p1 rows
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("p1");
        assertThat(rows.get(0).getBlob(2).toData()).isEqualTo(blobP1);
        assertThat(rows.get(1).getString(0).toString()).isEqualTo("p1");
        assertThat(rows.get(1).getBlob(2).toData()).isEqualTo(blobP1);
        // p2 row
        assertThat(rows.get(2).getString(0).toString()).isEqualTo("p2");
        assertThat(rows.get(2).getBlob(2).toData()).isEqualTo(blobP2);
    }

    @Test
    public void testBlobWithRowRangeFilterMultipleFiles() throws Exception {
        createTableDefault();

        // Write enough rows to create multiple blob file rolls (100 rows per batch * 10 batches)
        commitDefault(writeDataDefault(100, 10));

        Table table = getTableDefault();
        // Read only rows in range [50, 60] — a narrow subset
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withRowRanges(Collections.singletonList(new Range(50L, 60L)));
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                });
        assertThat(count.get()).isEqualTo(11);
    }

    @Test
    public void testBlobConsumerFlushBehavior() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        List<BlobDescriptor> descriptors = new ArrayList<>();
        AtomicInteger flushCount = new AtomicInteger(0);

        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.withBlobConsumer(
                    (blobFieldName, blobDescriptor) -> {
                        descriptors.add(blobDescriptor);
                        // Return true (flush) for every other blob
                        boolean shouldFlush = descriptors.size() % 2 == 0;
                        if (shouldFlush) {
                            flushCount.incrementAndGet();
                        }
                        return shouldFlush;
                    });
            for (int i = 0; i < 5; i++) {
                write.write(
                        GenericRow.of(
                                i, BinaryString.fromString("row" + i), new BlobData(blobBytes)));
            }
            BatchTableCommit commit = table.newBatchWriteBuilder().newCommit();
            commit.commit(write.prepareCommit());
            commit.close();
        }

        // All 5 descriptors collected
        assertThat(descriptors.size()).isEqualTo(5);
        assertThat(flushCount.get()).isEqualTo(2);

        // All descriptors should be valid and point to blob data
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        for (BlobDescriptor desc : descriptors) {
            assertThat(desc).isNotNull();
            assertThat(Blob.fromDescriptor(uriReader, desc).toData()).isEqualTo(blobBytes);
        }
    }

    @Test
    public void testMultipleBlobFieldsProjection() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-proj-test.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("proj"),
                                new BlobData(blobBytes),
                                blobRef)));

        // Project f0 and f2 only (indices 0, 2), skipping f1 and f3
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 2});
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getInt(0)).isEqualTo(1);
                    assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleBlobFieldsProjectOnlyDescriptorBlob() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-proj-desc.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("test"),
                                new BlobData(blobBytes),
                                blobRef)));

        // Project only f3 (index 3) — the descriptor blob, skipping raw blob f2
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(0).toDescriptor()).isEqualTo(descriptor);
                    assertThat(row.getBlob(0).toData()).isEqualTo(descriptorBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleBlobFieldsProjectBothBlobs() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-proj-both.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("test"),
                                new BlobData(blobBytes),
                                blobRef)));

        // Project only f2 and f3 (indices 2, 3) — both blob columns, no non-blob columns
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {2, 3});
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes);
                    assertThat(row.getBlob(1).toDescriptor()).isEqualTo(descriptor);
                    assertThat(row.getBlob(1).toData()).isEqualTo(descriptorBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleBlobFieldsProjectStringAndDescriptorBlob() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-proj-str-desc.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("test"),
                                new BlobData(blobBytes),
                                blobRef)));

        // Project f1 and f3 (indices 1, 3) — string + descriptor blob, skip raw blob
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {1, 3});
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getString(0).toString()).isEqualTo("test");
                    assertThat(row.getBlob(1).toDescriptor()).isEqualTo(descriptor);
                    assertThat(row.getBlob(1).toData()).isEqualTo(descriptorBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleBlobFieldsPartialNull() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-partial-null.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Arrays.asList(
                        // f2=blob, f3=null
                        GenericRow.of(
                                1, BinaryString.fromString("a"), new BlobData(blobBytes), null),
                        // f2=null, f3=descriptor
                        GenericRow.of(2, BinaryString.fromString("b"), null, blobRef),
                        // both non-null
                        GenericRow.of(
                                3, BinaryString.fromString("c"), new BlobData(blobBytes), blobRef),
                        // both null
                        GenericRow.of(4, BinaryString.fromString("d"), null, null)));

        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(serializer.copy(r)));
        assertThat(rows.size()).isEqualTo(4);
        rows.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));

        // Row 1: f2=blob, f3=null
        assertThat(rows.get(0).getBlob(2).toData()).isEqualTo(blobBytes);
        assertThat(rows.get(0).isNullAt(3)).isTrue();

        // Row 2: f2=null, f3=descriptor
        assertThat(rows.get(1).isNullAt(2)).isTrue();
        assertThat(rows.get(1).getBlob(3).toDescriptor()).isEqualTo(descriptor);

        // Row 3: both non-null
        assertThat(rows.get(2).getBlob(2).toData()).isEqualTo(blobBytes);
        assertThat(rows.get(2).getBlob(3).toDescriptor()).isEqualTo(descriptor);

        // Row 4: both null
        assertThat(rows.get(3).isNullAt(2)).isTrue();
        assertThat(rows.get(3).isNullAt(3)).isTrue();
    }

    @Test
    public void testMultipleBlobFieldsWithRowRangeFilter() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-range-multi.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        List<InternalRow> inputRows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            inputRows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("row" + i),
                            new BlobData(blobBytes),
                            blobRef));
        }
        writeDataDefault(inputRows);

        // Read only rows [3, 5]
        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(3L, 5L)));
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(serializer.copy(r)));
        assertThat(rows.size()).isEqualTo(3);

        rows.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        for (int i = 0; i < 3; i++) {
            assertThat(rows.get(i).getInt(0)).isEqualTo(i + 3);
            assertThat(rows.get(i).getBlob(2).toData()).isEqualTo(blobBytes);
            assertThat(rows.get(i).getBlob(3).toDescriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    public void testMultipleRawBlobFieldsAllProjections() throws Exception {
        // Create a table with two raw blob fields (both written to separate .blob files)
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.BLOB());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        FileStoreTable table = getTableDefault();
        byte[] blob1 = randomBytes();
        byte[] blob2 = randomBytes();

        writeRows(
                table,
                Arrays.asList(
                        GenericRow.of(1, new BlobData(blob1), new BlobData(blob2)),
                        GenericRow.of(2, new BlobData(blob1), new BlobData(blob2)),
                        GenericRow.of(3, new BlobData(blob1), new BlobData(blob2))));

        // Projection: f0 only
        ReadBuilder rb = table.newReadBuilder().withProjection(new int[] {0});
        RecordReader<InternalRow> reader = rb.newRead().createReader(rb.newScan().plan());
        InternalRowSerializer ser0 =
                new InternalRowSerializer(table.rowType().project(new int[] {0}));
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(ser0.copy(r)));
        assertThat(rows.size()).isEqualTo(3);
        rows.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        for (int i = 0; i < 3; i++) {
            assertThat(rows.get(i).getInt(0)).isEqualTo(i + 1);
        }

        // Projection: f1 only (first blob)
        rb = table.newReadBuilder().withProjection(new int[] {1});
        reader = rb.newRead().createReader(rb.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(0).toData()).isEqualTo(blob1);
                });
        assertThat(count.get()).isEqualTo(3);

        // Projection: f2 only (second blob)
        rb = table.newReadBuilder().withProjection(new int[] {2});
        reader = rb.newRead().createReader(rb.newScan().plan());
        count.set(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(0).toData()).isEqualTo(blob2);
                });
        assertThat(count.get()).isEqualTo(3);

        // Projection: f0 + f2 (skip f1 blob)
        rb = table.newReadBuilder().withProjection(new int[] {0, 2});
        reader = rb.newRead().createReader(rb.newScan().plan());
        InternalRowSerializer ser02 =
                new InternalRowSerializer(table.rowType().project(new int[] {0, 2}));
        List<InternalRow> rows2 = new ArrayList<>();
        reader.forEachRemaining(r -> rows2.add(ser02.copy(r)));
        assertThat(rows2.size()).isEqualTo(3);
        rows2.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        for (int i = 0; i < 3; i++) {
            assertThat(rows2.get(i).getInt(0)).isEqualTo(i + 1);
            assertThat(rows2.get(i).getBlob(1).toData()).isEqualTo(blob2);
        }
    }

    @Test
    public void testMultipleBlobFieldsAsDescriptorReadMode() throws Exception {
        createMixedModeTable();
        FileStoreTable table = getTableDefault();

        byte[] descriptorBytes = randomBytes();
        Path external = new Path(tempPath.resolve("upstream-as-desc-multi.bin").toString());
        writeFile(table.fileIO(), external, descriptorBytes);

        BlobDescriptor descriptor =
                new BlobDescriptor(external.toString(), 0, descriptorBytes.length);
        UriReader uriReader = UriReader.fromFile(table.fileIO());
        Blob blobRef = Blob.fromDescriptor(uriReader, descriptor);

        writeDataDefault(
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("desc-mode"),
                                new BlobData(blobBytes),
                                blobRef)));

        // Read with BLOB_AS_DESCRIPTOR=true
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        Table tableDescMode = table.copy(options);
        ReadBuilder readBuilder = tableDescMode.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    count.incrementAndGet();
                    // f2 (raw blob) — should now return a descriptor (lazy reference)
                    Blob blob2 = row.getBlob(2);
                    BlobDescriptor desc2 = blob2.toDescriptor();
                    assertThat(desc2).isNotNull();
                    assertThat(desc2.uri()).contains(".blob");
                    assertThat(blob2.toData()).isEqualTo(blobBytes);

                    // f3 (descriptor blob) — already stored as descriptor, should work
                    Blob blob3 = row.getBlob(3);
                    assertThat(blob3.toDescriptor()).isEqualTo(descriptor);
                    assertThat(blob3.toData()).isEqualTo(descriptorBytes);
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testBlobViewResolutionFailsOnMissingUpstream() throws Exception {
        // Create a downstream table with blob-view-field
        String downstreamName = "DownstreamViewFail";
        Schema.Builder downstreamSchema = Schema.newBuilder();
        downstreamSchema.column("id", DataTypes.INT());
        downstreamSchema.column("image", DataTypes.BLOB());
        downstreamSchema.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        downstreamSchema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        downstreamSchema.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        downstreamSchema.option(CoreOptions.BLOB_FIELD.key(), "image");
        downstreamSchema.option(CoreOptions.BLOB_VIEW_FIELD.key(), "image");
        catalog.createTable(identifier(downstreamName), downstreamSchema.build(), true);

        FileStoreTable downstreamTable = getTable(identifier(downstreamName));

        // Write a BlobView reference pointing to a non-existent upstream table
        BlobViewStruct viewStruct =
                new BlobViewStruct(Identifier.fromString("default.NonExistent"), 2, 0L);
        writeRows(
                downstreamTable,
                Collections.singletonList(GenericRow.of(1, Blob.fromView(viewStruct))));

        // Reading should fail because the upstream table doesn't exist
        ReadBuilder readBuilder = downstreamTable.newReadBuilder();
        assertThatThrownBy(() -> readBuilder.newRead().createReader(readBuilder.newScan().plan()))
                .isInstanceOf(RuntimeException.class);
    }

    private void createExternalStorageTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.BLOB_FIELD.key(), "f2");
        schemaBuilder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "f2");
        schemaBuilder.option(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "f2");
        schemaBuilder.option(
                CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(),
                tempPath.resolve("external-storage-blob-path").toString());
        catalog.createTable(identifier(), schemaBuilder.build(), true);
    }

    private void writeRows(Table table, Iterable<InternalRow> rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit();
        for (InternalRow row : rows) {
            write.write(row);
        }
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();
    }

    private static void assignFirstRowId(List<CommitMessage> commitMessages, long firstRowId) {
        commitMessages.forEach(
                commitMessage -> {
                    CommitMessageImpl impl = (CommitMessageImpl) commitMessage;
                    List<DataFileMeta> newFiles =
                            new ArrayList<>(impl.newFilesIncrement().newFiles());
                    impl.newFilesIncrement().newFiles().clear();
                    impl.newFilesIncrement()
                            .newFiles()
                            .addAll(
                                    newFiles.stream()
                                            .map(file -> file.assignFirstRowId(firstRowId))
                                            .collect(Collectors.toList()));
                });
    }

    private void createThreeTypeBlobTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB()); // raw-data blob
        schemaBuilder.column("f3", DataTypes.BLOB()); // descriptor blob
        schemaBuilder.column("f4", DataTypes.BLOB()); // descriptor blob with external storage
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.BLOB_FIELD.key(), "f2,f3,f4");
        schemaBuilder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "f3,f4");
        schemaBuilder.option(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "f4");
        schemaBuilder.option(
                CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(),
                tempPath.resolve("external-storage-blob-path-3type").toString());
        catalog.createTable(identifier(), schemaBuilder.build(), true);
    }

    private void createDescriptorTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.BLOB_FIELD.key(), "f2");
        schemaBuilder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "f2");
        catalog.createTable(identifier(), schemaBuilder.build(), true);
    }

    private void createMixedModeTable() throws Exception {
        createMixedModeTable(FILE_FORMAT_PARQUET);
    }

    private void createMixedModeTable(String format) throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.BLOB_FIELD.key(), "f2,f3");
        schemaBuilder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "f3");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), format);
        catalog.createTable(identifier(), schemaBuilder.build(), true);
    }

    private static void writeFile(FileIO fileIO, Path path, byte[] bytes) throws IOException {
        try (org.apache.paimon.fs.PositionOutputStream out = fileIO.newOutputStream(path, true)) {
            out.write(bytes);
        }
    }

    private static long countFilesWithSuffix(FileIO fileIO, Path root, String suffix)
            throws IOException {
        long count = 0;
        org.apache.paimon.fs.RemoteIterator<org.apache.paimon.fs.FileStatus> it =
                fileIO.listFilesIterative(root, true);
        while (it.hasNext()) {
            org.apache.paimon.fs.FileStatus status = it.next();
            if (status.getPath().getName().endsWith(suffix)) {
                count++;
            }
        }
        return count;
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(), BinaryString.fromBytes(randomBytes()), new BlobData(blobBytes));
    }

    private static RowType makeBlobRowType(
            List<String> fieldNames, Function<String, Integer> fieldIdFunc) {
        List<DataField> fields = new ArrayList<>();
        if (fieldNames == null) {
            fieldNames = Collections.emptyList();
        }
        for (String fieldName : fieldNames) {
            int fieldId = fieldIdFunc.apply(fieldName);
            DataField blobField = new DataField(fieldId, fieldName, DataTypes.BLOB());
            fields.add(blobField);
        }
        return new RowType(fields);
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[2 * 1024 * 124];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
