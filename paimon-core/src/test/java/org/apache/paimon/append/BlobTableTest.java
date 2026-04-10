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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
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
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.RowTrackingTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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

    private void createExternalStorageTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "f2");
        schemaBuilder.option(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "f2");
        schemaBuilder.option(
                CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(),
                tempPath.resolve("external-storage-blob-path").toString());
        catalog.createTable(identifier(), schemaBuilder.build(), true);
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
