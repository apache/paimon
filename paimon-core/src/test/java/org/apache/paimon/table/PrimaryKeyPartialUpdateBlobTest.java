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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.RowTrackingTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for primary-key partial-update tables with managed and view BLOB fields. */
public class PrimaryKeyPartialUpdateBlobTest extends TableTestBase {

    @Test
    public void testPartialUpdateManagedBlobMergeAndCompact() throws Exception {
        String tableName = "pk_pu_managed_blob";
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("payload", DataTypes.BLOB())
                        .primaryKey("id")
                        .option(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                        .option(CoreOptions.BLOB_FIELD.key(), "payload")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "none")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(tableName), schema, true);
        FileStoreTable table = getTable(identifier(tableName));

        byte[] first = new byte[] {1, 2, 3};
        byte[] second = new byte[] {4, 5, 6};

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, BinaryString.fromString("a"), new BlobData(first)));
            commit.commit(0, write.prepareCommit(false, 0));

            write.write(GenericRow.of(1, BinaryString.fromString("b"), null));
            commit.commit(1, write.prepareCommit(false, 1));

            write.write(GenericRow.of(1, null, new BlobData(second)));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        List<InternalRow> rows = readRows(readBuilder);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("b");
        assertThat(rows.get(0).getBlob(2).toData()).isEqualTo(second);

        List<DataFileMeta> dataFiles = listDataFiles(table);
        assertThat(dataFiles).isNotEmpty();
        assertThat(dataFiles.get(0).extraFiles())
                .anyMatch(extraFile -> extraFile.endsWith(".blobref"));

        compact(table, BinaryRow.EMPTY_ROW, 0);
        rows = readRows(table.newReadBuilder());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getBlob(2).toData()).isEqualTo(second);
        assertThat(listDataFiles(table)).hasSize(1);
    }

    @Test
    public void testPartialUpdateManagedBlobLastValueRetract() throws Exception {
        String tableName = "pk_pu_managed_blob_retract";
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BLOB())
                        .column("ts", DataTypes.INT())
                        .primaryKey("id")
                        .option(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                        .option(CoreOptions.BLOB_FIELD.key(), "payload")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "none")
                        .option("fields.ts.sequence-group", "payload")
                        .option("fields.payload.aggregate-function", "last_value")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(tableName), schema, true);
        FileStoreTable table = getTable(identifier(tableName));

        byte[] payload = new byte[] {1, 2, 3};
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, new BlobData(payload), 1));
            write.write(GenericRow.of(2, new BlobData(payload), 2));
            commit.commit(0, write.prepareCommit(false, 0));

            write.write(GenericRow.ofKind(RowKind.DELETE, 1, new BlobData(payload), 2));
            write.write(GenericRow.ofKind(RowKind.DELETE, 2, new BlobData(payload), 1));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        List<InternalRow> rows = readRows(table.newReadBuilder());
        assertLastValueRetractedRows(rows);

        compact(table, BinaryRow.EMPTY_ROW, 0);
        rows = readRows(table.newReadBuilder());
        assertLastValueRetractedRows(rows);
        assertThat(managedBlobReferences(table)).isEmpty();
    }

    @Test
    public void testPartialUpdateManagedBlobCollectionsIgnoreRetract() throws Exception {
        String tableName = "pk_pu_managed_blob_collections_ignore_retract";
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BLOB())
                        .column("payloads", DataTypes.ARRAY(DataTypes.BLOB()))
                        .column("assets", DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()))
                        .column("ts", DataTypes.INT())
                        .primaryKey("id")
                        .option(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                        .option(CoreOptions.BLOB_FIELD.key(), "payload,payloads,assets")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "none")
                        .option("fields.ts.sequence-group", "payload,payloads,assets")
                        .option("fields.default-aggregate-function", "last_non_null_value")
                        .option("fields.payload.ignore-retract", "true")
                        .option("fields.payloads.ignore-retract", "true")
                        .option("fields.assets.ignore-retract", "true")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(tableName), schema, true);
        FileStoreTable table = getTable(identifier(tableName));

        byte[] payload = new byte[] {1, 2, 3};
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.of(
                            1,
                            new BlobData(payload),
                            new GenericArray(new Object[] {new BlobData(payload)}),
                            blobMap("payload", payload),
                            1));
            commit.commit(0, write.prepareCommit(false, 0));

            write.write(
                    GenericRow.ofKind(
                            RowKind.DELETE,
                            1,
                            new BlobData(payload),
                            new GenericArray(new Object[] {new BlobData(payload)}),
                            blobMap("payload", payload),
                            2));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        assertManagedIgnoreRetractRow(table, payload);
        compact(table, BinaryRow.EMPTY_ROW, 0);
        assertManagedIgnoreRetractRow(table, payload);
    }

    @Test
    public void testPartialUpdateManagedBlobCollections() throws Exception {
        String tableName = "pk_pu_managed_blob_collections";
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("payloads", DataTypes.ARRAY(DataTypes.BLOB()))
                        .column("assets", DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()))
                        .primaryKey("id")
                        .option(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                        .option(CoreOptions.BLOB_FIELD.key(), "payloads,assets")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "none")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(tableName), schema, true);
        FileStoreTable table = getTable(identifier(tableName));

        byte[] first = new byte[] {1, 2, 3};
        byte[] second = new byte[] {4, 5, 6};
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryString.fromString("a"),
                            new GenericArray(new Object[] {new BlobData(first), null}),
                            blobMap("first", first)));
            commit.commit(0, write.prepareCommit(false, 0));

            write.write(GenericRow.of(1, BinaryString.fromString("b"), null, null));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        assertManagedCollectionRow(table, "b", first, "first", first, 2);

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.of(
                            1,
                            null,
                            new GenericArray(new Object[] {new BlobData(second)}),
                            blobMap("second", second)));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        assertManagedCollectionRow(table, "b", second, "second", second, 1);
        compact(table, BinaryRow.EMPTY_ROW, 0);
        assertManagedCollectionRow(table, "b", second, "second", second, 1);
    }

    @Test
    public void testPartialUpdateBlobViewResolvesOnRead() throws Exception {
        String upstreamName = "pk_pu_upstream_blob";
        Schema upstreamSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("image", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.BLOB_FIELD.key(), "image")
                        .build();
        catalog.createTable(identifier(upstreamName), upstreamSchema, true);
        FileStoreTable upstreamTable = getTable(identifier(upstreamName));

        byte[] imageBytes = new byte[] {72, 101, 108, 108, 111};
        byte[] secondImageBytes = new byte[] {87, 111, 114, 108, 100};
        write(
                upstreamTable,
                GenericRow.of(1, new BlobData(imageBytes)),
                GenericRow.of(2, new BlobData(secondImageBytes)));

        int imageFieldId = upstreamTable.rowType().getField("image").id();
        RowTrackingTable upstreamRowTracking = new RowTrackingTable(upstreamTable);
        ReadBuilder rowIdReader =
                upstreamRowTracking.newReadBuilder().withProjection(new int[] {0, 2});
        Map<Integer, Long> idToRowId = new HashMap<>();
        rowIdReader
                .newRead()
                .createReader(rowIdReader.newScan().plan())
                .forEachRemaining(row -> idToRowId.put(row.getInt(0), row.getLong(1)));
        assertThat(idToRowId).containsKeys(1, 2);

        String downstreamName = "pk_pu_blob_view";
        Schema downstreamSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("label", DataTypes.STRING())
                        .column("image_ref", DataTypes.BLOB())
                        .primaryKey("id")
                        .option(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                        .option(CoreOptions.BLOB_VIEW_FIELD.key(), "image_ref")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(downstreamName), downstreamSchema, true);
        FileStoreTable downstreamTable = getTable(identifier(downstreamName));

        String upstreamFullName = database + "." + upstreamName;
        write(
                downstreamTable,
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
                                        idToRowId.get(2)))));

        try (StreamTableWrite write = downstreamTable.newWrite(commitUser);
                StreamTableCommit commit = downstreamTable.newCommit(commitUser)) {
            write.write(GenericRow.of(1, BinaryString.fromString("updated"), null));
            commit.commit(0, write.prepareCommit(false, 0));
        }

        ReadBuilder readBuilder = downstreamTable.newReadBuilder();
        List<InternalRow> rows = readRows(readBuilder);
        assertThat(rows).hasSize(2);
        InternalRow updatedRow =
                rows.stream()
                        .filter(row -> row.getInt(0) == 1)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertThat(updatedRow.getString(1).toString()).isEqualTo("updated");
        Blob blob = updatedRow.getBlob(2);
        assertThat(blob).isInstanceOf(BlobView.class);
        assertThat(((BlobView) blob).isResolved()).isTrue();
        assertThat(blob.toData()).isEqualTo(imageBytes);

        PredicateBuilder predicateBuilder = new PredicateBuilder(downstreamTable.rowType());
        ReadBuilder filteredReadBuilder =
                downstreamTable.newReadBuilder().withFilter(predicateBuilder.equal(0, 2));
        InnerTableRead filteredRead = (InnerTableRead) filteredReadBuilder.newRead();
        filteredRead.withLimit(1);
        filteredRead.executeFilter();
        rows = read(filteredRead.createReader(filteredReadBuilder.newScan().plan()));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(2);
        assertThat(rows.get(0).getBlob(2).toData()).isEqualTo(secondImageBytes);
    }

    private List<InternalRow> readRows(ReadBuilder readBuilder) throws Exception {
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        return read(reader);
    }

    private void assertManagedCollectionRow(
            FileStoreTable table,
            String expectedName,
            byte[] expectedArrayValue,
            String expectedMapKey,
            byte[] expectedMapValue,
            int expectedArraySize)
            throws Exception {
        List<InternalRow> rows = readRows(table.newReadBuilder());
        assertThat(rows).hasSize(1);
        InternalRow row = rows.get(0);
        assertThat(row.getString(1).toString()).isEqualTo(expectedName);

        InternalArray payloads = row.getArray(2);
        assertThat(payloads.size()).isEqualTo(expectedArraySize);
        assertThat(payloads.getBlob(0).toData()).isEqualTo(expectedArrayValue);
        if (expectedArraySize > 1) {
            assertThat(payloads.isNullAt(1)).isTrue();
        }

        InternalMap assets = row.getMap(3);
        assertThat(assets.size()).isEqualTo(1);
        assertThat(assets.keyArray().getString(0).toString()).isEqualTo(expectedMapKey);
        assertThat(assets.valueArray().getBlob(0).toData()).isEqualTo(expectedMapValue);
    }

    private GenericMap blobMap(String key, byte[] value) {
        Map<BinaryString, Blob> blobs = new LinkedHashMap<>();
        blobs.put(BinaryString.fromString(key), new BlobData(value));
        return new GenericMap(blobs);
    }

    private void assertManagedIgnoreRetractRow(FileStoreTable table, byte[] expected)
            throws Exception {
        List<InternalRow> rows = readRows(table.newReadBuilder());
        assertThat(rows).hasSize(1);
        InternalRow row = rows.get(0);
        assertThat(row.getBlob(1).toData()).isEqualTo(expected);
        assertThat(row.getArray(2).getBlob(0).toData()).isEqualTo(expected);
        assertThat(row.getMap(3).valueArray().getBlob(0).toData()).isEqualTo(expected);
        assertThat(row.getInt(4)).isEqualTo(2);
    }

    private void assertLastValueRetractedRows(List<InternalRow> rows) {
        assertThat(rows).hasSize(2);
        assertThat(rows).allMatch(row -> row.isNullAt(1));
        assertThat(rows).allMatch(row -> row.getInt(2) == 2);
    }

    private List<ManagedBlobReferenceFile.Reference> managedBlobReferences(FileStoreTable table)
            throws Exception {
        List<DataFileMeta> dataFiles = listDataFiles(table);
        assertThat(dataFiles).hasSize(1);
        DataFileMeta dataFile = dataFiles.get(0);
        String referenceFile =
                dataFile.extraFiles().stream()
                        .filter(
                                file ->
                                        file.endsWith(
                                                ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing managed BLOB sidecar."));
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        return ManagedBlobReferenceFile.read(
                table.fileIO(), pathFactory.toAlignedPath(referenceFile, dataFile));
    }

    private List<DataFileMeta> listDataFiles(FileStoreTable table) {
        return table.newSnapshotReader().read().dataSplits().stream()
                .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                .collect(Collectors.toList());
    }

    private List<InternalRow> read(RecordReader<InternalRow> reader) throws Exception {
        List<InternalRow> rows = new java.util.ArrayList<>();
        reader.forEachRemaining(rows::add);
        reader.close();
        return rows;
    }
}
