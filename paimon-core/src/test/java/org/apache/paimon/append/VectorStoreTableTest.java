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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with vector-store and data evolution. */
public class VectorStoreTableTest extends TableTestBase {

    private static final int VECTOR_DIM = 10;

    private AtomicInteger uniqueIdGen = new AtomicInteger(0);

    private Map<Integer, InternalRow> rowsWritten = new HashMap<>();

    @Test
    public void testBasic() throws Exception {
        int rowNum = RANDOM.nextInt(64) + 1;

        createTableDefault();

        commitDefault(writeDataDefault(rowNum, 1));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                DataEvolutionSplitRead.splitFieldBunches(
                        filesMetas, key -> makeBlobRowType(key.writeCols(), f -> 0));

        assertThat(fieldGroups.size()).isEqualTo(3);
        assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(1).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(2).files().size()).isEqualTo(1);

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getBlob(2)).isEqualTo(expected.getBlob(2));
                    assertThat(row.getVector(3).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(4)).isEqualTo(expected.getInt(4));
                });

        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Test
    public void testMultiBatch() throws Exception {
        int rowNum = (RANDOM.nextInt(64) + 1) * 2;

        createTableDefault();

        commitDefault(writeDataDefault(rowNum / 2, 2));

        AtomicInteger counter = new AtomicInteger(0);

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
            assertThat(fieldGroups.size()).isEqualTo(3);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(2).files().size()).isEqualTo(1);
        }

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getBlob(2)).isEqualTo(expected.getBlob(2));
                    assertThat(row.getVector(3).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(4)).isEqualTo(expected.getInt(4));
                });
        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Test
    public void testRolling() throws Exception {
        // 100k vector-store data would create 1 normal, 1 blob, and 3 vector-store files
        int rowNum = 100 * 1000 * 3;

        createTableDefault();

        commitDefault(writeDataDefault(rowNum / 3, 3));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(3);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(
                            batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
            assertThat(fieldGroups.size()).isEqualTo(3);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(2).files().size()).isEqualTo(3);
        }

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getBlob(2)).isEqualTo(expected.getBlob(2));
                    assertThat(row.getVector(3).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(4)).isEqualTo(expected.getInt(4));
                });

        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Test
    public void testWithoutBlob() throws Exception {
        // 100k vector-store data would create 1 normal, 1 blob, and 3 vector-store files
        int rowNum = 100 * 1000 * 3;

        catalog.createTable(identifier(), schemaWithoutBlob(), true);

        commitDefault(writeDataWithoutBlob(rowNum / 3, 3));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(3);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(
                            batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
            assertThat(fieldGroups.size()).isEqualTo(2);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(3);
        }

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getVector(2).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(3)).isEqualTo(expected.getInt(4));
                });

        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()));
        schemaBuilder.column("f4", DataTypes.INT());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "2 MB");
        schemaBuilder.option(CoreOptions.VECTOR_STORE_TARGET_FILE_SIZE.key(), "4 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.VECTOR_STORE_FIELDS.key(), "f3,f4");
        schemaBuilder.option(CoreOptions.VECTOR_STORE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        return schemaBuilder.build();
    }

    private Schema schemaWithoutBlob() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()));
        schemaBuilder.column("f3", DataTypes.INT());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "2 MB");
        schemaBuilder.option(CoreOptions.VECTOR_STORE_TARGET_FILE_SIZE.key(), "4 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.VECTOR_STORE_FIELDS.key(), "f2,f3");
        schemaBuilder.option(CoreOptions.VECTOR_STORE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        return schemaBuilder.build();
    }

    protected List<CommitMessage> writeDataWithoutBlob(int size, int times) throws Exception {
        Table table = getTableDefault();
        List<CommitMessage> messages = new ArrayList<>();
        for (int time = 0; time < times; time++) {
            StreamWriteBuilder builder = table.newStreamWriteBuilder();
            builder.withCommitUser(commitUser);
            try (StreamTableWrite streamTableWrite = builder.newWrite()) {
                for (int j = 0; j < size; j++) {
                    InternalRow row = dataDefault(time, j);
                    streamTableWrite.write(
                            GenericRow.of(
                                    row.getInt(0),
                                    row.getString(1),
                                    row.getVector(3),
                                    row.getInt(4)));
                }
                messages.addAll(streamTableWrite.prepareCommit(false, Long.MAX_VALUE));
            }
        }
        return messages;
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        byte[] stringBytes = new byte[1];
        RANDOM.nextBytes(stringBytes);
        byte[] blobBytes = new byte[1];
        RANDOM.nextBytes(blobBytes);
        byte[] vectorBytes = new byte[VECTOR_DIM];
        RANDOM.nextBytes(vectorBytes);
        float[] vector = new float[VECTOR_DIM];
        for (int i = 0; i < VECTOR_DIM; i++) {
            vector[i] = vectorBytes[i];
        }
        int id = uniqueIdGen.getAndIncrement();
        InternalRow row =
                GenericRow.of(
                        id,
                        BinaryString.fromBytes(stringBytes),
                        new BlobData(blobBytes),
                        BinaryVector.fromPrimitiveArray(vector),
                        RANDOM.nextInt(32) + 1);
        rowsWritten.put(id, row);
        return row;
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
}
