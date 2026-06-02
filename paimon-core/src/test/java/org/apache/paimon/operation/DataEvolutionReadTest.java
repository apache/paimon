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

package org.apache.paimon.operation;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.DataEvolutionSplitRead.BlobFileBunch;
import org.apache.paimon.operation.DataEvolutionSplitRead.FieldBunch;
import org.apache.paimon.operation.DataEvolutionSplitRead.VectorFileBunch;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.operation.DataEvolutionSplitRead.splitFieldBunches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for blob and vector field bunches. */
public class DataEvolutionReadTest {

    private VectorFileBunch vectorBunch;

    @BeforeEach
    public void setUp() {
        vectorBunch = new VectorFileBunch(Long.MAX_VALUE, false);
    }

    @Test
    public void testAddSingleVectorEntry() {
        DataFileMeta vectorEntry = createVectorFile("vector1", 0L, 100L, 1L);

        vectorBunch.add(vectorEntry);

        assertThat(vectorBunch.files).hasSize(1);
        assertThat(vectorBunch.files.get(0)).isEqualTo(vectorEntry);
        assertThat(vectorBunch.rowCount()).isEqualTo(100);
        assertThat(vectorBunch.files.get(0).firstRowId()).isEqualTo(0);
        assertThat(vectorBunch.files.get(0).writeCols()).isEqualTo(Arrays.asList("vector_col"));
    }

    @Test
    public void testAddVectorEntryAndTail() {
        DataFileMeta vectorEntry = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorTail = createVectorFile("vector2", 100, 200, 1);

        vectorBunch.add(vectorEntry);
        vectorBunch.add(vectorTail);

        assertThat(vectorBunch.files).hasSize(2);
        assertThat(vectorBunch.files.get(0)).isEqualTo(vectorEntry);
        assertThat(vectorBunch.files.get(1)).isEqualTo(vectorTail);
        assertThat(vectorBunch.rowCount()).isEqualTo(300);
        assertThat(vectorBunch.files.get(0).firstRowId()).isEqualTo(0);
        assertThat(vectorBunch.files.get(0).writeCols()).isEqualTo(Arrays.asList("vector_col"));
        assertThat(vectorBunch.files.get(0).schemaId()).isEqualTo(0L);
    }

    @Test
    public void testAddNonVectorFileThrowsException() {
        DataFileMeta normalFile = createNormalFile("normal1.parquet", 0, 100, 1, 0L);

        assertThatThrownBy(() -> vectorBunch.add(normalFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only vector-store file can be added to this bunch.");
    }

    @Test
    public void testAddVectorFileWithSameFirstRowId() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 0, 50, 2);

        vectorBunch.add(vectorEntry1);
        // Adding file with same firstRowId but higher sequence number should throw exception
        assertThatThrownBy(() -> vectorBunch.add(vectorEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Vector file with same first row id should have decreasing sequence number.");
    }

    @Test
    public void testAddVectorFileWithSameFirstRowIdAndLowerSequenceNumber() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 2);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 0, 50, 1);

        vectorBunch.add(vectorEntry1);
        // Adding file with same firstRowId and lower sequence number should be ignored
        vectorBunch.add(vectorEntry2);

        assertThat(vectorBunch.files).hasSize(1);
        assertThat(vectorBunch.files.get(0)).isEqualTo(vectorEntry1);
    }

    @Test
    public void testAddVectorFileWithOverlappingRowId() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 2);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 50, 150, 1);

        vectorBunch.add(vectorEntry1);
        // Adding file with overlapping row id and lower sequence number should be ignored
        vectorBunch.add(vectorEntry2);

        assertThat(vectorBunch.files).hasSize(1);
        assertThat(vectorBunch.files.get(0)).isEqualTo(vectorEntry1);
    }

    @Test
    public void testAddVectorFileWithOverlappingRowIdAndHigherSequenceNumber() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 50, 150, 2);

        vectorBunch.add(vectorEntry1);
        // Adding file with overlapping row id and higher sequence number should throw exception
        assertThatThrownBy(() -> vectorBunch.add(vectorEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Vector file with overlapping row id should have decreasing sequence number.");
    }

    @Test
    public void testAddVectorFileWithNonContinuousRowId() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 200, 300, 1);

        vectorBunch.add(vectorEntry1);
        // Adding file with non-continuous row id should throw exception
        assertThatThrownBy(() -> vectorBunch.add(vectorEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Vector file first row id should be continuous, expect 100 but got 200");
    }

    @Test
    public void testAddVectorFileWithDifferentWriteCols() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorEntry2 =
                createVectorFileWithCols("vector2", 100, 200, 1, Arrays.asList("different_col"));

        vectorBunch.add(vectorEntry1);
        // Adding file with different write columns should throw exception
        assertThatThrownBy(() -> vectorBunch.add(vectorEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("All files in this bunch should have the same write columns.");
    }

    @Test
    public void testComplexVectorBunchScenario() {
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 100, 200, 1);
        DataFileMeta vectorEntry3 = createVectorFile("vector3", 300, 300, 1);
        DataFileMeta vectorTail = createVectorFile("vector4", 600, 400, 1);

        vectorBunch.add(vectorEntry1);
        vectorBunch.add(vectorEntry2);
        vectorBunch.add(vectorEntry3);
        vectorBunch.add(vectorTail);

        assertThat(vectorBunch.files).hasSize(4);
        assertThat(vectorBunch.rowCount()).isEqualTo(1000);
        assertThat(vectorBunch.files.get(0).firstRowId()).isEqualTo(0);
        assertThat(vectorBunch.files.get(0).writeCols()).isEqualTo(Arrays.asList("vector_col"));
    }

    @Test
    public void testComplexBlobBunchScenario2() {

        List<DataFileMeta> waited = new ArrayList<>();

        waited.add(createNormalFile("others.parquet", 0, 1000, 1, 1));
        waited.add(createBlobFile("blob1", 0, 1000, 1));
        waited.add(createBlobFile("blob2", 0, 500, 2));
        waited.add(createBlobFile("blob3", 500, 250, 2));
        waited.add(createBlobFile("blob4", 750, 250, 2));
        waited.add(createBlobFile("blob5", 0, 100, 3));
        waited.add(createBlobFile("blob6", 100, 400, 3));
        waited.add(createBlobFile("blob7", 750, 100, 3));
        waited.add(createBlobFile("blob8", 850, 150, 3));
        waited.add(createBlobFile("blob9", 100, 650, 4));

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(waited);
        assertThat(batches.size()).isEqualTo(1);

        List<DataFileMeta> batch = batches.get(0);

        assertThat(batch.get(1).fileName()).contains("blob5");
        assertThat(batch.get(2).fileName()).contains("blob2");
        assertThat(batch.get(3).fileName()).contains("blob1");
        assertThat(batch.get(4).fileName()).contains("blob9");
        assertThat(batch.get(5).fileName()).contains("blob6");
        assertThat(batch.get(6).fileName()).contains("blob3");
        assertThat(batch.get(7).fileName()).contains("blob7");
        assertThat(batch.get(8).fileName()).contains("blob4");
        assertThat(batch.get(9).fileName()).contains("blob8");

        List<FieldBunch> fieldBunches =
                splitFieldBunches(batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
        assertThat(fieldBunches.size()).isEqualTo(2);

        BlobFileBunch blobBunch = (BlobFileBunch) fieldBunches.get(1);
        assertThat(blobBunch.files).hasSize(9);
        assertThat(blobBunch.files.get(0).fileName()).contains("blob5");
        assertThat(blobBunch.files.get(1).fileName()).contains("blob2");
        assertThat(blobBunch.files.get(2).fileName()).contains("blob1");
        assertThat(blobBunch.files.get(3).fileName()).contains("blob9");
        assertThat(blobBunch.files.get(4).fileName()).contains("blob6");
        assertThat(blobBunch.files.get(5).fileName()).contains("blob3");
        assertThat(blobBunch.files.get(6).fileName()).contains("blob7");
        assertThat(blobBunch.files.get(7).fileName()).contains("blob4");
        assertThat(blobBunch.files.get(8).fileName()).contains("blob8");
    }

    @Test
    public void testComplexBlobBunchScenario3() {

        List<DataFileMeta> waited = new ArrayList<>();

        waited.add(createNormalFile("others.parquet", 0, 1000, 1, 1));
        waited.add(createBlobFile("blob1", 0, 1000, 1));
        waited.add(createBlobFile("blob2", 0, 500, 2));
        waited.add(createBlobFile("blob3", 500, 250, 2));
        waited.add(createBlobFile("blob4", 750, 250, 2));
        waited.add(createBlobFile("blob5", 0, 100, 3));
        waited.add(createBlobFile("blob6", 100, 400, 3));
        waited.add(createBlobFile("blob7", 750, 100, 3));
        waited.add(createBlobFile("blob8", 850, 150, 3));
        waited.add(createBlobFile("blob9", 100, 650, 4));
        waited.add(
                createBlobFileWithCols("blob11", 0, 1000, 1, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob12", 0, 500, 2, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob13", 500, 250, 2, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob14", 750, 250, 2, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob15", 0, 100, 3, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob16", 100, 400, 3, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob17", 750, 100, 3, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob18", 850, 150, 3, Collections.singletonList("blobc2")));
        waited.add(
                createBlobFileWithCols("blob19", 100, 650, 4, Collections.singletonList("blobc2")));

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(waited);
        assertThat(batches.size()).isEqualTo(1);

        List<DataFileMeta> batch = batches.get(0);

        List<FieldBunch> fieldBunches =
                splitFieldBunches(
                        batch, file -> makeBlobRowType(file.writeCols(), String::hashCode));
        assertThat(fieldBunches.size()).isEqualTo(3);

        BlobFileBunch blobBunch = (BlobFileBunch) fieldBunches.get(1);
        assertThat(blobBunch.files).hasSize(9);
        assertThat(blobBunch.files.get(0).fileName()).contains("blob5");
        assertThat(blobBunch.files.get(1).fileName()).contains("blob2");
        assertThat(blobBunch.files.get(2).fileName()).contains("blob1");
        assertThat(blobBunch.files.get(3).fileName()).contains("blob9");
        assertThat(blobBunch.files.get(4).fileName()).contains("blob6");
        assertThat(blobBunch.files.get(5).fileName()).contains("blob3");
        assertThat(blobBunch.files.get(6).fileName()).contains("blob7");
        assertThat(blobBunch.files.get(7).fileName()).contains("blob4");
        assertThat(blobBunch.files.get(8).fileName()).contains("blob8");

        blobBunch = (BlobFileBunch) fieldBunches.get(2);
        assertThat(blobBunch.files).hasSize(9);
        assertThat(blobBunch.files.get(0).fileName()).contains("blob15");
        assertThat(blobBunch.files.get(1).fileName()).contains("blob12");
        assertThat(blobBunch.files.get(2).fileName()).contains("blob11");
        assertThat(blobBunch.files.get(3).fileName()).contains("blob19");
        assertThat(blobBunch.files.get(4).fileName()).contains("blob16");
        assertThat(blobBunch.files.get(5).fileName()).contains("blob13");
        assertThat(blobBunch.files.get(6).fileName()).contains("blob17");
        assertThat(blobBunch.files.get(7).fileName()).contains("blob14");
        assertThat(blobBunch.files.get(8).fileName()).contains("blob18");
    }

    @Test
    public void testBlobOnlySplitWithMultipleBlobFields() {
        List<DataFileMeta> files = new ArrayList<>();
        files.add(createBlobFileWithCols("blob1", 0, 100, 1, Collections.singletonList("blobc1")));
        files.add(createBlobFileWithCols("blob2", 0, 100, 1, Collections.singletonList("blobc2")));

        List<FieldBunch> fieldBunches =
                splitFieldBunches(
                        files, file -> makeBlobRowType(file.writeCols(), String::hashCode));

        assertThat(fieldBunches).hasSize(2);
        assertThat(fieldBunches.get(0).rowCount()).isEqualTo(100);
        assertThat(fieldBunches.get(1).rowCount()).isEqualTo(100);
    }

    /** Creates a blob file with the specified parameters. */
    private DataFileMeta createBlobFile(
            String fileName, long firstRowId, long rowCount, long maxSequenceNumber) {
        return createBlobFileWithCols(
                fileName, firstRowId, rowCount, maxSequenceNumber, Arrays.asList("blob_col"));
    }

    /** Creates a blob file with a specified schemaId. */
    private DataFileMeta createBlobFileWithSchema(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId) {
        return DataFileMeta.create(
                fileName + ".blob",
                rowCount,
                rowCount,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                maxSequenceNumber,
                schemaId,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                rowCount,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                Arrays.asList("blob_col"));
    }

    /** Creates a blob file with custom write columns. */
    private DataFileMeta createBlobFileWithCols(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            List<String> writeCols) {
        return DataFileMeta.create(
                fileName + ".blob",
                rowCount,
                rowCount,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                maxSequenceNumber,
                0L,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                rowCount,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                writeCols);
    }

    private DataFileMeta createVectorFile(
            String fileName, long firstRowId, long rowCount, long maxSequenceNumber) {
        return createVectorFileWithCols(
                fileName, firstRowId, rowCount, maxSequenceNumber, Arrays.asList("vector_col"));
    }

    private DataFileMeta createVectorFileWithSchema(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId) {
        return createFile(
                fileName + ".vector.avro",
                firstRowId,
                rowCount,
                maxSequenceNumber,
                schemaId,
                Arrays.asList("vector_col"));
    }

    private DataFileMeta createVectorFileWithCols(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            List<String> writeCols) {
        return createFile(
                fileName + ".vector.avro", firstRowId, rowCount, maxSequenceNumber, 0L, writeCols);
    }

    private DataFileMeta createFile(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId,
            List<String> writeCols) {
        return DataFileMeta.create(
                fileName,
                rowCount,
                rowCount,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                maxSequenceNumber,
                schemaId,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                rowCount,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                writeCols);
    }

    @Test
    void testAddVectorFilesWithDifferentSchemaId() {
        DataFileMeta vectorEntry1 = createVectorFileWithSchema("vector1", 0, 100, 1, 0L);
        DataFileMeta vectorEntry2 = createVectorFileWithSchema("vector2", 100, 200, 1, 1L);

        vectorBunch.add(vectorEntry1);
        assertThatThrownBy(() -> vectorBunch.add(vectorEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("All files in this bunch should have the same schema id.");
    }

    @Test
    void testAddBlobFilesWithDifferentSchemaId() {
        BlobFileBunch blobBunch = new BlobFileBunch(300, false);
        DataFileMeta blobEntry1 = createBlobFileWithSchema("blob1", 0, 100, 1, 0L);
        DataFileMeta blobEntry2 = createBlobFileWithSchema("blob2", 100, 200, 1, 1L);

        blobBunch.add(blobEntry1);
        assertThatCode(() -> blobBunch.add(blobEntry2)).doesNotThrowAnyException();

        assertThat(blobBunch.files).hasSize(2);
        assertThat(blobBunch.files.get(0).schemaId()).isEqualTo(0L);
        assertThat(blobBunch.files.get(1).schemaId()).isEqualTo(1L);
        assertThat(blobBunch.rowCount()).isEqualTo(300);
    }

    @Test
    public void testRowIdPushDown() {
        VectorFileBunch vectorBunch = new VectorFileBunch(Long.MAX_VALUE, true);
        DataFileMeta vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        DataFileMeta vectorEntry2 = createVectorFile("vector2", 200, 300, 1);
        vectorBunch.add(vectorEntry1);
        VectorFileBunch finalVectorBunch = vectorBunch;
        DataFileMeta finalVectorEntry = vectorEntry2;
        assertThatCode(() -> finalVectorBunch.add(finalVectorEntry)).doesNotThrowAnyException();

        vectorBunch = new VectorFileBunch(Long.MAX_VALUE, true);
        vectorEntry1 = createVectorFile("vector1", 0, 100, 1);
        vectorEntry2 = createVectorFile("vector2", 50, 200, 2);
        vectorBunch.add(vectorEntry1);
        vectorBunch.add(vectorEntry2);
        assertThat(vectorBunch.files).containsExactlyInAnyOrder(vectorEntry2);

        VectorFileBunch finalVectorBunch2 = vectorBunch;
        DataFileMeta vectorEntry3 = createVectorFile("vector2", 250, 100, 2);
        assertThatCode(() -> finalVectorBunch2.add(vectorEntry3)).doesNotThrowAnyException();
    }

    /** Creates a normal (non-blob) file for testing. */
    private DataFileMeta createNormalFile(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId) {
        return DataFileMeta.create(
                fileName,
                rowCount,
                rowCount,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0L,
                maxSequenceNumber,
                schemaId,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                rowCount,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                null);
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
