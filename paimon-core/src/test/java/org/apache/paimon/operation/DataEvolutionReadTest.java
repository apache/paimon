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
import org.apache.paimon.utils.Range;

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
        vectorBunch = new VectorFileBunch(0, null);
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
    public void testNewVectorVersionPreservesOldTail() {
        DataFileMeta oldFile = createVectorFile("old", 0, 100, 1);
        DataFileMeta newFile = createVectorFile("new", 0, 50, 2);
        vectorBunch.add(oldFile);
        vectorBunch.add(newFile);

        assertVectorSelection(vectorBunch, newFile, new Range(0, 49), oldFile, new Range(50, 99));
    }

    @Test
    public void testNewVectorVersionReplacesCoveredOldRows() {
        DataFileMeta newFile = createVectorFile("new", 0, 100, 2);
        DataFileMeta oldFile = createVectorFile("old", 0, 50, 1);
        vectorBunch.add(newFile);
        vectorBunch.add(oldFile);

        assertThat(vectorBunch.rowCount()).isEqualTo(100);
        assertThat(vectorBunch.selectedFiles()).hasSize(1);
        assertThat(vectorBunch.selectedFiles().get(0).file).isEqualTo(newFile);
        assertThat(vectorBunch.selectedFiles().get(0).range).isEqualTo(new Range(0, 99));
    }

    @Test
    public void testOlderOverlappingVectorFileSuppliesUncoveredTail() {
        DataFileMeta newFile = createVectorFile("new", 0, 100, 2);
        DataFileMeta oldFile = createVectorFile("old", 50, 150, 1);
        vectorBunch.add(newFile);
        vectorBunch.add(oldFile);

        assertVectorSelection(vectorBunch, newFile, new Range(0, 99), oldFile, new Range(100, 199));
    }

    @Test
    public void testNewOverlappingVectorFilePreservesOldPrefix() {
        DataFileMeta oldFile = createVectorFile("old", 0, 100, 1);
        DataFileMeta newFile = createVectorFile("new", 50, 150, 2);
        vectorBunch.add(oldFile);
        vectorBunch.add(newFile);

        assertVectorSelection(vectorBunch, oldFile, new Range(0, 49), newFile, new Range(50, 199));
    }

    @Test
    public void testVectorSelectionRetainsGaps() {
        DataFileMeta first = createVectorFile("first", 0, 100, 1);
        DataFileMeta second = createVectorFile("second", 200, 300, 1);
        vectorBunch.add(first);
        vectorBunch.add(second);

        assertVectorSelection(vectorBunch, first, new Range(0, 99), second, new Range(200, 499));
    }

    @Test
    public void testVectorSelectionRetainsPhysicalColumnNames() {
        DataFileMeta oldFile = createVectorFile("old", 0, 100, 1);
        DataFileMeta renamedFile =
                createVectorFileWithCols(
                        "renamed", 100, 200, 2, Collections.singletonList("renamed_vector"));
        vectorBunch.add(oldFile);
        vectorBunch.add(renamedFile);

        assertVectorSelection(
                vectorBunch, oldFile, new Range(0, 99), renamedFile, new Range(100, 299));
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
                Arrays.asList("blob_col"),
                null);
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
                writeCols,
                null);
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
                writeCols,
                null);
    }

    @Test
    void testVectorSelectionAcrossSchemas() {
        DataFileMeta oldFile = createVectorFileWithSchema("old", 0, 100, 1, 0L);
        DataFileMeta newFile = createVectorFileWithSchema("new", 100, 200, 2, 1L);
        vectorBunch.add(oldFile);
        vectorBunch.add(newFile);

        assertVectorSelection(vectorBunch, oldFile, new Range(0, 99), newFile, new Range(100, 299));
    }

    @Test
    void testAddBlobFilesWithDifferentSchemaId() {
        BlobFileBunch blobBunch = new BlobFileBunch(new Range(0, 299), false);
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
    void testBlobBunchUsesNormalFileRangeForPartialCoverage() {
        BlobFileBunch blobBunch = new BlobFileBunch(new Range(100, 109), false);
        blobBunch.add(createBlobFile("blob", 103, 5, 1));

        assertThat(blobBunch.rowCount()).isEqualTo(10);
        assertThat(blobBunch.logicalRange()).isEqualTo(new Range(100, 109));
    }

    @Test
    void testBlobBunchRejectsRangeOutsideNormalFile() {
        BlobFileBunch blobBunch = new BlobFileBunch(new Range(100, 109), false);
        blobBunch.add(createBlobFile("blob", 99, 2, 1));

        assertThatThrownBy(blobBunch::rowCount)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("should be within normal file range");
    }

    @Test
    public void testVectorSelectionClipsToNormalRange() {
        VectorFileBunch clipped = new VectorFileBunch(0, new Range(50, 149));
        DataFileMeta oldFile = createVectorFile("old", 0, 200, 1);
        DataFileMeta newFile = createVectorFile("new", 100, 100, 2);
        clipped.add(oldFile);
        clipped.add(newFile);

        assertVectorSelection(clipped, oldFile, new Range(50, 99), newFile, new Range(100, 149));
        assertThat(oldFile.nonNullRowIdRange()).isEqualTo(new Range(0, 199));
        assertThat(newFile.nonNullRowIdRange()).isEqualTo(new Range(100, 199));
    }

    private static void assertVectorSelection(
            VectorFileBunch bunch,
            DataFileMeta first,
            Range firstRange,
            DataFileMeta second,
            Range secondRange) {
        assertThat(bunch.selectedFiles()).hasSize(2);
        assertThat(bunch.selectedFiles().get(0).file).isEqualTo(first);
        assertThat(bunch.selectedFiles().get(0).range).isEqualTo(firstRange);
        assertThat(bunch.selectedFiles().get(1).file).isEqualTo(second);
        assertThat(bunch.selectedFiles().get(1).range).isEqualTo(secondRange);
        assertThat(bunch.rowCount()).isEqualTo(firstRange.count() + secondRange.count());
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
                null,
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
