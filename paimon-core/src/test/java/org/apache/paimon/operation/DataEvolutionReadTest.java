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
import org.apache.paimon.operation.DataEvolutionSplitRead.BlobBunch;
import org.apache.paimon.operation.DataEvolutionSplitRead.FieldBunch;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.operation.DataEvolutionSplitRead.splitFieldBunches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobBunch}. */
public class DataEvolutionReadTest {

    private BlobBunch blobBunch;

    @BeforeEach
    public void setUp() {
        blobBunch = new BlobBunch(Long.MAX_VALUE, false);
    }

    @Test
    public void testAddSingleBlobEntry() {
        DataFileMeta blobEntry = createBlobFile("blob1", 0L, 100L, 1L);

        blobBunch.add(blobEntry);

        assertThat(blobBunch.files).hasSize(1);
        assertThat(blobBunch.files.get(0)).isEqualTo(blobEntry);
        assertThat(blobBunch.rowCount()).isEqualTo(100);
        assertThat(blobBunch.files.get(0).firstRowId()).isEqualTo(0);
        assertThat(blobBunch.files.get(0).writeCols()).isEqualTo(Arrays.asList("blob_col"));
    }

    @Test
    public void testAddBlobEntryAndTail() {
        DataFileMeta blobEntry = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobTail = createBlobFile("blob2", 100, 200, 1);

        blobBunch.add(blobEntry);
        blobBunch.add(blobTail);

        assertThat(blobBunch.files).hasSize(2);
        assertThat(blobBunch.files.get(0)).isEqualTo(blobEntry);
        assertThat(blobBunch.files.get(1)).isEqualTo(blobTail);
        assertThat(blobBunch.rowCount()).isEqualTo(300);
        assertThat(blobBunch.files.get(0).firstRowId()).isEqualTo(0);
        assertThat(blobBunch.files.get(0).writeCols()).isEqualTo(Arrays.asList("blob_col"));
        assertThat(blobBunch.files.get(0).schemaId()).isEqualTo(0L);
    }

    @Test
    public void testAddNonBlobFileThrowsException() {
        DataFileMeta normalFile = createNormalFile("normal1.parquet", 0, 100, 1, 0L);

        assertThatThrownBy(() -> blobBunch.add(normalFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only blob file can be added to a blob bunch.");
    }

    @Test
    public void testAddBlobFileWithSameFirstRowId() {
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 0, 50, 2);

        blobBunch.add(blobEntry1);
        // Adding file with same firstRowId but higher sequence number should throw exception
        assertThatThrownBy(() -> blobBunch.add(blobEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Blob file with same first row id should have decreasing sequence number.");
    }

    @Test
    public void testAddBlobFileWithSameFirstRowIdAndLowerSequenceNumber() {
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 2);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 0, 50, 1);

        blobBunch.add(blobEntry1);
        // Adding file with same firstRowId and lower sequence number should be ignored
        blobBunch.add(blobEntry2);

        assertThat(blobBunch.files).hasSize(1);
        assertThat(blobBunch.files.get(0)).isEqualTo(blobEntry1);
    }

    @Test
    public void testAddBlobFileWithOverlappingRowId() {
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 2);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 50, 150, 1);

        blobBunch.add(blobEntry1);
        // Adding file with overlapping row id and lower sequence number should be ignored
        blobBunch.add(blobEntry2);

        assertThat(blobBunch.files).hasSize(1);
        assertThat(blobBunch.files.get(0)).isEqualTo(blobEntry1);
    }

    @Test
    public void testAddBlobFileWithOverlappingRowIdAndHigherSequenceNumber() {
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 50, 150, 2);

        blobBunch.add(blobEntry1);
        // Adding file with overlapping row id and higher sequence number should throw exception
        assertThatThrownBy(() -> blobBunch.add(blobEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Blob file with overlapping row id should have decreasing sequence number.");
    }

    @Test
    public void testAddBlobFileWithNonContinuousRowId() {
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 200, 300, 1);

        blobBunch.add(blobEntry1);
        // Adding file with non-continuous row id should throw exception
        assertThatThrownBy(() -> blobBunch.add(blobEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Blob file first row id should be continuous, expect 100 but got 200");
    }

    @Test
    public void testAddBlobFileWithDifferentWriteCols() {
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobEntry2 =
                createBlobFileWithCols("blob2", 100, 200, 1, Arrays.asList("different_col"));

        blobBunch.add(blobEntry1);
        // Adding file with different write columns should throw exception
        assertThatThrownBy(() -> blobBunch.add(blobEntry2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("All files in a blob bunch should have the same write columns.");
    }

    @Test
    public void testComplexBlobBunchScenario() {
        // Create a complex scenario with multiple blob entries and a tail
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 100, 200, 1);
        DataFileMeta blobEntry3 = createBlobFile("blob3", 300, 300, 1);
        DataFileMeta blobTail = createBlobFile("blob4", 600, 400, 1);

        blobBunch.add(blobEntry1);
        blobBunch.add(blobEntry2);
        blobBunch.add(blobEntry3);
        blobBunch.add(blobTail);

        assertThat(blobBunch.files).hasSize(4);
        assertThat(blobBunch.rowCount()).isEqualTo(1000);
        assertThat(blobBunch.files.get(0).firstRowId()).isEqualTo(0);
        assertThat(blobBunch.files.get(0).writeCols()).isEqualTo(Arrays.asList("blob_col"));
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

        assertThat(batch.get(1).fileName()).contains("blob5"); // pick
        assertThat(batch.get(2).fileName()).contains("blob2"); // skip
        assertThat(batch.get(3).fileName()).contains("blob1"); // skip
        assertThat(batch.get(4).fileName()).contains("blob9"); // pick
        assertThat(batch.get(5).fileName()).contains("blob6"); // skip
        assertThat(batch.get(6).fileName()).contains("blob3"); // skip
        assertThat(batch.get(7).fileName()).contains("blob7"); // pick
        assertThat(batch.get(8).fileName()).contains("blob4"); // skip
        assertThat(batch.get(9).fileName()).contains("blob8"); // pick

        List<FieldBunch> fieldBunches = splitFieldBunches(batch, file -> 0);
        assertThat(fieldBunches.size()).isEqualTo(2);

        BlobBunch blobBunch = (BlobBunch) fieldBunches.get(1);
        assertThat(blobBunch.files).hasSize(4);
        assertThat(blobBunch.files.get(0).fileName()).contains("blob5");
        assertThat(blobBunch.files.get(1).fileName()).contains("blob9");
        assertThat(blobBunch.files.get(2).fileName()).contains("blob7");
        assertThat(blobBunch.files.get(3).fileName()).contains("blob8");
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
                splitFieldBunches(batch, file -> file.writeCols().get(0).hashCode());
        assertThat(fieldBunches.size()).isEqualTo(3);

        BlobBunch blobBunch = (BlobBunch) fieldBunches.get(1);
        assertThat(blobBunch.files).hasSize(4);
        assertThat(blobBunch.files.get(0).fileName()).contains("blob5");
        assertThat(blobBunch.files.get(1).fileName()).contains("blob9");
        assertThat(blobBunch.files.get(2).fileName()).contains("blob7");
        assertThat(blobBunch.files.get(3).fileName()).contains("blob8");

        blobBunch = (BlobBunch) fieldBunches.get(2);
        assertThat(blobBunch.files).hasSize(4);
        assertThat(blobBunch.files.get(0).fileName()).contains("blob15");
        assertThat(blobBunch.files.get(1).fileName()).contains("blob19");
        assertThat(blobBunch.files.get(2).fileName()).contains("blob17");
        assertThat(blobBunch.files.get(3).fileName()).contains("blob18");
    }

    /** Creates a blob file with the specified parameters. */
    private DataFileMeta createBlobFile(
            String fileName, long firstRowId, long rowCount, long maxSequenceNumber) {
        return createBlobFileWithCols(
                fileName, firstRowId, rowCount, maxSequenceNumber, Arrays.asList("blob_col"));
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

    @Test
    public void testRowIdPushDown() {
        BlobBunch blobBunch = new BlobBunch(Long.MAX_VALUE, true);
        DataFileMeta blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        DataFileMeta blobEntry2 = createBlobFile("blob2", 200, 300, 1);
        blobBunch.add(blobEntry1);
        BlobBunch finalBlobBunch = blobBunch;
        DataFileMeta finalBlobEntry = blobEntry2;
        assertThatCode(() -> finalBlobBunch.add(finalBlobEntry)).doesNotThrowAnyException();

        blobBunch = new BlobBunch(Long.MAX_VALUE, true);
        blobEntry1 = createBlobFile("blob1", 0, 100, 1);
        blobEntry2 = createBlobFile("blob2", 50, 200, 2);
        blobBunch.add(blobEntry1);
        blobBunch.add(blobEntry2);
        assertThat(blobBunch.files).containsExactlyInAnyOrder(blobEntry2);

        BlobBunch finalBlobBunch2 = blobBunch;
        DataFileMeta blobEntry3 = createBlobFile("blob2", 250, 100, 2);
        assertThatCode(() -> finalBlobBunch2.add(blobEntry3)).doesNotThrowAnyException();
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
}
