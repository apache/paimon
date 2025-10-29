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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link DataEvolutionSplitGenerator}. */
public class DataEvolutionSplitGeneratorTest {

    @Test
    public void testSplitWithSameFirstRowId() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 1, 10);
        DataFileMeta file2 = createFile("file2.parquet", 1L, 1, 20);
        DataFileMeta file3 = createFile("file3.parquet", 1L, 1, 30);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(1, result.size());
        assertEquals(Arrays.asList(file3, file2, file1), result.get(0));
    }

    @Test
    public void testSplitWithMixedFirstRowId() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 1, 1);
        DataFileMeta file2 = createFile("file2.parquet", 2L, 1, 2);
        DataFileMeta file3 = createFile("file3.parquet", 1L, 1, 3);
        DataFileMeta file4 = createFile("file4.parquet", 2L, 1, 4);
        DataFileMeta file5 = createFile("file5.parquet", 3L, 1, 5);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4, file5);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(3, result.size());
        assertEquals(Arrays.asList(file3, file1), result.get(0));
        assertEquals(Arrays.asList(file4, file2), result.get(1));
        assertEquals(Collections.singletonList(file5), result.get(2));
    }

    @Test
    public void testSplitWithComplexScenario() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 1, 1);
        DataFileMeta file2 = createFile("file2.parquet", 2L, 1, 3);
        DataFileMeta file3 = createFile("file3.parquet", 3L, 1, 5);
        DataFileMeta file4 = createFile("file4.parquet", 1L, 1, 2);
        DataFileMeta file5 = createFile("file5.parquet", 4L, 1, 8);
        DataFileMeta file6 = createFile("file6.parquet", 2L, 1, 4);
        DataFileMeta file7 = createFile("file7.parquet", 3L, 1, 6);
        DataFileMeta file8 = createFile("file8.parquet", 3L, 1, 7);
        DataFileMeta file9 = createFile("file9.parquet", 5L, 1, 9);

        List<DataFileMeta> files =
                Arrays.asList(file1, file2, file3, file4, file5, file6, file7, file8, file9);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(5, result.size());
        assertEquals(Arrays.asList(file4, file1), result.get(0));
        assertEquals(Arrays.asList(file6, file2), result.get(1));
        assertEquals(Arrays.asList(file8, file7, file3), result.get(2));
        assertEquals(Collections.singletonList(file5), result.get(3));
        assertEquals(Collections.singletonList(file9), result.get(4));
    }

    @Test
    public void testSplitWithMultipleBlobFilesPerGroup() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 10, 1);
        DataFileMeta file2 = createFile("file2.blob", 1L, 1, 1);
        DataFileMeta file3 = createFile("file3.blob", 2L, 9, 1);
        DataFileMeta file4 = createFile("file4.parquet", 20L, 10, 2);
        DataFileMeta file5 = createFile("file5.blob", 20L, 5, 2);
        DataFileMeta file6 = createFile("file6.blob", 25L, 5, 2);
        DataFileMeta file7 = createFile("file7.parquet", 1L, 10, 3);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4, file5, file6, file7);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(2, result.size());
        assertEquals(Arrays.asList(file7, file1, file2, file3), result.get(0));
        assertEquals(Arrays.asList(file4, file5, file6), result.get(1));
    }

    private static DataFileMeta createFile(
            String name, long firstRowId, long rowCount, long maxSequence) {
        return DataFileMeta.create(
                name,
                10000L,
                (int) rowCount,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                null,
                0L,
                maxSequence,
                0,
                0,
                0L,
                null,
                FileSource.APPEND,
                null,
                firstRowId,
                null);
    }
}
