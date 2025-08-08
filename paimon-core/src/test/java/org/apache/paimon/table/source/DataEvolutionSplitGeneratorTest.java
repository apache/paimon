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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link DataEvolutionSplitGenerator}. */
public class DataEvolutionSplitGeneratorTest {

    private static DataFileMeta createFile(
            String name, @Nullable Long firstRowId, long maxSequence) {
        return new DataFileMeta(
                name,
                10000L,
                1,
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

    @Test
    public void testSplitWithNullFirstRowId() {
        DataFileMeta file1 = createFile("file1", null, 10);
        DataFileMeta file2 = createFile("file2", 1L, 20);
        DataFileMeta file3 = createFile("file3", 1L, 30);
        DataFileMeta file4 = createFile("file4", null, 40);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(3, result.size());
        assertEquals(Collections.singletonList(file4), result.get(0));
        assertEquals(Collections.singletonList(file1), result.get(1));
        assertEquals(Arrays.asList(file3, file2), result.get(2));
    }

    @Test
    public void testSplitWithSameFirstRowId() {
        DataFileMeta file1 = createFile("file1", 1L, 10);
        DataFileMeta file2 = createFile("file2", 1L, 20);
        DataFileMeta file3 = createFile("file3", 1L, 30);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(1, result.size());
        assertEquals(Arrays.asList(file3, file2, file1), result.get(0));
    }

    @Test
    public void testSplitWithDifferentFirstRowId() {
        DataFileMeta file1 = createFile("file1", 1L, 10);
        DataFileMeta file2 = createFile("file2", 2L, 20);
        DataFileMeta file3 = createFile("file3", 3L, 30);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(3, result.size());
        assertEquals(Collections.singletonList(file1), result.get(0));
        assertEquals(Collections.singletonList(file2), result.get(1));
        assertEquals(Collections.singletonList(file3), result.get(2));
    }

    @Test
    public void testSplitWithMixedFirstRowId() {
        DataFileMeta file1 = createFile("file1", 1L, 10);
        DataFileMeta file2 = createFile("file2", 2L, 20);
        DataFileMeta file3 = createFile("file3", 1L, 30);
        DataFileMeta file4 = createFile("file4", 2L, 40);
        DataFileMeta file5 = createFile("file5", 3L, 50);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4, file5);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(3, result.size());
        assertEquals(Arrays.asList(file3, file1), result.get(0));
        assertEquals(Arrays.asList(file4, file2), result.get(1));
        assertEquals(Collections.singletonList(file5), result.get(2));
    }

    @Test
    public void testSplitWithComplexScenario() {
        DataFileMeta file1 = createFile("file1", 1L, 30);
        DataFileMeta file2 = createFile("file2", 2L, 40);
        DataFileMeta file3 = createFile("file3", null, 10);
        DataFileMeta file4 = createFile("file4", 1L, 20);
        DataFileMeta file5 = createFile("file5", null, 50);
        DataFileMeta file6 = createFile("file6", 2L, 60);
        DataFileMeta file7 = createFile("file7", 3L, 70);
        DataFileMeta file8 = createFile("file8", 3L, 80);
        DataFileMeta file9 = createFile("file9", null, 90);

        List<DataFileMeta> files =
                Arrays.asList(file1, file2, file3, file4, file5, file6, file7, file8, file9);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(6, result.size());
        assertEquals(Collections.singletonList(file9), result.get(0));
        assertEquals(Collections.singletonList(file5), result.get(1));
        assertEquals(Collections.singletonList(file3), result.get(2));
        assertEquals(Arrays.asList(file1, file4), result.get(3));
        assertEquals(Arrays.asList(file6, file2), result.get(4));
        assertEquals(Arrays.asList(file8, file7), result.get(5));
    }

    @Test
    public void testOnlyNullFirstRowId() {
        DataFileMeta file1 = createFile("file1", null, 10);
        DataFileMeta file2 = createFile("file2", null, 20);
        DataFileMeta file3 = createFile("file3", null, 30);

        List<DataFileMeta> files = Arrays.asList(file2, file1, file3);
        List<List<DataFileMeta>> result = DataEvolutionSplitGenerator.split(files);

        assertEquals(3, result.size());
        assertEquals(Collections.singletonList(file3), result.get(0));
        assertEquals(Collections.singletonList(file2), result.get(1));
        assertEquals(Collections.singletonList(file1), result.get(2));
    }
}
