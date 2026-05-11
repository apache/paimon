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
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionSplitGenerator}. */
public class DataEvolutionSplitGeneratorTest {

    @Test
    public void testCountBlobSize() {
        // When countBlobSize is true, blob file sizes should be counted in split weight
        List<DataFileMeta> files =
                Arrays.asList(
                        newFile("file1.parquet", 0L, 100L, 50),
                        newBlobFile("file1", 0L, 100L, 50000),
                        newFile("file2.parquet", 100L, 100L, 50),
                        newBlobFile("file2", 100L, 100L, 50000));

        DataEvolutionSplitGenerator generator = new DataEvolutionSplitGenerator(500, 1, true);
        List<SplitGenerator.SplitGroup> splits = generator.splitForBatch(files);
        assertThat(splits.size()).isEqualTo(2);

        generator = new DataEvolutionSplitGenerator(500, 1, false);
        splits = generator.splitForBatch(files);
        assertThat(splits.size()).isEqualTo(1);
    }

    private static DataFileMeta newFile(
            String name, long firstRowId, long rowCount, long fileSize) {
        return DataFileMeta.create(
                name,
                fileSize,
                rowCount,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                null,
                0,
                0,
                0,
                0,
                null,
                null,
                FileSource.APPEND,
                null,
                firstRowId,
                null);
    }

    private static DataFileMeta newBlobFile(
            String name, long firstRowId, long rowCount, long fileSize) {
        String blobFileName = name.endsWith(".blob") ? name : name + ".blob";
        return newFile(blobFileName, firstRowId, rowCount, fileSize);
    }
}
