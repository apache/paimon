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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testIncrementalPackingMatchesBatchWeightsAndRawConversion() {
        List<DataFileMeta> first =
                Arrays.asList(
                        newFile("base-0.parquet", 0, 10, 100),
                        newFile("update-0.parquet", 0, 10, 20));
        List<DataFileMeta> second =
                Arrays.asList(
                        newFile("base-10.parquet", 10, 10, 80), newBlobFile("blob-10", 10, 3, 500));
        List<DataFileMeta> third =
                Collections.singletonList(newFile("base-20.parquet", 20, 10, 40));
        List<DataFileMeta> input = new ArrayList<>();
        input.addAll(first);
        input.addAll(second);
        input.addAll(third);
        for (boolean countBlobSize : Arrays.asList(false, true)) {
            DataEvolutionSplitGenerator generator =
                    new DataEvolutionSplitGenerator(200, 50, countBlobSize);
            List<SplitGenerator.SplitGroup> expected = generator.splitForBatch(input);
            List<SplitGenerator.SplitGroup> actual = new ArrayList<>();
            DataEvolutionSplitGenerator.BatchPacker<RuntimeException> packer =
                    generator.newBatchPacker(1024, actual::add);
            packer.addRangeGroup(first);
            packer.addRangeGroup(second);
            packer.addRangeGroup(third);
            packer.finish();
            assertThat(actual).hasSize(expected.size());
            for (int i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i).files).containsExactlyElementsOf(expected.get(i).files);
                assertThat(actual.get(i).rawConvertible).isEqualTo(expected.get(i).rawConvertible);
            }
        }
    }

    @Test
    void testFileCapDoesNotCutAConnectedDependencyGroup() {
        DataEvolutionSplitGenerator generator = new DataEvolutionSplitGenerator(1000, 0, false);
        List<SplitGenerator.SplitGroup> emitted = new ArrayList<>();
        DataEvolutionSplitGenerator.BatchPacker<RuntimeException> packer =
                generator.newBatchPacker(2, emitted::add);
        List<DataFileMeta> connected =
                Arrays.asList(
                        newFile("a.parquet", 0, 10, 0),
                        newFile("b.parquet", 0, 10, 0),
                        newFile("c.parquet", 0, 10, 0));
        DataFileMeta independent = newFile("next.parquet", 10, 10, 0);
        packer.addRangeGroup(connected);
        packer.addRangeGroup(Collections.singletonList(independent));
        packer.finish();
        assertThat(emitted).hasSize(2);
        assertThat(emitted.get(0).files).containsExactlyElementsOf(connected);
        assertThat(emitted.get(0).rawConvertible).isFalse();
        assertThat(emitted.get(1).files).containsExactly(independent);
        assertThat(emitted.get(1).rawConvertible).isTrue();
    }

    @Test
    void testEmissionIsImmediateAndCompletedGroupsStayStable() {
        DataEvolutionSplitGenerator generator = new DataEvolutionSplitGenerator(2, 1, false);
        List<SplitGenerator.SplitGroup> emitted = new ArrayList<>();
        DataEvolutionSplitGenerator.BatchPacker<RuntimeException> packer =
                generator.newBatchPacker(2, emitted::add);
        DataFileMeta first = newFile("first.parquet", 0, 1, 1);
        DataFileMeta second = newFile("second.parquet", 1, 1, 1);
        DataFileMeta third = newFile("third.parquet", 2, 1, 1);
        packer.addRangeGroup(Collections.singletonList(first));
        packer.addRangeGroup(Collections.singletonList(second));
        assertThat(emitted).isEmpty();
        packer.addRangeGroup(Collections.singletonList(third));
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).files).containsExactly(first, second);
        packer.finish();
        assertThat(emitted).hasSize(2);
        assertThat(emitted.get(0).files).containsExactly(first, second);
        assertThat(emitted.get(1).files).containsExactly(third);
        assertThatThrownBy(() -> packer.addRangeGroup(Collections.singletonList(third)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testEmptyPackingAndInvalidLimit() {
        DataEvolutionSplitGenerator generator = new DataEvolutionSplitGenerator(1000, 0, false);
        List<SplitGenerator.SplitGroup> emitted = new ArrayList<>();
        assertThatThrownBy(() -> generator.newBatchPacker(0, emitted::add))
                .isInstanceOf(IllegalArgumentException.class);
        DataEvolutionSplitGenerator.BatchPacker<RuntimeException> packer =
                generator.newBatchPacker(2, emitted::add);
        packer.finish();
        assertThat(emitted).isEmpty();
    }

    @Test
    void testCheckedConsumerFailureCannotBeResumedAsASuccessfulPlan() throws Exception {
        DataEvolutionSplitGenerator generator = new DataEvolutionSplitGenerator(1, 1, false);
        DataFileMeta first = newFile("first.parquet", 0, 1, 1);
        DataFileMeta second = newFile("second.parquet", 1, 1, 1);
        for (boolean failOnFinish : Arrays.asList(false, true)) {
            IOException failure = new IOException("metadata store write failed");
            DataEvolutionSplitGenerator.BatchPacker<IOException> packer =
                    generator.newBatchPacker(
                            2,
                            group -> {
                                throw failure;
                            });
            packer.addRangeGroup(Collections.singletonList(first));
            if (failOnFinish) {
                assertThatThrownBy(packer::finish).isSameAs(failure);
            } else {
                assertThatThrownBy(() -> packer.addRangeGroup(Collections.singletonList(second)))
                        .isSameAs(failure);
            }
            assertThatThrownBy(packer::finish).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> packer.addRangeGroup(Collections.singletonList(second)))
                    .isInstanceOf(IllegalStateException.class);
        }
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
