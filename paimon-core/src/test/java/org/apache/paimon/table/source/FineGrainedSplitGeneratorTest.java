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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.format.FormatMetadataReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link FineGrainedSplitGenerator}. */
public class FineGrainedSplitGeneratorTest {

    private static final long THRESHOLD = 100L;
    private static final int MAX_SPLITS = 10;

    private FileIO fileIO;
    private FileStorePathFactory pathFactory;
    private FormatMetadataReader parquetReader;
    private SplitGenerator baseGenerator;

    @BeforeEach
    public void setUp() throws Exception {
        fileIO = mock(FileIO.class);
        pathFactory = mock(FileStorePathFactory.class);
        parquetReader = mock(FormatMetadataReader.class);
        baseGenerator = new AppendOnlySplitGenerator(1000, 1, false);

        when(pathFactory.bucketPath(any(BinaryRow.class), any(int.class)))
                .thenReturn(new Path("file:/tmp/bucket"));
        when(parquetReader.supportsFinerGranularity()).thenReturn(true);
    }

    @Test
    public void testSmallFileNotSplit() throws Exception {
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong()))
                .thenReturn(Collections.singletonList(new FileSplitBoundary(0, 50, 100)));

        FineGrainedSplitGenerator generator = newGenerator();
        List<DataFileMeta> files = Collections.singletonList(newParquetFile("small.parquet", 50));

        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(files, EMPTY_ROW, 0);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).boundary).isNull();
    }

    @Test
    public void testLargeFileProducesOneSplitGroupPerBoundary() throws Exception {
        List<FileSplitBoundary> boundaries =
                Arrays.asList(
                        new FileSplitBoundary(0, 100, 1000),
                        new FileSplitBoundary(100, 100, 1000),
                        new FileSplitBoundary(200, 100, 1000));
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong())).thenReturn(boundaries);

        FineGrainedSplitGenerator generator = newGenerator();
        List<DataFileMeta> files =
                Collections.singletonList(newParquetFile("large.parquet", 300));

        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(files, EMPTY_ROW, 0);

        assertThat(groups).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(groups.get(i).boundary).isNotNull();
            assertThat(groups.get(i).boundary).isEqualTo(boundaries.get(i));
            assertThat(groups.get(i).files).containsExactly(files.get(0));
        }
    }

    @Test
    public void testEachGroupCarriesItsOwnDistinctBoundary() throws Exception {
        List<FileSplitBoundary> boundaries =
                Arrays.asList(
                        new FileSplitBoundary(0, 50, 500),
                        new FileSplitBoundary(50, 75, 750));
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong())).thenReturn(boundaries);

        FineGrainedSplitGenerator generator = newGenerator();
        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(
                        Collections.singletonList(newParquetFile("f.parquet", 200)),
                        EMPTY_ROW,
                        0);

        assertThat(groups.get(0).boundary.offset()).isEqualTo(0);
        assertThat(groups.get(1).boundary.offset()).isEqualTo(50);
        assertThat(groups.get(0).boundary.length()).isEqualTo(50);
        assertThat(groups.get(1).boundary.length()).isEqualTo(75);
    }

    @Test
    public void testMaxSplitsCapApplied() throws Exception {
        List<FileSplitBoundary> boundaries =
                Arrays.asList(
                        new FileSplitBoundary(0, 40, 400),
                        new FileSplitBoundary(40, 40, 400),
                        new FileSplitBoundary(80, 40, 400),
                        new FileSplitBoundary(120, 40, 400),
                        new FileSplitBoundary(160, 40, 400));
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong())).thenReturn(boundaries);

        FineGrainedSplitGenerator generator =
                new FineGrainedSplitGenerator(
                        baseGenerator,
                        THRESHOLD,
                        3,
                        fileIO,
                        pathFactory,
                        Collections.singletonMap("parquet", parquetReader));

        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(
                        Collections.singletonList(newParquetFile("big.parquet", 200)),
                        EMPTY_ROW,
                        0);

        assertThat(groups).hasSize(3);
    }

    @Test
    public void testUnsupportedFormatNotSplit() throws Exception {
        FineGrainedSplitGenerator generator = newGenerator();
        List<DataFileMeta> files =
                Collections.singletonList(newFile("data.avro", 500));

        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(files, EMPTY_ROW, 0);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).boundary).isNull();
    }

    @Test
    public void testSingleBoundaryNotSplit() throws Exception {
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong()))
                .thenReturn(Collections.singletonList(new FileSplitBoundary(0, 200, 2000)));

        FineGrainedSplitGenerator generator = newGenerator();
        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(
                        Collections.singletonList(newParquetFile("one-rg.parquet", 200)),
                        EMPTY_ROW,
                        0);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).boundary).isNull();
    }

    @Test
    public void testMixedSmallAndLargeFiles() throws Exception {
        List<FileSplitBoundary> boundaries =
                Arrays.asList(
                        new FileSplitBoundary(0, 100, 1000),
                        new FileSplitBoundary(100, 100, 1000));
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong())).thenReturn(boundaries);

        DataFileMeta small = newParquetFile("small.parquet", 50);
        DataFileMeta large = newParquetFile("large.parquet", 300);
        FineGrainedSplitGenerator generator = newGenerator();

        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(Arrays.asList(small, large), EMPTY_ROW, 0);

        // small file → 1 group without boundary; large file → 2 groups with boundaries
        assertThat(groups).hasSize(3);
        long nullBoundaryCount =
                groups.stream().filter(g -> g.boundary == null).count();
        long nonNullBoundaryCount =
                groups.stream().filter(g -> g.boundary != null).count();
        assertThat(nullBoundaryCount).isEqualTo(1);
        assertThat(nonNullBoundaryCount).isEqualTo(2);
    }

    @Test
    public void testStreamingAlwaysDelegates() throws Exception {
        FineGrainedSplitGenerator generator = newGenerator();
        List<DataFileMeta> files =
                Collections.singletonList(newParquetFile("large.parquet", 300));

        List<SplitGenerator.SplitGroup> groups = generator.splitForStreaming(files);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).boundary).isNull();
    }

    @Test
    public void testIOExceptionFallsBackToSingleSplit() throws Exception {
        when(parquetReader.getSplitBoundaries(any(), any(), anyLong()))
                .thenThrow(new IOException("disk error"));

        FineGrainedSplitGenerator generator = newGenerator();
        List<SplitGenerator.SplitGroup> groups =
                generator.splitForBatch(
                        Collections.singletonList(newParquetFile("bad.parquet", 300)),
                        EMPTY_ROW,
                        0);

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).boundary).isNull();
    }

    private FineGrainedSplitGenerator newGenerator() {
        Map<String, FormatMetadataReader> readers = new HashMap<>();
        readers.put("parquet", parquetReader);
        return new FineGrainedSplitGenerator(
                baseGenerator, THRESHOLD, MAX_SPLITS, fileIO, pathFactory, readers);
    }

    private static DataFileMeta newParquetFile(String name, long fileSize) {
        return newFile(name, fileSize);
    }

    private static DataFileMeta newFile(String name, long fileSize) {
        return DataFileMeta.create(
                name,
                fileSize,
                100L,
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
                null,
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                null);
    }
}
