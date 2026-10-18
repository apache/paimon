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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileMetadataCache;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParquetReaderFactory#candidateRowRanges}. */
public class ParquetCandidateRowRangesTest {

    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT());
    private static final int ROWS = 100_000;

    @TempDir public File folder;

    private final LocalFileIO fileIO = new LocalFileIO();
    private Path path;

    @BeforeEach
    public void before() throws Exception {
        path = new Path(folder.getPath(), UUID.randomUUID() + ".parquet");
        Options options = new Options();
        // several row groups of several pages each
        options.set("parquet.block.size", String.valueOf(256 * 1024));
        options.set("parquet.page.size", String.valueOf(8 * 1024));
        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, options));
        FormatWriter writer = factory.create(fileIO.newOutputStream(path, false), "snappy");
        for (int i = 0; i < ROWS; i++) {
            writer.addElement(
                    GenericRow.of(i, BinaryString.fromString("value-" + i), (long) (i % 7)));
        }
        writer.close();
    }

    @Test
    public void testRangesCoverMatchesAndPrune() throws Exception {
        List<Range> ranges = candidateRowRanges(builder().lessThan(0, 1000), null);

        assertThat(ranges).isNotNull();
        assertThat(covered(ranges)).isLessThan(ROWS / 10);
        assertThat(contains(ranges, 0, 999)).isTrue();
    }

    @Test
    public void testRangesAreFilePositionsAcrossRowGroups() throws Exception {
        List<Range> ranges =
                candidateRowRanges(
                        PredicateBuilder.and(
                                builder().greaterOrEqual(0, 60_000),
                                builder().lessOrEqual(0, 60_010)),
                        null);

        assertThat(ranges).isNotNull();
        assertThat(contains(ranges, 60_000, 60_010)).isTrue();
        assertThat(contains(ranges, 0, 0)).isFalse();
        assertThat(covered(ranges)).isLessThan(ROWS / 10);
        assertThat(readSelected(ranges, 0)).contains(60_000, 60_005, 60_010);
    }

    @Test
    public void testNoMatchAndNoFilter() throws Exception {
        assertThat(candidateRowRanges(builder().greaterThan(0, ROWS), null)).isEmpty();
        assertThat(
                        new ParquetReaderFactory(new Options(), ROW_TYPE, 500, null)
                                .candidateRowRanges(context(null)))
                .isNull();
    }

    @Test
    public void testSelectedRowsContainAllMatches() throws Exception {
        List<Range> ranges = candidateRowRanges(builder().lessThan(0, 1000), null);
        List<Integer> ids = readSelected(ranges, 0);

        List<Integer> matched = new ArrayList<>();
        for (int id : ids) {
            if (id < 1000) {
                matched.add(id);
            }
        }
        // the format reader skips whole pages only, rows are cut exactly by the caller
        assertThat(matched).hasSize(1000);
        assertThat((long) ids.size()).isGreaterThanOrEqualTo(covered(ranges));
    }

    @Test
    public void testFooterIsReadOnceWithCache() throws Exception {
        int[] loads = new int[1];
        FileMetadataCache cache =
                new FileMetadataCache() {
                    @Override
                    public <T> T getOrLoad(Path path, IOFunction<Path, T> loader)
                            throws IOException {
                        return super.getOrLoad(
                                path,
                                p -> {
                                    loads[0]++;
                                    return loader.apply(p);
                                });
                    }
                };

        List<Range> ranges = candidateRowRanges(builder().lessThan(0, 1000), cache);
        ParquetReaderFactory reader = new ParquetReaderFactory(new Options(), ROW_TYPE, 500, null);
        int rows = 0;
        try (RecordReader<InternalRow> recordReader =
                reader.createReader(context(cache, toSelection(ranges)))) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = recordReader.readBatch()) != null) {
                while (batch.next() != null) {
                    rows++;
                }
                batch.releaseBatch();
            }
        }

        assertThat(loads[0]).isEqualTo(1);
        assertThat((long) rows).isGreaterThanOrEqualTo(covered(ranges));
        assertThat(rows).isLessThan(ROWS / 10);
    }

    private static PredicateBuilder builder() {
        return new PredicateBuilder(ROW_TYPE);
    }

    private List<Range> candidateRowRanges(Predicate predicate, FileMetadataCache cache)
            throws IOException {
        return new ParquetReaderFactory(
                        new Options(), ROW_TYPE, 500, Collections.singletonList(predicate))
                .candidateRowRanges(context(cache));
    }

    private FormatReaderContext context(FileMetadataCache cache) throws IOException {
        return context(cache, null);
    }

    private FormatReaderContext context(FileMetadataCache cache, RoaringBitmap32 selection)
            throws IOException {
        return new FormatReaderContext(
                fileIO, path, fileIO.getFileSize(path), selection, null, cache);
    }

    private List<Integer> readSelected(List<Range> ranges, int field) throws IOException {
        List<Integer> values = new ArrayList<>();
        ParquetReaderFactory reader = new ParquetReaderFactory(new Options(), ROW_TYPE, 500, null);
        try (RecordReader<InternalRow> recordReader =
                reader.createReader(context(null, toSelection(ranges)))) {
            recordReader.forEachRemaining(row -> values.add(row.getInt(field)));
        }
        return values;
    }

    private static RoaringBitmap32 toSelection(List<Range> ranges) {
        RoaringBitmap32 selection = new RoaringBitmap32();
        for (Range range : ranges) {
            for (long row = range.from; row <= range.to; row++) {
                selection.add((int) row);
            }
        }
        return selection;
    }

    private static long covered(List<Range> ranges) {
        return ranges.stream().mapToLong(Range::count).sum();
    }

    private static boolean contains(List<Range> ranges, long from, long to) {
        return ranges.stream().anyMatch(range -> range.from <= from && range.to >= to);
    }
}
