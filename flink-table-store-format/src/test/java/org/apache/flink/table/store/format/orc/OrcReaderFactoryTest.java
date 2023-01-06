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

package org.apache.flink.table.store.format.orc;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.format.orc.filter.OrcFilters;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OrcReaderFactory}. */
class OrcReaderFactoryTest {

    /** Small batch size for test more boundary conditions. */
    protected static final int BATCH_SIZE = 9;

    private static final RowType FLAT_FILE_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType()
                    },
                    new String[] {
                        "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7",
                        "_col8"
                    });

    private static final RowType DECIMAL_FILE_TYPE =
            RowType.of(new LogicalType[] {new DecimalType(10, 5)}, new String[] {"_col0"});

    private static Path flatFile;
    private static Path decimalFile;

    @BeforeAll
    static void setupFiles(@TempDir java.nio.file.Path tmpDir) {
        flatFile = copyFileFromResource("test-data-flat.orc", tmpDir.resolve("test-data-flat.orc"));
        decimalFile =
                copyFileFromResource(
                        "test-data-decimal.orc", tmpDir.resolve("test-data-decimal.orc"));
    }

    @Test
    void testReadFileInSplits() throws IOException {
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        // read all splits
        for (FileSourceSplit split : createSplits(flatFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        assertThat(row.isNullAt(0)).isFalse();
                        assertThat(row.isNullAt(1)).isFalse();
                        totalF0.addAndGet(row.getInt(0));
                        assertThat(row.getString(1).toString()).isNotNull();
                        cnt.incrementAndGet();
                    });
        }

        // check that all rows have been read
        assertThat(cnt.get()).isEqualTo(1920800);
        assertThat(totalF0.get()).isEqualTo(1844737280400L);
    }

    @Test
    void testReadFileWithSelectFields() throws IOException {
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {2, 0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        // read all splits
        for (FileSourceSplit split : createSplits(flatFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        assertThat(row.isNullAt(0)).isFalse();
                        assertThat(row.isNullAt(1)).isFalse();
                        assertThat(row.isNullAt(2)).isFalse();
                        assertThat(row.getString(0).toString()).isNotNull();
                        totalF0.addAndGet(row.getInt(1));
                        assertThat(row.getString(2).toString()).isNotNull();
                        cnt.incrementAndGet();
                    });
        }

        // check that all rows have been read
        assertThat(cnt.get()).isEqualTo(1920800);
        assertThat(totalF0.get()).isEqualTo(1844737280400L);
    }

    @Test
    void testReadDecimalTypeFile() throws IOException {
        OrcReaderFactory format = createFormat(DECIMAL_FILE_TYPE, new int[] {0});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicInteger nullCount = new AtomicInteger(0);

        // read all splits
        for (FileSourceSplit split : createSplits(decimalFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        if (cnt.get() == 0) {
                            // validate first row
                            assertThat(row).isNotNull();
                            assertThat(row.getArity()).isEqualTo(1);
                            assertThat(row.getDecimal(0, 10, 5))
                                    .isEqualTo(DecimalDataUtils.castFrom(-1000.5d, 10, 5));
                        } else {
                            if (!row.isNullAt(0)) {
                                assertThat(row.getDecimal(0, 10, 5)).isNotNull();
                            } else {
                                nullCount.incrementAndGet();
                            }
                        }
                        cnt.incrementAndGet();
                    });
        }

        assertThat(cnt.get()).isEqualTo(6000);
        assertThat(nullCount.get()).isEqualTo(2000);
    }

    @Test
    void testReadFileAndRestore() throws IOException {
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {0, 1});

        // pick a middle split
        FileSourceSplit split = createSplits(flatFile, 3).get(1);

        int expectedCnt = 660000;

        innerTestRestore(format, split, expectedCnt, 656700330000L);
    }

    @Test
    void testReadFileAndRestoreWithFilter() throws IOException {
        List<OrcFilters.Predicate> filter =
                Collections.singletonList(
                        new OrcFilters.Or(
                                new OrcFilters.Between(
                                        "_col0", PredicateLeaf.Type.LONG, 0L, 975000L),
                                new OrcFilters.Equals("_col0", PredicateLeaf.Type.LONG, 980001L),
                                new OrcFilters.Between(
                                        "_col0", PredicateLeaf.Type.LONG, 990000L, 1800000L)));
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {0, 1}, filter);

        // pick a middle split
        FileSourceSplit split = createSplits(flatFile, 1).get(0);

        int expectedCnt = 1795000;
        long expectedTotalF0 = 1615113397500L;

        innerTestRestore(format, split, expectedCnt, expectedTotalF0);
    }

    private void innerTestRestore(
            OrcReaderFactory format, FileSourceSplit split, int expectedCnt, long expectedTotalF0)
            throws IOException {
        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        Consumer<RowData> consumer =
                row -> {
                    assertThat(row.isNullAt(0)).isFalse();
                    assertThat(row.isNullAt(1)).isFalse();
                    totalF0.addAndGet(row.getInt(0));
                    assertThat(row.getString(1).toString()).isNotNull();
                    cnt.incrementAndGet();
                };

        Utils.forEachRemaining(createReader(format, split), consumer);

        // check that all rows have been read
        assertThat(cnt.get()).isEqualTo(expectedCnt);
        assertThat(totalF0.get()).isEqualTo(expectedTotalF0);
    }

    protected OrcReaderFactory createFormat(RowType formatType, int[] selectedFields) {
        return createFormat(formatType, selectedFields, new ArrayList<>());
    }

    protected OrcReaderFactory createFormat(
            RowType formatType,
            int[] selectedFields,
            List<OrcFilters.Predicate> conjunctPredicates) {
        return new OrcReaderFactory(
                new Configuration(), formatType, selectedFields, conjunctPredicates, BATCH_SIZE);
    }

    private BulkFormat.Reader<RowData> createReader(OrcReaderFactory format, FileSourceSplit split)
            throws IOException {
        return format.createReader(new org.apache.flink.configuration.Configuration(), split);
    }

    private void forEach(OrcReaderFactory format, FileSourceSplit split, Consumer<RowData> action)
            throws IOException {
        Utils.forEachRemaining(createReader(format, split), action);
    }

    static Path copyFileFromResource(String resourceName, java.nio.file.Path file) {
        try (InputStream resource =
                OrcReaderFactoryTest.class
                        .getClassLoader()
                        .getResource(resourceName)
                        .openStream()) {
            Files.createDirectories(file.getParent());
            Files.copy(resource, file);
            return new Path(file.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<FileSourceSplit> createSplits(Path path, int minNumSplits)
            throws IOException {
        final List<FileSourceSplit> splits = new ArrayList<>(minNumSplits);
        final FileStatus fileStatus = path.getFileSystem().getFileStatus(path);
        final long len = fileStatus.getLen();
        final long preferSplitSize = len / minNumSplits + (len % minNumSplits == 0 ? 0 : 1);
        int splitNum = 0;
        long position = 0;
        while (position < len) {
            long splitLen = Math.min(preferSplitSize, len - position);
            splits.add(
                    new FileSourceSplit(
                            String.valueOf(splitNum++),
                            path,
                            position,
                            splitLen,
                            fileStatus.getModificationTime(),
                            len));
            position += splitLen;
        }
        return splits;
    }
}
