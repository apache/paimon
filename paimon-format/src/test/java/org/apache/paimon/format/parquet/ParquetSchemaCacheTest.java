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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the schema cache in {@link ParquetReaderFactory}.
 *
 * <p>The cache stores the computed {@code requestedSchema} and {@code fields} list keyed by the
 * actual {@code fileSchema} read from the Parquet footer. This avoids re-running {@code
 * clipParquetSchema} and {@code buildFieldsList} for every file that shares the same on-disk schema
 * within one factory instance.
 */
public class ParquetSchemaCacheTest {

    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT());

    @TempDir public File folder;

    // -------------------------------------------------------------------------
    // Cache population: empty before first read, one entry after
    // -------------------------------------------------------------------------

    @Test
    void testCacheIsEmptyBeforeFirstRead() throws Exception {
        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        assertThat(getSchemaCacheSize(factory)).isEqualTo(0);
    }

    @Test
    void testCacheIsPopulatedAfterFirstRead() throws Exception {
        Path path = writeSingleFile();

        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        readAll(factory, path);

        assertThat(getSchemaCacheSize(factory)).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // Cache reuse: same fileSchema produces only one cache entry
    // -------------------------------------------------------------------------

    @Test
    void testSameFileSchemaCausesOnlyOneCacheEntry() throws Exception {
        Path path1 = writeSingleFile();
        Path path2 = writeSingleFile();
        Path path3 = writeSingleFile();

        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        readAll(factory, path1);
        readAll(factory, path2);
        readAll(factory, path3);

        // All files have the same on-disk schema → only one cache entry
        assertThat(getSchemaCacheSize(factory)).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // Correctness: data is read correctly both on cold and warm cache
    // -------------------------------------------------------------------------

    @Test
    void testDataReadCorrectlyOnFirstRead() throws Exception {
        Path path = writeSingleFile();

        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        assertThat(countRows(factory, path)).isEqualTo(3);
    }

    @Test
    void testDataReadCorrectlyOnSubsequentCachedReads() throws Exception {
        Path path = writeSingleFile();

        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        assertThat(countRows(factory, path)).isEqualTo(3);
        assertThat(countRows(factory, path)).isEqualTo(3);
        assertThat(countRows(factory, path)).isEqualTo(3);
    }

    @Test
    void testMultipleFilesReadCorrectlyWithCache() throws Exception {
        Path path1 = writeSingleFile();
        Path path2 = writeSingleFile();

        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        assertThat(countRows(factory, path1)).isEqualTo(3);
        assertThat(countRows(factory, path2)).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // Independent instances: different factory instances have independent caches
    // -------------------------------------------------------------------------

    @Test
    void testTwoFactoryInstancesHaveIndependentCaches() throws Exception {
        Path path = writeSingleFile();

        ParquetReaderFactory factory1 =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);
        ParquetReaderFactory factory2 =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        readAll(factory1, path);

        assertThat(getSchemaCacheSize(factory1)).isEqualTo(1);
        assertThat(getSchemaCacheSize(factory2)).isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Path writeSingleFile() throws Exception {
        Path path = new Path(folder.getPath(), UUID.randomUUID() + ".parquet");
        Options conf = new Options();
        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, conf));
        FormatWriter writer =
                factory.create(new LocalFileIO().newOutputStream(path, false), "snappy");
        writer.addElement(
                GenericRow.of(1, org.apache.paimon.data.BinaryString.fromString("a"), 10L));
        writer.addElement(
                GenericRow.of(2, org.apache.paimon.data.BinaryString.fromString("b"), 20L));
        writer.addElement(
                GenericRow.of(3, org.apache.paimon.data.BinaryString.fromString("c"), 30L));
        writer.close();
        return path;
    }

    private void readAll(ParquetReaderFactory factory, Path path) throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        reader.forEachRemaining(row -> {});
    }

    private int countRows(ParquetReaderFactory factory, Path path) throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        AtomicInteger cnt = new AtomicInteger(0);
        reader.forEachRemaining(row -> cnt.incrementAndGet());
        return cnt.get();
    }

    private int getSchemaCacheSize(ParquetReaderFactory factory) throws Exception {
        Field field = ParquetReaderFactory.class.getDeclaredField("schemaCache");
        field.setAccessible(true);
        Map<?, ?> cache = (Map<?, ?>) field.get(factory);
        return cache.size();
    }
}
