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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileMetadataCache;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shared {@link FileMetadataCache} is keyed by file path alone, so it may only hold full
 * footers: a selective footer is projection-specific, and caching it would serve later reads of the
 * same file a footer missing their column chunks. Selective reads must bypass the cache; full
 * footers keep using (and populating) it.
 */
class ParquetReaderFactoryFooterCacheTest {

    @TempDir java.nio.file.Path tempDir;

    /**
     * Two different projections of the same file, one shared context and cache, selective parser
     * enabled: the first read must not poison the cache for the second (pre-fix, the second read
     * failed on its missing column chunk), and neither read may consult the path-keyed cache.
     */
    @Test
    void testSharedCacheAcrossProjections() throws IOException {
        Path file = writeFile();
        CountingCache cache = new CountingCache();
        LocalFileIO fileIO = LocalFileIO.create();
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), null, null, cache);

        Options conf = new Options();
        conf.set(CoreOptions.PARQUET_FOOTER_SELECTIVE_PARSER_ENABLED, true);

        RowType idType =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.BIGINT())));
        assertThat(readAll(new ParquetReaderFactory(conf, idType, 1024, null), context))
                .containsExactly(1L, 2L, 3L);

        // same cache, same file, different projection: the cached entry must not be the
        // projection-specific footer from the read above
        RowType priceType =
                new RowType(Collections.singletonList(new DataField(1, "price", DataTypes.INT())));
        assertThat(
                        readAll(
                                new ParquetReaderFactory(conf, priceType, 1024, null),
                                context,
                                row -> row.getInt(0)))
                .containsExactly(10, 20, 30);

        // projection-specific footers never touched the shared cache
        assertThat(cache.lookups.get()).isEqualTo(0);
    }

    /** With the option off, full footers keep going through the shared cache. */
    @Test
    void testFullFooterUsesCache() throws IOException {
        Path file = writeFile();
        CountingCache cache = new CountingCache();
        LocalFileIO fileIO = LocalFileIO.create();
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), null, null, cache);

        Options conf = new Options();
        RowType idType =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.BIGINT())));

        assertThat(readAll(new ParquetReaderFactory(conf, idType, 1024, null), context))
                .containsExactly(1L, 2L, 3L);
        assertThat(readAll(new ParquetReaderFactory(conf, idType, 1024, null), context))
                .containsExactly(1L, 2L, 3L);
        // both reads consulted the cache (which parsed the footer only once)
        assertThat(cache.lookups.get()).isEqualTo(2);
    }

    private List<Object> readAll(ParquetReaderFactory factory, FormatReaderContext context)
            throws IOException {
        return readAll(factory, context, row -> row.getLong(0));
    }

    private List<Object> readAll(
            ParquetReaderFactory factory,
            FormatReaderContext context,
            Function<InternalRow, Object> extractor)
            throws IOException {
        List<Object> values = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader = factory.createReader(context)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    values.add(extractor.apply(row));
                }
                batch.releaseBatch();
            }
        }
        return values;
    }

    /** A file with BIGINT id and INT price columns, three rows. */
    private Path writeFile() throws IOException {
        RowType fileType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "price", DataTypes.INT())));
        Path file = new Path(tempDir.toUri().toString(), "cache.parquet");
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(fileType).create(out, "zstd");
            writer.addElement(GenericRow.of(1L, 10));
            writer.addElement(GenericRow.of(2L, 20));
            writer.addElement(GenericRow.of(3L, 30));
            writer.close();
        }
        return file;
    }

    /** Observes whether the cache is consulted, without changing its behavior. */
    private static class CountingCache extends FileMetadataCache {

        final AtomicInteger lookups = new AtomicInteger();

        @Override
        public <T> T getOrLoad(Path path, IOFunction<Path, T> loader) throws IOException {
            lookups.incrementAndGet();
            return super.getOrLoad(path, loader);
        }
    }
}
