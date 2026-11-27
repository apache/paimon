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
import org.apache.paimon.data.BinaryString;
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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.Range;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;
import shaded.parquet.org.apache.thrift.transport.TMemoryBuffer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
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

    /**
     * candidateRowRanges and createReader share the path-keyed footer cache, so they must parse the
     * footer with the same metadata filter. candidateRowRanges used to read with the full-file
     * range (a non-NO_FILTER filter) while createReader used NO_FILTER: for a file whose footer
     * lists a column chunk that is physically stored near the end, Parquet 1.16.0's midpoint-based
     * range filter computes a row-group midpoint outside the file and drops the valid row group.
     * The truncated footer was cached, so a later unfiltered read returned no rows even though a
     * direct read returned all of them.
     */
    @Test
    void testCachedFooterMatchesDirectReadWithReorderedColumnChunks() throws Exception {
        Path file = writeFileWithNameChunkFirstInFooter();
        LocalFileIO fileIO = LocalFileIO.create();
        long fileSize = fileIO.getFileSize(file);

        // selective parser disabled: full footers go through the shared cache
        Options conf = new Options();

        RowType idType =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.BIGINT())));
        Predicate idEquals2 = new PredicateBuilder(idType).equal(0, 2L);

        // seed the shared cache via candidateRowRanges with id = 2
        FileMetadataCache cache = new FileMetadataCache();
        FormatReaderContext candidateContext =
                new FormatReaderContext(fileIO, file, fileSize, null, null, cache);
        List<Range> ranges =
                new ParquetReaderFactory(conf, idType, 1024, Collections.singletonList(idEquals2))
                        .candidateRowRanges(candidateContext);

        // the candidate ranges cover the matching row (row index 1 == id 2)
        assertThat(ranges).isNotNull().isNotEmpty();
        assertThat(ranges.stream().anyMatch(range -> range.from <= 1 && range.to >= 1)).isTrue();

        // the same unfiltered id projection, read directly and with the seeded cache: both reads
        // must see the full row group and return [1, 2, 3]
        assertThat(
                        readAll(
                                new ParquetReaderFactory(conf, idType, 1024, null),
                                new FormatReaderContext(fileIO, file, fileSize, null, null, null)))
                .containsExactly(1L, 2L, 3L);
        assertThat(
                        readAll(
                                new ParquetReaderFactory(conf, idType, 1024, null),
                                new FormatReaderContext(fileIO, file, fileSize, null, null, cache)))
                .containsExactly(1L, 2L, 3L);
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

    /**
     * Writes a file with BIGINT id and STRING name columns, then rewrites its footer so the row
     * group lists the "name" chunk first and gives it a data-page offset near the end of the file,
     * even though the chunks are physically stored in schema order (id first). With the first chunk
     * in footer order anchored near the end, Parquet 1.16.0's midpoint-based range filter computes
     * a row-group midpoint outside the file and drops the (valid) row group.
     */
    private Path writeFileWithNameChunkFirstInFooter() throws Exception {
        RowType fileType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING())));
        Path original = new Path(tempDir.toUri().toString(), "reordered-data.parquet");
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(original, false)) {
            FormatWriter writer = format.createWriterFactory(fileType).create(out, "zstd");
            writer.addElement(GenericRow.of(1L, BinaryString.fromString("a")));
            writer.addElement(GenericRow.of(2L, BinaryString.fromString("b")));
            writer.addElement(GenericRow.of(3L, BinaryString.fromString("c")));
            writer.close();
        }

        byte[] bytes = Files.readAllBytes(java.nio.file.Paths.get(original.toUri()));
        int fileLen = bytes.length;
        int footerLength =
                (bytes[fileLen - 8] & 0xff)
                        | ((bytes[fileLen - 7] & 0xff) << 8)
                        | ((bytes[fileLen - 6] & 0xff) << 16)
                        | ((bytes[fileLen - 5] & 0xff) << 24);
        int footerStart = fileLen - 8 - footerLength;

        FileMetaData fileMetaData = new FileMetaData();
        fileMetaData.read(
                new TCompactProtocol(
                        new TIOStreamTransport(
                                new ByteArrayInputStream(
                                        Arrays.copyOfRange(
                                                bytes, footerStart, footerStart + footerLength)))));

        RowGroup rowGroup = fileMetaData.getRow_groups().get(0);
        List<ColumnChunk> columns = rowGroup.getColumns();
        ColumnChunk nameChunk = null;
        for (ColumnChunk column : columns) {
            if (column.getMeta_data().getPath_in_schema().get(0).equals("name")) {
                nameChunk = column;
                break;
            }
        }
        if (nameChunk == null) {
            throw new IllegalStateException("name column chunk not found in the footer");
        }
        columns.remove(nameChunk);
        columns.add(0, nameChunk);
        // a dictionary page would make getOffset use its (real) earlier offset; drop it and anchor
        // the chunk near the end of the file
        nameChunk.getMeta_data().unsetDictionary_page_offset();
        nameChunk.getMeta_data().setData_page_offset(fileLen - 8);

        byte[] newFooter = writeFooter(fileMetaData);
        byte[] rewritten = new byte[footerStart + newFooter.length + 8];
        System.arraycopy(bytes, 0, rewritten, 0, footerStart);
        System.arraycopy(newFooter, 0, rewritten, footerStart, newFooter.length);
        int footerIndex = footerStart + newFooter.length;
        rewritten[footerIndex] = (byte) (newFooter.length & 0xff);
        rewritten[footerIndex + 1] = (byte) ((newFooter.length >> 8) & 0xff);
        rewritten[footerIndex + 2] = (byte) ((newFooter.length >> 16) & 0xff);
        rewritten[footerIndex + 3] = (byte) ((newFooter.length >> 24) & 0xff);
        System.arraycopy(bytes, fileLen - 4, rewritten, footerIndex + 4, 4);

        Path path = new Path(tempDir.toUri().toString(), "reordered-footer.parquet");
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(path, false)) {
            out.write(rewritten);
        }
        return path;
    }

    private static byte[] writeFooter(FileMetaData fileMetaData) throws Exception {
        TMemoryBuffer buffer = new TMemoryBuffer(4096);
        fileMetaData.write(new TCompactProtocol(buffer));
        return Arrays.copyOf(buffer.getArray(), buffer.length());
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
