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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Prefix and range seeks must include entire boundary suffixes across SST blocks. */
class CompositeBTreeRangeTest {

    @TempDir java.nio.file.Path tempPath;

    private final RowType type =
            RowType.of(
                    new DataField(10, "category", DataTypes.INT()),
                    new DataField(20, "item_number", DataTypes.INT()),
                    new DataField(30, "suffix", DataTypes.INT()));
    private final PredicateBuilder builder = new PredicateBuilder(type);
    private final List<GenericRow> rows = new ArrayList<>();
    private final LocalFileIO io = LocalFileIO.create();

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testPrefixAndRangeSeeksAcrossBlocks(int version) throws Exception {
        Options options = options(version);
        List<GlobalIndexIOMeta> files = write(options);
        List<Predicate> queries =
                Arrays.asList(
                        builder.equal(0, 1),
                        PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 7)),
                        PredicateBuilder.and(builder.equal(0, 1), builder.greaterThan(1, 7)),
                        PredicateBuilder.and(builder.equal(0, 1), builder.greaterOrEqual(1, 7)),
                        PredicateBuilder.and(builder.equal(0, 1), builder.lessThan(1, 7)),
                        PredicateBuilder.and(builder.equal(0, 1), builder.lessOrEqual(1, 7)),
                        PredicateBuilder.and(builder.equal(0, 1), builder.between(1, -1, 7)),
                        PredicateBuilder.and(
                                builder.equal(0, 1),
                                builder.greaterThan(1, -1),
                                builder.lessThan(1, 7)),
                        PredicateBuilder.and(
                                builder.equal(0, 1),
                                builder.greaterOrEqual(1, -1),
                                builder.lessOrEqual(1, 7)),
                        builder.greaterThan(0, 0),
                        builder.lessOrEqual(0, 0),
                        builder.between(0, -1, 1),
                        PredicateBuilder.and(
                                builder.equal(0, 1), builder.greaterThan(1, Integer.MAX_VALUE)),
                        PredicateBuilder.and(
                                builder.equal(0, 1), builder.lessThan(1, Integer.MIN_VALUE)),
                        PredicateBuilder.and(
                                builder.equal(0, 1),
                                builder.greaterThan(1, 7),
                                builder.lessOrEqual(1, 7)),
                        PredicateBuilder.and(
                                builder.equal(0, 1), builder.equal(1, 7), builder.lessThan(1, 7)),
                        PredicateBuilder.and(builder.equal(0, null), builder.greaterThan(1, 7)),
                        PredicateBuilder.and(builder.equal(0, 1), builder.lessThan(1, null)));
        ExecutorService executor = newDirectExecutorService();
        try {
            for (List<Range> ranges :
                    Arrays.asList(null, Collections.singletonList(new Range(20, 150)))) {
                try (GlobalIndexReader reader =
                        GlobalIndexer.create("btree", type.getFields(), options)
                                .createReader(
                                        file -> io.newInputStream(file.filePath()),
                                        files,
                                        rows.size(),
                                        ranges,
                                        executor)) {
                    for (Predicate query : queries) {
                        RoaringNavigableMap64 expected = new RoaringNavigableMap64();
                        for (int i = 0; i < rows.size(); i++) {
                            if ((ranges == null || (i >= 20 && i <= 150))
                                    && query.test(rows.get(i))) {
                                expected.add(i);
                            }
                        }
                        assertThat(reader.visitComposite(query).join()).isPresent();
                        assertThat(
                                        reader.visitComposite(query)
                                                .join()
                                                .get()
                                                .results()
                                                .toRangeList())
                                .as("%s, local ranges %s", query, ranges)
                                .containsExactlyElementsOf(expected.toRangeList());
                    }
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testBudgetFallbackDoesNotDisablePointLookupsOrProvenEmptyRanges() throws Exception {
        Options options = options(2);
        List<GlobalIndexIOMeta> files = write(options);
        options.set(BTreeIndexOptions.BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE, MemorySize.ofBytes(0));
        ExecutorService executor = newDirectExecutorService();
        try (GlobalIndexReader reader =
                GlobalIndexer.create("btree", type.getFields(), options)
                        .createReader(
                                file -> io.newInputStream(file.filePath()),
                                files,
                                rows.size(),
                                null,
                                executor)) {
            assertThat(reader.visitComposite(builder.equal(0, 1)).join()).isEmpty();
            Predicate point =
                    PredicateBuilder.and(
                            builder.equal(0, 1), builder.equal(1, 7), builder.equal(2, 0));
            assertThat(reader.visitComposite(point).join().get().results().isEmpty()).isFalse();
            assertThat(
                            reader.visitComposite(builder.greaterThan(0, Integer.MAX_VALUE))
                                    .join()
                                    .get()
                                    .results()
                                    .isEmpty())
                    .isTrue();
            for (Predicate unsupported :
                    Arrays.asList(
                            builder.equal(1, 7),
                            builder.isNull(0),
                            builder.in(0, Arrays.asList(-1, 0, 1)))) {
                assertThat(reader.visitComposite(unsupported).join()).isEmpty();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private Options options(int version) {
        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_FILE_VERSION, version);
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE, MemorySize.ofBytes(128));
        options.set(BTreeIndexOptions.BTREE_INDEX_COMPRESSION, "lz4");
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOOM_FILTER_ENABLED, true);
        return options;
    }

    private List<GlobalIndexIOMeta> write(Options options) throws Exception {
        for (Integer category :
                Arrays.asList(null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE)) {
            for (Integer number :
                    Arrays.asList(null, Integer.MIN_VALUE, -1, 0, 7, 8, Integer.MAX_VALUE)) {
                for (int suffix = 0; suffix < 5; suffix++) {
                    rows.add(GenericRow.of(category, number, suffix));
                }
            }
        }
        rows.add(GenericRow.of(1, 7, 0));
        rows.add(GenericRow.of(1, 7, null));
        CompositeKeySerializer serializer = new CompositeKeySerializer(type);
        rows.sort((left, right) -> serializer.createComparator().compare(left, right));
        Path directory = new Path(tempPath.toUri());
        GlobalIndexFileWriter files =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String name) throws IOException {
                        return io.newOutputStream(new Path(directory, name), false);
                    }
                };
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexer.create("btree", type.getFields(), options)
                                .createWriter(files);
        for (int i = 0; i < rows.size(); i++) {
            writer.write(rows.get(i), i);
        }
        List<GlobalIndexIOMeta> result = new ArrayList<>();
        for (ResultEntry entry : writer.finish()) {
            Path path = new Path(directory, entry.fileName());
            result.add(
                    new GlobalIndexIOMeta(
                            path, io.getFileSize(path), entry.rowCount(), entry.meta()));
        }
        return result;
    }
}
