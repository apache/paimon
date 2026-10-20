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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexer;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexer;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests scalar index payload rewrites through query results. */
class GlobalIndexRowIdRewriteTest {

    private static final DataField FIELD = new DataField(0, "value", new IntType());
    private static final FieldRef REF = new FieldRef(0, "value", new IntType());

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private Path basePath;
    private GlobalIndexFileReader fileReader;
    private GlobalIndexFileWriter fileWriter;
    private AtomicInteger outputFiles;

    @BeforeEach
    void setUp() {
        fileIO = LocalFileIO.create();
        basePath = new Path(tempPath.toUri());
        outputFiles = new AtomicInteger();
        fileReader = meta -> fileIO.newInputStream(meta.filePath());
        fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        outputFiles.incrementAndGet();
                        return fileIO.newOutputStream(new Path(basePath, fileName), false);
                    }
                };
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree-v1", "btree-v2", "bitmap"})
    void testPermutationAndRemovalAcrossPayloadFiles(String type) throws Exception {
        GlobalIndexer indexer = indexer(type);
        List<GlobalIndexIOMeta> source = new ArrayList<>();
        source.addAll(write(indexer, 0, 2, null, 1, 2));
        source.addAll(write(indexer, 4, -3, 4, null, 2));
        long[] mapping = {3, 1, -1, 0, 4, -1, 2, 5};
        List<ResultEntry> rewritten =
                indexer.rewriteRowIds(
                        fileReader, source, 8, 6, id -> mapping[(int) id], fileWriter);
        assertThat(rewritten).hasSize(2);
        assertThat(rewritten.stream().mapToLong(ResultEntry::rowCount).sum()).isEqualTo(6);
        try (GlobalIndexReader reader = reader(indexer, rewritten, 6)) {
            assertRows(reader.visitEqual(REF, 2).join(), 0, 3, 5);
            assertRows(reader.visitEqual(REF, -3).join(), 4);
            assertRows(reader.visitEqual(REF, 1).join());
            assertRows(reader.visitEqual(REF, 4).join());
            assertRows(reader.visitIsNull(REF).join(), 1, 2);
            assertRows(reader.visitIsNotNull(REF).join(), 0, 3, 4, 5);
            assertRows(reader.visitNotEqual(REF, 2).join(), 4);
            assertRows(reader.visitLessThan(REF, 0).join(), 4);
        }
        // A rewrite must leave the old payload usable by readers pinned to its snapshot.
        try (GlobalIndexReader reader =
                indexer.createReader(fileReader, source, 8, null, newDirectExecutorService())) {
            assertRows(reader.visitEqual(REF, 2).join(), 0, 3, 7);
            assertRows(reader.visitIsNull(REF).join(), 1, 6);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree-v1", "btree-v2", "bitmap"})
    void testMappedPostingIdsAreSortedAndUnique(String type) throws Exception {
        GlobalIndexer indexer = indexer(type);
        List<GlobalIndexIOMeta> source = write(indexer, 0, 7, 7, null, 7);
        long[] mapping = {2, 1, 0, 1};
        List<ResultEntry> rewritten =
                indexer.rewriteRowIds(
                        fileReader, source, 4, 3, id -> mapping[(int) id], fileWriter);
        assertThat(rewritten).hasSize(1);
        assertThat(rewritten.get(0).rowCount()).isEqualTo(3);
        try (GlobalIndexReader reader = reader(indexer, rewritten, 3)) {
            assertRows(reader.visitEqual(REF, 7).join(), 1, 2);
            assertRows(reader.visitIsNull(REF).join(), 0);
            assertRows(reader.visitNotEqual(REF, 8).join(), 1, 2);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree-v1", "btree-v2", "bitmap"})
    void testNullOnlyAndFullyExcludedPayloads(String type) throws Exception {
        GlobalIndexer indexer = indexer(type);
        List<GlobalIndexIOMeta> source = write(indexer, 0, null, null);
        List<ResultEntry> rewritten =
                indexer.rewriteRowIds(fileReader, source, 2, 2, id -> 1 - id, fileWriter);
        try (GlobalIndexReader reader = reader(indexer, rewritten, 2)) {
            assertRows(reader.visitIsNull(REF).join(), 0, 1);
            assertRows(reader.visitIsNotNull(REF).join());
        }
        int before = outputFiles.get();
        assertThat(indexer.rewriteRowIds(fileReader, source, 2, 1, id -> -1, fileWriter)).isEmpty();
        assertThat(indexer.rewriteRowIds(fileReader, source, 2, 0, id -> -1, fileWriter)).isEmpty();
        assertThat(outputFiles).hasValue(before);
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree-v1", "btree-v2", "bitmap"})
    void testRejectsMappingOutsideTargetDomain(String type) throws Exception {
        GlobalIndexer indexer = indexer(type);
        List<GlobalIndexIOMeta> source = write(indexer, 0, 7);
        assertThatThrownBy(
                        () -> indexer.rewriteRowIds(fileReader, source, 1, 1, id -> 1, fileWriter))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid target row ID");
    }

    private GlobalIndexer indexer(String type) {
        Options options = new Options();
        if (type.equals("bitmap")) {
            return new BitmapGlobalIndexer(FIELD, options);
        }
        options.set(BTreeIndexOptions.BTREE_INDEX_FILE_VERSION, type.equals("btree-v1") ? 1 : 2);
        return new BTreeGlobalIndexer(Collections.singletonList(FIELD), options);
    }

    private List<GlobalIndexIOMeta> write(GlobalIndexer indexer, long start, Integer... values)
            throws IOException {
        List<Pair<Integer, Long>> rows = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            rows.add(Pair.of(values[i], start + i));
        }
        Comparator<Integer> comparator = Comparator.nullsFirst(Integer::compareTo);
        rows.sort((left, right) -> comparator.compare(left.getKey(), right.getKey()));
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) indexer.createWriter(fileWriter);
        for (Pair<Integer, Long> row : rows) {
            writer.write(row.getKey(), row.getValue());
        }
        return toMetas(writer.finish());
    }

    private GlobalIndexReader reader(
            GlobalIndexer indexer, List<ResultEntry> entries, long rowCount) throws IOException {
        return indexer.createReader(
                fileReader, toMetas(entries), rowCount, null, newDirectExecutorService());
    }

    private List<GlobalIndexIOMeta> toMetas(List<ResultEntry> entries) throws IOException {
        List<GlobalIndexIOMeta> result = new ArrayList<>();
        for (ResultEntry entry : entries) {
            Path path = new Path(basePath, entry.fileName());
            result.add(
                    new GlobalIndexIOMeta(
                            path, fileIO.getFileSize(path), entry.rowCount(), entry.meta()));
        }
        return result;
    }

    private void assertRows(Optional<GlobalIndexResult> result, long... expected) {
        assertThat(result).isPresent();
        assertThat(result.get().results().toArray(Integer.MAX_VALUE)).containsExactly(expected);
    }
}
