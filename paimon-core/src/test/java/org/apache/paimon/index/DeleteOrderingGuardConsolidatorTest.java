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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardReader;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardState;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies replacement ownership and cleanup even if closing an input reader fails. */
class DeleteOrderingGuardConsolidatorTest {

    @TempDir java.nio.file.Path tempDir;
    private final LocalFileIO fileIO = LocalFileIO.create();
    private final CacheManager cacheManager = new CacheManager(MemorySize.ofMebiBytes(1), 0.2);

    @AfterEach
    void closeCache() throws Exception {
        cacheManager.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCloseOnlyDeletesUndeliveredOutput(boolean delivered) throws Exception {
        DeleteOrderingGuardSerializer serializer = serializer();
        DeleteOrderingGuardFile file = file(serializer);
        List<IndexFileMeta> sources = sources(file, serializer);
        CompactResult result;
        try (DeleteOrderingGuardConsolidator consolidator =
                consolidator(file, serializer, sources, false)) {
            result = consolidator.rewrite(sources);
            assertThat(result.newIndexFiles())
                    .singleElement()
                    .satisfies(meta -> assertThat(meta.rowCount()).isEqualTo(1));
            assertThat(fileIO.exists(file.path(result.newIndexFiles().get(0)))).isTrue();
            if (delivered) {
                consolidator.release(result);
            }
        }
        assertThat(fileIO.exists(file.path(result.newIndexFiles().get(0)))).isEqualTo(delivered);
        for (IndexFileMeta source : sources) {
            assertThat(fileIO.exists(file.path(source))).isTrue();
        }
    }

    @Test
    void testAbortDeletesReplacementAndPreservesSources() throws Exception {
        DeleteOrderingGuardSerializer serializer = serializer();
        DeleteOrderingGuardFile file = file(serializer);
        List<IndexFileMeta> sources = sources(file, serializer);
        try (DeleteOrderingGuardConsolidator consolidator =
                consolidator(file, serializer, sources, false)) {
            CompactResult result = consolidator.rewrite(sources);
            consolidator.abort(result);
            assertThat(fileIO.exists(file.path(result.newIndexFiles().get(0)))).isFalse();
            for (IndexFileMeta source : sources) {
                assertThat(fileIO.exists(file.path(source))).isTrue();
            }
        }
    }

    @Test
    void testReaderCloseFailureAbortsFinishedReplacement() throws Exception {
        DeleteOrderingGuardSerializer serializer = serializer();
        DeleteOrderingGuardFile file = file(serializer);
        List<IndexFileMeta> sources = sources(file, serializer);
        try (DeleteOrderingGuardConsolidator consolidator =
                consolidator(file, serializer, sources, true)) {
            assertThatThrownBy(() -> consolidator.rewrite(sources))
                    .hasStackTraceContaining("Injected guard reader close failure");
            assertThat(fileIO.listStatus(new Path(tempDir.toUri()))).hasSize(2);
            for (IndexFileMeta source : sources) {
                assertThat(fileIO.exists(file.path(source))).isTrue();
            }
        }
    }

    private DeleteOrderingGuardConsolidator consolidator(
            DeleteOrderingGuardFile file,
            DeleteOrderingGuardSerializer serializer,
            List<IndexFileMeta> sources,
            boolean failClose)
            throws IOException {
        RecordReader<KeyValue> input =
                new IteratorRecordReader<KeyValue>(Arrays.asList(bound(), bound()).iterator()) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        if (failClose) {
                            throw new IOException("Injected guard reader close failure");
                        }
                    }
                };
        DeleteOrderingGuardState state = mock(DeleteOrderingGuardState.class);
        when(state.open(sources))
                .thenReturn(
                        new DeleteOrderingGuardReader(
                                Collections.singletonList(input),
                                (a, b) -> Integer.compare(a.getInt(0), b.getInt(0))));
        return new DeleteOrderingGuardConsolidator(state, serializer, file, null, 2);
    }

    private List<IndexFileMeta> sources(
            DeleteOrderingGuardFile file, DeleteOrderingGuardSerializer serializer)
            throws IOException {
        return Arrays.asList(source(file, serializer), source(file, serializer));
    }

    private IndexFileMeta source(
            DeleteOrderingGuardFile file, DeleteOrderingGuardSerializer serializer)
            throws IOException {
        try (DeleteOrderingGuardFile.Writer writer = file.createWriter()) {
            writer.write(serializer.extract(bound()));
            return writer.finish();
        }
    }

    private KeyValue bound() {
        return new KeyValue().replace(GenericRow.of(1), 4, RowKind.DELETE, GenericRow.of(4, null));
    }

    private DeleteOrderingGuardSerializer serializer() {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "f0");
        return new DeleteOrderingGuardSerializer(
                0,
                RowType.of(DataTypes.INT()),
                RowType.of(DataTypes.INT(), DataTypes.STRING()),
                new CoreOptions(options));
    }

    private DeleteOrderingGuardFile file(DeleteOrderingGuardSerializer serializer) {
        Path dir = new Path(tempDir.toUri());
        return new DeleteOrderingGuardFile(
                fileIO,
                new IndexPathFactory() {
                    @Override
                    public Path toPath(String name) {
                        return new Path(dir, name);
                    }

                    @Override
                    public Path newPath() {
                        return new Path(dir, UUID.randomUUID().toString());
                    }

                    @Override
                    public boolean isExternalPath() {
                        return false;
                    }
                },
                serializer,
                cacheManager,
                128);
    }
}
