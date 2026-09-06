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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.ChangelogProducer.INPUT;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link MergeTreeWriter#flushWriteBuffer} closes a changelog writer and a data writer in the same
 * {@code finally}. The data writer is a local, reachable from nowhere else once the method unwinds,
 * so a failing changelog close must not skip it.
 */
class MergeTreeWriterCloseFailureTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void dataWriterIsClosedWhenTheChangelogWriterFails() throws Exception {
        Path path = new Path(tempDir.toString());
        MergeTreeWriter writer = createWriter(path);

        writer.write(kv(1, 10));
        writer.write(kv(2, 20));

        // Every close fails here, which is what a broken underlying stream looks like. The
        // failure surfaces wrapped, because the writers close through AsyncPositionOutputStream.
        assertThatThrownBy(() -> writer.prepareCommit(false))
                .hasStackTraceContaining("close failed for");

        // The point of the test: no output stream is left open. Before the fix the data
        // writer's close was never reached, so its stream stayed registered.
        assertThat(TraceableFileIO.openOutputStreams(p -> p.toString().startsWith(path.toString())))
                .isEmpty();
    }

    private MergeTreeWriter createWriter(Path path) {
        RowType keyType = new RowType(singletonList(new DataField(0, "k", new IntType())));
        RowType valueType = new RowType(singletonList(new DataField(0, "v", new IntType())));

        Options options = new Options();
        options.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        options.set(CoreOptions.PAGE_SIZE, new MemorySize(4096));
        CoreOptions coreOptions = new CoreOptions(options);

        FileFormat avro = new FlushingFileFormat("avro");
        FileStorePathFactory pathFactory = createNonPartFactory(path);
        Function<String, FileStorePathFactory> pathFactoryMap = ignore -> pathFactory;

        KeyValueFileWriterFactory writerFactory =
                KeyValueFileWriterFactory.builder(
                                new CloseFailingFileIO(),
                                0,
                                keyType,
                                valueType,
                                avro,
                                pathFactoryMap,
                                coreOptions.targetFileSize(true))
                        .build(BinaryRow.EMPTY_ROW, 0, coreOptions);

        Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));
        MergeTreeWriter writer =
                new MergeTreeWriter(
                        false,
                        MemorySize.ofKibiBytes(10),
                        128,
                        CompressOptions.defaultOptions(),
                        null,
                        new NoopCompactManager(),
                        -1L,
                        comparator,
                        DeduplicateMergeFunction.factory().create(),
                        writerFactory,
                        false,
                        INPUT,
                        null,
                        null);
        writer.setMemoryPool(
                new HeapMemorySegmentPool(coreOptions.writeBufferSize(), coreOptions.pageSize()));
        return writer;
    }

    private KeyValue kv(int k, int v) {
        return new KeyValue().replace(GenericRow.of(k), RowKind.INSERT, GenericRow.of(v));
    }

    /** Closes the underlying stream and then reports the failure, as a full disk would. */
    private static class CloseFailingFileIO extends TraceableFileIO {

        @Override
        public PositionOutputStream newOutputStream(Path f, boolean overwrite) throws IOException {
            return new PositionOutputStreamWrapper(super.newOutputStream(f, overwrite)) {
                @Override
                public void close() throws IOException {
                    out.close();
                    throw new IOException("close failed for " + f.getName());
                }
            };
        }
    }
}
