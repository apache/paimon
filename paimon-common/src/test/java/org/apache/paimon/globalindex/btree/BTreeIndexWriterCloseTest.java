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

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link BTreeIndexWriter} releases the file it opened, on every path that abandons it.
 */
public class BTreeIndexWriterCloseTest {

    /**
     * finish() writes the null bitmap, the bloom filter, the index block and the footer before
     * closing. A write that fails part way through - a full disk is the ordinary cause - must not
     * leave the file open.
     */
    @Test
    public void testFinishReleasesTheFileWhenWritingFails() throws IOException {
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexWriter writer =
                new BTreeIndexWriter(
                        failingWriter(closed),
                        KeySerializer.create(new IntType()),
                        1024,
                        (BlockCompressionFactory) null);

        for (int i = 0; i < 200; i++) {
            writer.write(i, (long) i);
        }

        assertThatThrownBy(writer::finish)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error in closing BTree index writer");

        assertThat(closed).hasValue(1);
    }

    /**
     * A build can be abandoned before finish() ever runs - the row source fails, or a Flink or
     * Spark index build task is cancelled. The owner cleanup paths, PkSortedIndexFile#build and
     * SortedSingleColumnIndexWriter#close, release the writer only if they can see it as an
     * AutoCloseable, and skip it silently otherwise. This is that exact idiom.
     */
    @Test
    public void testTheOwnerCleanupPathReleasesAnAbandonedWriter() throws IOException {
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexWriter writer =
                new BTreeIndexWriter(
                        failingWriter(closed),
                        KeySerializer.create(new IntType()),
                        1024,
                        (BlockCompressionFactory) null);
        writer.write(1, 1L);

        if (writer instanceof AutoCloseable) {
            IOUtils.closeQuietly((AutoCloseable) writer);
        }

        assertThat(closed).hasValue(1);
    }

    /**
     * SortedGlobalIndexWriter holds the task writer in a try-with-resources, so close() can follow
     * a successful finish(). It must not close the stream a second time.
     */
    @Test
    public void testCloseIsIdempotent() throws IOException {
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexWriter writer =
                new BTreeIndexWriter(
                        failingWriter(closed),
                        KeySerializer.create(new IntType()),
                        1024,
                        (BlockCompressionFactory) null);
        writer.write(1, 1L);

        writer.close();
        writer.close();

        assertThat(closed).hasValue(1);
    }

    private static GlobalIndexFileWriter failingWriter(AtomicInteger closed) {
        return new GlobalIndexFileWriter() {

            @Override
            public String newFileName(String prefix) {
                return "test-btree" + prefix;
            }

            @Override
            public PositionOutputStream newOutputStream(String fileName) {
                return new PositionOutputStream() {

                    private long pos;
                    private final long capacity = 1500L;

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        write(new byte[] {(byte) b}, 0, 1);
                    }

                    @Override
                    public void write(byte[] b) throws IOException {
                        write(b, 0, b.length);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        if (pos + len > capacity) {
                            throw new IOException("no space left on device");
                        }
                        pos += len;
                    }

                    @Override
                    public void flush() {}

                    @Override
                    public void close() {
                        closed.incrementAndGet();
                    }
                };
            }
        };
    }
}
