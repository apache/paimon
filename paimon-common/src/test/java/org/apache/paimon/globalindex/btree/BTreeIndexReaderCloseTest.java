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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link BTreeIndexReader} always releases the file handle it opens. */
public class BTreeIndexReaderCloseTest {

    private static final int RECORD_NUM = 1000;

    @TempDir private java.nio.file.Path tempPath;

    private FileIO fileIO;
    private KeySerializer keySerializer;
    private GlobalIndexIOMeta meta;

    @BeforeEach
    public void setUp() throws Exception {
        fileIO = LocalFileIO.create();
        IntType dataType = new IntType();
        keySerializer = KeySerializer.create(dataType);

        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "test-btree-" + UUID.randomUUID() + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(path(fileName), true);
                    }
                };

        BTreeGlobalIndexer indexer =
                new BTreeGlobalIndexer(new DataField(1, "testField", dataType), new Options());
        GlobalIndexSingleColumnWriter writer = indexer.createWriter(fileWriter);
        for (int i = 0; i < RECORD_NUM; i++) {
            writer.write(i, (long) i);
        }
        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        ResultEntry entry = results.get(0);
        Path filePath = path(entry.fileName());
        meta = new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), results.get(0).meta());
    }

    /** A reader over a healthy file keeps the handle open, and close() releases it. */
    @Test
    public void testCloseReleasesTheInput() throws Exception {
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexReader reader =
                new BTreeIndexReader(
                        keySerializer,
                        tracking(closed),
                        meta,
                        new CacheManager(MemorySize.VALUE_8_MB, 0));
        assertThat(closed).hasValue(0);

        reader.close();
        assertThat(closed).hasValue(1);
    }

    /**
     * A corrupted footer makes the constructor fail after the file handle has been opened. Nothing
     * else holds a reference to it at that point, so the constructor has to release it itself.
     */
    @Test
    public void testFailedConstructionReleasesTheInput() throws Exception {
        corruptFooterMagic();

        AtomicInteger closed = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                new BTreeIndexReader(
                                        keySerializer,
                                        tracking(closed),
                                        meta,
                                        new CacheManager(MemorySize.VALUE_8_MB, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad magic number");

        assertThat(closed).hasValue(1);
    }

    /**
     * The reader and the file handle are two separate resources. A reader that fails to close must
     * still surface its own failure unchanged, but it must not strand the handle.
     */
    @Test
    public void testCloseReleasesTheInputWhenTheReaderFails() throws Exception {
        FailingCacheManager cacheManager = new FailingCacheManager();
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexReader reader =
                new BTreeIndexReader(keySerializer, tracking(closed), meta, cacheManager);

        cacheManager.failing = true;
        assertThatThrownBy(reader::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("cache is down");

        assertThat(closed).hasValue(1);
    }

    private Path path(String fileName) {
        return new Path(new Path(tempPath.toUri()), fileName);
    }

    /** Overwrite the four magic-number bytes the footer ends with. */
    private void corruptFooterMagic() throws IOException {
        try (RandomAccessFile file =
                new RandomAccessFile(new java.io.File(meta.filePath().toUri()), "rw")) {
            file.seek(file.length() - 4);
            file.writeInt(~BTreeFileFooter.MAGIC_NUMBER);
        }
    }

    private GlobalIndexFileReader tracking(AtomicInteger closed) {
        return ioMeta -> {
            SeekableInputStream delegate = fileIO.newInputStream(ioMeta.filePath());
            return new SeekableInputStream() {
                @Override
                public void seek(long desired) throws IOException {
                    delegate.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public int read() throws IOException {
                    return delegate.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return delegate.read(b, off, len);
                }

                @Override
                public void close() throws IOException {
                    closed.incrementAndGet();
                    delegate.close();
                }
            };
        };
    }

    /** Fails page invalidation, which is what closing the reader's bloom filter does. */
    private static class FailingCacheManager extends CacheManager {

        private boolean failing = false;

        FailingCacheManager() {
            super(MemorySize.VALUE_8_MB, 0);
        }

        @Override
        public void invalidPage(CacheKey key) {
            if (failing) {
                throw new RuntimeException("cache is down");
            }
            super.invalidPage(key);
        }
    }
}
