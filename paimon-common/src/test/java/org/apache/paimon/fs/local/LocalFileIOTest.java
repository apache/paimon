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

package org.apache.paimon.fs.local;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LocalFileIO}. */
public class LocalFileIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCopy() throws Exception {
        Path srcFile = new Path(tempDir.resolve("src.txt").toUri());
        Path dstFile = new Path(tempDir.resolve("dst.txt").toUri());

        FileIO fileIO = new LocalFileIO();
        fileIO.tryToWriteAtomic(srcFile, "foobar");

        fileIO.copyFile(srcFile, dstFile, false);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
        fileIO.deleteQuietly(dstFile);

        fileIO.copyFile(srcFile, dstFile, false);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
    }

    @Test
    public void testOverwriteFileUtf8IsAtomicForConcurrentReaders() throws Exception {
        Path file = new Path(tempDir.resolve("atomic-overwrite.txt").toUri());
        BlockingLocalFileIO fileIO = new BlockingLocalFileIO();
        String oldContent = "old:" + repeated('a', 64 * 1024);
        String newContent = "new:" + repeated('b', 128 * 1024);
        fileIO.writeFile(file, oldContent, false);

        fileIO.blockNextWrite();
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        Thread writer =
                new Thread(
                        () -> {
                            try {
                                fileIO.overwriteFileUtf8(file, newContent);
                            } catch (Throwable t) {
                                writeFailure.set(t);
                            }
                        });
        writer.start();

        try {
            assertThat(fileIO.awaitPartialWrite()).isTrue();
            for (int i = 0; i < 100; i++) {
                assertThat(fileIO.readFileUtf8(file)).isEqualTo(oldContent);
            }
        } finally {
            fileIO.finishWrite();
        }

        writer.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(writer.isAlive()).isFalse();
        assertThat(writeFailure.get()).isNull();
        assertThat(fileIO.readFileUtf8(file)).isEqualTo(newContent);
    }

    private static String repeated(char value, int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static class BlockingLocalFileIO extends LocalFileIO {

        private final AtomicBoolean blockNextWrite = new AtomicBoolean();
        private final CountDownLatch partialWrite = new CountDownLatch(1);
        private final CountDownLatch finishWrite = new CountDownLatch(1);

        private void blockNextWrite() {
            blockNextWrite.set(true);
        }

        private boolean awaitPartialWrite() throws InterruptedException {
            return partialWrite.await(10, TimeUnit.SECONDS);
        }

        private void finishWrite() {
            finishWrite.countDown();
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            PositionOutputStream delegate = super.newOutputStream(path, overwrite);
            if (!blockNextWrite.compareAndSet(true, false)) {
                return delegate;
            }

            return new PositionOutputStream() {
                private boolean blocked;

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public void write(int value) throws IOException {
                    delegate.write(value);
                    blockAfterPartialWrite();
                }

                @Override
                public void write(byte[] bytes) throws IOException {
                    write(bytes, 0, bytes.length);
                }

                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    if (length == 0) {
                        return;
                    }
                    int firstLength = Math.max(1, length / 2);
                    delegate.write(bytes, offset, firstLength);
                    blockAfterPartialWrite();
                    delegate.write(bytes, offset + firstLength, length - firstLength);
                }

                private void blockAfterPartialWrite() throws IOException {
                    if (blocked) {
                        return;
                    }
                    blocked = true;
                    partialWrite.countDown();
                    try {
                        if (!finishWrite.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting to finish the test write.");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(
                                "Interrupted while waiting to finish the test write.", e);
                    }
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }
    }
}
