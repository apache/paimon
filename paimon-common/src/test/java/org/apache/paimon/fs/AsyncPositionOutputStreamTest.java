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

package org.apache.paimon.fs;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncPositionOutputStreamTest {

    @Test
    public void testNormal() throws IOException {
        ByteArrayPositionOutputStream byteOut = new ByteArrayPositionOutputStream();
        AsyncPositionOutputStream out = new AsyncPositionOutputStream(byteOut);
        out.write(1);
        out.write(new byte[] {2, 3});
        out.write(new byte[] {3, 4, 5}, 1, 1);
        out.close();
        assertThat(byteOut.out.toByteArray()).isEqualTo(new byte[] {1, 2, 3, 4});
    }

    @Test
    public void testGetPos() throws IOException {
        AsyncPositionOutputStream out =
                new AsyncPositionOutputStream(new ByteArrayPositionOutputStream());
        out.write(new byte[] {1, 2, 3});
        assertThat(out.getPos()).isEqualTo(3);
        out.write(new byte[] {5, 6, 7});
        assertThat(out.getPos()).isEqualTo(6);
    }

    @Test
    public void testFlush() throws IOException {
        ByteArrayPositionOutputStream byteOut =
                new ByteArrayPositionOutputStream() {
                    @Override
                    public void write(byte[] b) throws IOException {
                        try {
                            Thread.sleep(100);
                            super.write(b);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        AsyncPositionOutputStream out = new AsyncPositionOutputStream(byteOut);
        out.write(new byte[] {1, 2, 3});
        out.write(new byte[] {5, 6, 7});
        out.flush();
        assertThat(byteOut.getPos()).isEqualTo(6);

        out.write(new byte[] {8, 9});

        // test repeat flush
        out.flush();
        out.flush();
        assertThat(byteOut.getPos()).isEqualTo(8);

        assertThat(out.getBufferQueue().size()).isEqualTo(1);
    }

    @Test
    public void testFlushWithException() throws IOException {
        String msg = "your exception!";
        ByteArrayPositionOutputStream byteOut =
                new ByteArrayPositionOutputStream() {
                    @Override
                    public void write(byte[] b, int off, int len) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        throw new RuntimeException(msg);
                    }
                };
        AsyncPositionOutputStream out = new AsyncPositionOutputStream(byteOut);
        out.write(new byte[] {5, 6, 7});
        assertThatThrownBy(out::flush).hasMessage(msg);
    }

    @Test
    public void testClose() throws IOException {
        ByteArrayPositionOutputStream byteOut =
                new ByteArrayPositionOutputStream() {
                    @Override
                    public void write(byte[] b) throws IOException {
                        try {
                            Thread.sleep(100);
                            super.write(b);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        AsyncPositionOutputStream out = new AsyncPositionOutputStream(byteOut);
        out.write(new byte[] {1, 2, 3});
        out.write(new byte[] {5, 6, 7});
        out.close();
        assertThat(byteOut.getPos()).isEqualTo(6);
    }

    @Test
    public void testThrowException() throws IOException {
        String msg = "your exception!";
        ByteArrayPositionOutputStream out =
                new ByteArrayPositionOutputStream() {
                    @Override
                    public void write(byte[] b, int off, int len) {
                        throw new RuntimeException(msg);
                    }
                };

        AsyncPositionOutputStream asyncOut = new AsyncPositionOutputStream(out);
        asyncOut.write(new byte[] {1, 2, 3});
        assertThatThrownBy(asyncOut::close).hasMessageContaining(msg);
        assertThat(out.closed).isTrue();
    }

    private static class ByteArrayPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream out;

        private boolean closed;

        private ByteArrayPositionOutputStream() {
            this.out = new ByteArrayOutputStream();
        }

        @Override
        public long getPos() {
            return out.size();
        }

        @Override
        public void write(int b) {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
            closed = true;
        }
    }
}
