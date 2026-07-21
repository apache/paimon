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

package org.apache.paimon.data;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobReuseSource}. */
public class BlobReuseSourceTest {

    /** A UriReader over in-memory files that counts how many streams it opened. */
    private static final class CountingUriReader implements UriReader {
        private final Map<String, byte[]> files;
        int openCount;

        CountingUriReader(Map<String, byte[]> files) {
            this.files = files;
        }

        @Override
        public SeekableInputStream newInputStream(String uri) {
            openCount++;
            return new ByteArraySeekableStream(files.get(uri));
        }
    }

    private static byte[] range(int from, int to) {
        byte[] b = new byte[to - from];
        for (int i = from; i < to; i++) {
            b[i - from] = (byte) i;
        }
        return b;
    }

    /** A forward-only stream: reads and getPos work, but seek is unsupported. */
    private static class ForwardOnlyStream extends SeekableInputStream {
        private final byte[] data;
        private int pos;

        ForwardOnlyStream(byte[] data) {
            this.data = data;
        }

        @Override
        public void seek(long desired) {
            throw new UnsupportedOperationException("forward-only");
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xff : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void close() {}
    }

    @Test
    public void testBoundedToDescriptorWindow() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://f", range(0, 20));
        CountingUriReader reader = new CountingUriReader(files);
        BlobReuseSource reuse = new BlobReuseSource();

        // A window [5, 5) exposes only bytes 5..9 and then EOF -- not the rest of the file.
        BlobRef ref = new BlobRef(reader, new BlobDescriptor("mem://f", 5, 5));
        try (SeekableInputStream in = reuse.openBounded(ref)) {
            assertThat(IOUtils.readFully(in, false)).containsExactly(range(5, 10));
            assertThat(in.read()).isEqualTo(-1);
        }
        reuse.close();
    }

    @Test
    public void testReusesOneOpenAcrossWindowsAndSeeksWithin() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://f", range(0, 20));
        CountingUriReader reader = new CountingUriReader(files);
        BlobReuseSource reuse = new BlobReuseSource();

        byte[] first = new byte[5];
        IOUtils.readFully(
                reuse.openBounded(new BlobRef(reader, new BlobDescriptor("mem://f", 0, 5))), first);
        byte[] second = new byte[3];
        IOUtils.readFully(
                reuse.openBounded(new BlobRef(reader, new BlobDescriptor("mem://f", 12, 3))),
                second);
        reuse.close();

        assertThat(first).containsExactly(range(0, 5));
        assertThat(second).containsExactly(range(12, 15));
        // Same reader + uri across the two windows: opened once, not per reference.
        assertThat(reader.openCount).isEqualTo(1);
    }

    @Test
    public void testReopensWhenSourceUriChanges() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://a", range(0, 10));
        files.put("mem://b", range(100, 110));
        CountingUriReader reader = new CountingUriReader(files);
        BlobReuseSource reuse = new BlobReuseSource();

        BlobRef a = new BlobRef(reader, new BlobDescriptor("mem://a", 0, 4));
        BlobRef b = new BlobRef(reader, new BlobDescriptor("mem://b", 0, 4));
        reuse.prepareFor(a);
        reuse.openBounded(a);
        reuse.prepareFor(b);
        reuse.openBounded(b);
        reuse.close();

        assertThat(reader.openCount).isEqualTo(2);
    }

    @Test
    public void testPrepareForDifferentSourcePropagatesCloseError() throws IOException {
        UriReader reader =
                u ->
                        u.equals("mem://a")
                                ? new ByteArraySeekableStream(range(0, 4)) {
                                    @Override
                                    public void close() {
                                        throw new RuntimeException("close failure");
                                    }
                                }
                                : new ByteArraySeekableStream(range(0, 4));
        BlobReuseSource reuse = new BlobReuseSource();
        reuse.openBounded(new BlobRef(reader, new BlobDescriptor("mem://a", 0, 4)));
        // Switching away from a surfaces its close error rather than swallowing it.
        assertThatThrownBy(
                        () ->
                                reuse.prepareFor(
                                        new BlobRef(reader, new BlobDescriptor("mem://b", 0, 4))))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("close failure");
    }

    @Test
    public void testForwardOnlyRewindCloseErrorSurfaces() throws IOException {
        // Forward-only source can't rewind and also fails to close: the close error must surface,
        // not be swallowed by the reopen.
        UriReader reader =
                u ->
                        new ForwardOnlyStream(range(0, 5)) {
                            @Override
                            public void close() {
                                throw new RuntimeException("close failure");
                            }
                        };
        BlobReuseSource reuse = new BlobReuseSource();
        BlobRef ref = new BlobRef(reader, new BlobDescriptor("mem://f", 0, 5));
        reuse.prepareFor(ref);
        IOUtils.readFully(reuse.openBounded(ref), new byte[5]);
        // Second same-uri ref needs a rewind the source can't do; its failing close surfaces.
        assertThatThrownBy(() -> reuse.prepareFor(ref))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("close failure");
    }

    @Test
    public void testStaleViewCannotReadNewSource() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://a", range(0, 20));
        files.put("mem://b", range(100, 110));
        CountingUriReader reader = new CountingUriReader(files);
        BlobReuseSource reuse = new BlobReuseSource();

        // A view over a wide window of file a, then open a second ref into file b.
        SeekableInputStream stale =
                reuse.openBounded(new BlobRef(reader, new BlobDescriptor("mem://a", 0, 20)));
        BlobRef b = new BlobRef(reader, new BlobDescriptor("mem://b", 0, 4));
        reuse.prepareFor(b);
        reuse.openBounded(b);

        // The stale view must not read file b's bytes with the old (larger) length.
        assertThatThrownBy(() -> stale.read(new byte[20], 0, 20))
                .isInstanceOf(IllegalStateException.class);
        reuse.close();
    }

    @Test
    public void testViewIsStaleAfterClose() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://f", range(0, 20));
        BlobReuseSource reuse = new BlobReuseSource();
        SeekableInputStream view =
                reuse.openBounded(
                        new BlobRef(
                                new CountingUriReader(files), new BlobDescriptor("mem://f", 0, 5)));
        reuse.close();
        assertThatThrownBy(view::read).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSeekErrorNotMaskedByCloseError() {
        // seek fails (IOException); dropping the source then fails to close (RuntimeException).
        UriReader reader =
                u ->
                        new ByteArraySeekableStream(range(0, 20)) {
                            @Override
                            public void seek(long desired) throws IOException {
                                throw new IOException("seek failure");
                            }

                            @Override
                            public void close() {
                                throw new RuntimeException("close failure");
                            }
                        };
        BlobReuseSource reuse = new BlobReuseSource();
        // The original seek error must surface, not the follow-on close error.
        assertThatThrownBy(
                        () ->
                                reuse.openBounded(
                                        new BlobRef(reader, new BlobDescriptor("mem://f", 2, 3))))
                .isInstanceOf(IOException.class)
                .hasMessage("seek failure");
    }

    @Test
    public void testForwardOnlySourceReopensToRewind() throws IOException {
        // Two same-uri offset-0 refs: the second needs a rewind the forward-only stream can't do,
        // so the source is reopened rather than failing.
        int[] opens = {0};
        UriReader reader =
                u -> {
                    opens[0]++;
                    return new ForwardOnlyStream(range(0, 5));
                };
        BlobReuseSource reuse = new BlobReuseSource();

        BlobRef ref = new BlobRef(reader, new BlobDescriptor("mem://f", 0, 5));
        byte[] first = new byte[5];
        reuse.prepareFor(ref);
        IOUtils.readFully(reuse.openBounded(ref), first);
        byte[] second = new byte[5];
        reuse.prepareFor(ref);
        IOUtils.readFully(reuse.openBounded(ref), second);
        reuse.close();

        assertThat(first).containsExactly(range(0, 5));
        assertThat(second).containsExactly(range(0, 5));
        assertThat(opens[0]).isEqualTo(2); // reopened to rewind
    }

    @Test
    public void testReadArrayHonorsInputStreamContract() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://f", range(0, 20));
        BlobReuseSource reuse = new BlobReuseSource();
        SeekableInputStream in =
                reuse.openBounded(
                        new BlobRef(
                                new CountingUriReader(files), new BlobDescriptor("mem://f", 0, 5)));
        assertThat(in.read(new byte[4], 0, 0)).isZero(); // len 0 -> 0, not -1
        assertThatThrownBy(() -> in.read(null, 0, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> in.read(new byte[4], -1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> in.read(new byte[4], 0, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);
        reuse.close();
    }

    @Test
    public void testOpenBoundedRejectsMisuse() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://a", range(0, 10));
        files.put("mem://b", range(0, 10));
        CountingUriReader reader = new CountingUriReader(files);
        BlobReuseSource reuse = new BlobReuseSource();

        // Subclass (may override newInputStream()) and unknown length are rejected.
        BlobRef subclass = new BlobRef(reader, new BlobDescriptor("mem://a", 0, 4)) {};
        assertThatThrownBy(() -> reuse.openBounded(subclass))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                reuse.openBounded(
                                        new BlobRef(reader, new BlobDescriptor("mem://a", 0, -1))))
                .isInstanceOf(IllegalArgumentException.class);

        // A ref not matching the open source (prepareFor skipped) is rejected, not silently reused.
        reuse.openBounded(new BlobRef(reader, new BlobDescriptor("mem://a", 0, 4)));
        assertThatThrownBy(
                        () ->
                                reuse.openBounded(
                                        new BlobRef(reader, new BlobDescriptor("mem://b", 0, 4))))
                .isInstanceOf(IllegalStateException.class);
        reuse.close();
    }

    @Test
    public void testReturnedStreamIsReadOnly() throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        files.put("mem://f", range(0, 20));
        BlobReuseSource reuse = new BlobReuseSource();
        SeekableInputStream in =
                reuse.openBounded(
                        new BlobRef(
                                new CountingUriReader(files), new BlobDescriptor("mem://f", 5, 5)));
        assertThatThrownBy(() -> in.seek(0)).isInstanceOf(UnsupportedOperationException.class);
        reuse.close();
    }
}
