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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.UriReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Reuses one underlying source stream across consecutive {@link BlobRef}s that read from the same
 * file, so writing N such references opens the source once instead of once per reference. Callers
 * only ever receive a stream bounded to a reference's descriptor window; the underlying {@link
 * UriReader} never leaves this class, so a caller cannot read past the blob bounds or open another
 * URI. Not a public API.
 */
public final class BlobReuseSource implements Closeable {

    @Nullable private UriReader reader;
    @Nullable private String uri;
    @Nullable private SeekableInputStream underlying;
    private long position;
    // Bumped on every open/close so a previously handed-out view can't read the new source.
    private long epoch;

    /**
     * Positions the open source for {@code ref}, or closes it (so {@link #openBounded} reopens a
     * fresh one) when it reads a different file or can't rewind. Its close error surfaces here
     * rather than inside {@code openBounded}, so cleanup of a previous, already-copied source is
     * never mistaken for {@code ref}'s fetch failure and turned into a NULL write. Call before
     * {@code openBounded}.
     */
    public void prepareFor(BlobRef ref) throws IOException {
        epoch++; // invalidate any view handed out for the previous reference
        if (underlying == null) {
            return;
        }
        long offset = ref.toDescriptor().offset();
        boolean sameSource = reader == ref.uriReader() && ref.toDescriptor().uri().equals(uri);
        // Different file, or same file that can't rewind: drop it so openBounded reopens fresh.
        if (!sameSource || (position != offset && !trySeek(offset))) {
            close();
        }
    }

    /**
     * Returns a stream bounded to {@code ref}'s descriptor, reusing the source left open by {@link
     * #prepareFor} or opening a fresh one, seeking only when not already positioned. Open/seek
     * errors propagate so the caller can apply its write-null policy; on such an error the source
     * is dropped.
     */
    public SeekableInputStream openBounded(BlobRef ref) throws IOException {
        BlobDescriptor descriptor = ref.toDescriptor();
        // Fail closed: a mismatched ref would silently read the wrong or unbounded bytes.
        if (ref.getClass() != BlobRef.class) {
            throw new IllegalArgumentException(
                    "BlobReuseSource does not support BlobRef subclasses.");
        }
        if (descriptor.length() < 0) {
            throw new IllegalArgumentException("BlobReuseSource requires a non-negative length.");
        }
        if (underlying != null && (reader != ref.uriReader() || !descriptor.uri().equals(uri))) {
            throw new IllegalStateException(
                    "openBounded ref differs from prepareFor; call prepareFor first.");
        }
        epoch++; // invalidate any view handed out for the previous reference
        long offset = descriptor.offset();
        try {
            if (underlying == null) {
                underlying = ref.uriReader().newInputStream(descriptor.uri());
                reader = ref.uriReader();
                uri = descriptor.uri();
                position = 0;
            }
            if (position != offset) {
                underlying.seek(offset);
                position = offset;
            }
        } catch (IOException | RuntimeException | Error e) {
            discardQuietly();
            throw e;
        }
        return new BoundedSource(epoch, descriptor.length());
    }

    /** Seeks the open source, returning false instead of throwing when it can't reposition. */
    private boolean trySeek(long offset) {
        try {
            underlying.seek(offset);
            position = offset;
            return true;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    /** Drops the underlying source, swallowing any close error (used after a copy failure). */
    public void discardQuietly() {
        try {
            close();
        } catch (RuntimeException | Error | IOException ignored) {
            // Swallow so the triggering open/seek/read error isn't masked.
        }
    }

    @Override
    public void close() throws IOException {
        epoch++; // invalidate any outstanding view
        closeUnderlying();
    }

    private void closeUnderlying() throws IOException {
        SeekableInputStream toClose = underlying;
        underlying = null;
        reader = null;
        uri = null;
        position = 0;
        if (toClose != null) {
            toClose.close();
        }
    }

    /**
     * A read-only view over the shared underlying stream, capped at the descriptor length. Reads
     * advance the shared position; {@link #close()} keeps the underlying open for the next blob. It
     * is only valid until the next {@code openBounded}/{@code close}; using it afterwards throws
     * rather than reading the new source.
     */
    private final class BoundedSource extends SeekableInputStream {

        private final long viewEpoch;
        private final long length;
        private long remaining;

        private BoundedSource(long viewEpoch, long length) {
            this.viewEpoch = viewEpoch;
            this.length = length;
            this.remaining = length;
        }

        private void checkValid() {
            if (viewEpoch != epoch) {
                throw new IllegalStateException(
                        "Stale BlobReuseSource stream: a newer reference was opened or it was closed.");
            }
        }

        @Override
        public int read() throws IOException {
            checkValid();
            if (remaining <= 0) {
                return -1;
            }
            int b = underlying.read();
            if (b >= 0) {
                remaining--;
                position++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            checkValid();
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }
            if (remaining <= 0) {
                return -1;
            }
            int n = underlying.read(b, off, (int) Math.min(len, remaining));
            if (n > 0) {
                remaining -= n;
                position += n;
            }
            return n;
        }

        @Override
        public void seek(long desired) {
            throw new UnsupportedOperationException("BlobReuseSource stream is read-only.");
        }

        @Override
        public long getPos() {
            checkValid();
            return length - remaining;
        }

        @Override
        public void close() {
            remaining = 0;
        }
    }
}
