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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.RowRangeIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Complete row-id interval unions and physical locations of a manifest's Avro blocks. */
public final class ManifestRowIdIndex {
    public static final String SUFFIX = ".row-id-index";
    private static final Logger LOG = LoggerFactory.getLogger(ManifestRowIdIndex.class);
    private static final long MAGIC = 0x5041494d52494458L;
    private static final int HEADER_BYTES = 68;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_AVRO_HEADER = 1024 * 1024;

    private ManifestRowIdIndex() {}

    public static Path path(Path manifest) {
        return new Path(manifest.toString() + SUFFIX);
    }

    /** Independent read/write switches and construction/serialization bounds. */
    public static final class Settings {
        public final boolean write;
        public final boolean read;
        public final int maxRanges;
        public final int maxBytes;

        public Settings(Options options) {
            write = options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_WRITE);
            read = options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_READ);
            maxRanges = options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_RANGES);
            maxBytes = options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_BYTES);
            checkArgument(
                    maxRanges > 0 && maxRanges <= 1048576,
                    "manifest.row-id-index.max-ranges must be in [1, 1048576]");
            checkArgument(
                    maxBytes >= 128 && maxBytes <= 64 * 1024 * 1024,
                    "manifest.row-id-index.max-bytes must be in [128, 67108864]");
        }
    }

    /** Original file offset/length and zero-based manifest entry ordinal, not table row id. */
    public static final class Block {
        public final long offset;
        public final long length;
        public final long firstRecord;
        public final long recordCount;

        public Block(long offset, long length, long firstRecord, long recordCount) {
            this.offset = offset;
            this.length = length;
            this.firstRecord = firstRecord;
            this.recordCount = recordCount;
        }
    }

    /** Selected blocks in original file order. Empty means the manifest can be excluded. */
    public static final class Selection {
        private final byte[] header;
        private final List<Block> blocks;

        private Selection(byte[] header, List<Block> blocks) {
            this.header = header;
            this.blocks = Collections.unmodifiableList(blocks);
        }

        public List<Block> blocks() {
            return blocks;
        }
    }

    /** Bounded range union. No row-id enumeration, even for a range ending at Long.MAX_VALUE. */
    public static final class Builder {
        private final Settings settings;
        private final TreeMap<Long, Long> ranges = new TreeMap<>();
        private final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        private final DataOutputStream out = new DataOutputStream(payload);
        private final int countPosition;
        private boolean complete;
        private long nextOffset;
        private long nextRecord;
        private Block current;
        private long entriesInBlock;
        private int blocks;
        private int rangeCount;

        public Builder(Settings settings, @Nullable byte[] header) throws IOException {
            this.settings = settings;
            this.complete =
                    header != null
                            && header.length <= MAX_AVRO_HEADER
                            && header.length + HEADER_BYTES + DIGEST_BYTES + 8 <= settings.maxBytes;
            countPosition = complete ? 4 + header.length : 0;
            if (complete) {
                out.writeInt(header.length);
                out.write(header);
                out.writeInt(0);
                nextOffset = header.length;
            }
        }

        public boolean complete() {
            return complete;
        }

        private void disable(String reason) {
            complete = false;
            ranges.clear();
            payload.reset();
            LOG.debug("Omitting manifest row-id block index: {}", reason);
        }

        public void beginBlock(long offset, long length, long records) throws IOException {
            if (!complete) {
                return;
            }
            require(current == null && offset == nextOffset && length > 0 && records > 0);
            current = new Block(offset, length, nextRecord, records);
            entriesInBlock = 0;
        }

        public void add(@Nullable Long first, long count) {
            if (!complete) {
                return;
            }
            if (current == null) {
                throw new IllegalStateException("No current Avro block");
            }
            entriesInBlock++;
            if (first == null || first < 0 || count <= 0 || count - 1 > Long.MAX_VALUE - first) {
                disable("unknown or invalid row-id coverage");
                return;
            }
            long start = first;
            long end = first + (count - 1);
            Map.Entry<Long, Long> before = ranges.floorEntry(start);
            if (before != null && before.getValue() >= start - 1) {
                start = before.getKey();
                end = Math.max(end, before.getValue());
                ranges.remove(before.getKey());
            }
            Map.Entry<Long, Long> next;
            while ((next = ranges.ceilingEntry(start)) != null
                    && (next.getKey() <= end || next.getKey() - end == 1)) {
                end = Math.max(end, next.getValue());
                ranges.remove(next.getKey());
            }
            if (rangeCount + ranges.size() >= settings.maxRanges) {
                disable("range budget exceeded");
                return;
            }
            ranges.put(start, end);
        }

        public void endBlock() throws IOException {
            if (!complete) {
                return;
            }
            require(current != null && entriesInBlock == current.recordCount && !ranges.isEmpty());
            if (HEADER_BYTES + DIGEST_BYTES + (long) payload.size() + 36 + 16L * ranges.size()
                    > settings.maxBytes) {
                disable("serialized byte budget exceeded");
                return;
            }
            out.writeLong(current.offset);
            out.writeLong(current.length);
            out.writeLong(current.firstRecord);
            out.writeLong(current.recordCount);
            out.writeInt(ranges.size());
            for (Map.Entry<Long, Long> range : ranges.entrySet()) {
                out.writeLong(range.getKey());
                out.writeLong(range.getValue());
            }
            nextOffset = Math.addExact(current.offset, current.length);
            nextRecord = Math.addExact(current.firstRecord, current.recordCount);
            rangeCount += ranges.size();
            blocks++;
            ranges.clear();
            current = null;
        }

        @Nullable
        public byte[] serialize(String name, long fileSize, long entryCount) throws IOException {
            if (!complete) {
                return null;
            }
            require(current == null && nextOffset == fileSize && nextRecord == entryCount);
            byte[] body = payload.toByteArray();
            ByteBuffer.wrap(body).putInt(countPosition, blocks);
            ByteArrayOutputStream buffer =
                    new ByteArrayOutputStream(HEADER_BYTES + body.length + DIGEST_BYTES);
            DataOutputStream envelope = new DataOutputStream(buffer);
            envelope.writeLong(MAGIC);
            envelope.writeShort(2);
            envelope.writeShort(2); // sorted inclusive interval unions per Avro block
            envelope.writeInt(1); // COMPLETE; all other bits reserved
            envelope.write(digest(name.getBytes(StandardCharsets.UTF_8)));
            envelope.writeLong(fileSize);
            envelope.writeLong(entryCount);
            envelope.writeInt(body.length);
            envelope.write(body);
            envelope.write(digest(buffer.toByteArray()));
            return buffer.toByteArray();
        }
    }

    /** Rebuild from the final physical blocks, including raw-copy and encoded rewrite paths. */
    @Nullable
    public static byte[] build(FileIO io, Path path, long size, long records, Settings settings)
            throws IOException {
        try (ManifestAvroReader reader = new ManifestAvroReader(io.newInputStream(path))) {
            Builder builder = new Builder(settings, reader.headerBytes());
            ProjectedManifestEntry.Projection projection =
                    ProjectedManifestEntry.ROW_RANGE_PROJECTION;
            ProjectedManifestEntry entry = projection.createEntry();
            while (builder.complete() && reader.hasNext()) {
                ManifestAvroReader.RawBlock block = reader.next();
                builder.beginBlock(reader.blockOffset(), reader.blockLength(), block.recordCount());
                ManifestAvroReader.RowIterator rows = block.toRows(projection.projectedType());
                while (builder.complete() && rows.hasNext()) {
                    entry.replace(rows.next());
                    builder.add(entry.file().firstRowId(), entry.file().rowCount());
                }
                builder.endBlock();
            }
            return builder.serialize(path.getName(), size, records);
        }
    }

    /** Validate the complete index before allowing any negative decision. */
    public static Selection select(
            byte[] data, ManifestFileMeta manifest, RowRangeIndex query, Settings settings)
            throws IOException {
        require(data.length >= 128 && data.length <= settings.maxBytes);
        int checksumOffset = data.length - DIGEST_BYTES;
        require(
                MessageDigest.isEqual(
                        digest(Arrays.copyOf(data, checksumOffset)),
                        Arrays.copyOfRange(data, checksumOffset, data.length)));
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data, 0, checksumOffset));
        require(
                in.readLong() == MAGIC
                        && in.readUnsignedShort() == 2
                        && in.readUnsignedShort() == 2
                        && in.readInt() == 1);
        byte[] nameHash = new byte[DIGEST_BYTES];
        in.readFully(nameHash);
        require(
                MessageDigest.isEqual(
                        nameHash, digest(manifest.fileName().getBytes(StandardCharsets.UTF_8))));
        require(in.readLong() == manifest.fileSize());
        long entries = Math.addExact(manifest.numAddedFiles(), manifest.numDeletedFiles());
        require(in.readLong() == entries && in.readInt() == checksumOffset - HEADER_BYTES);
        int headerLength = in.readInt();
        require(
                headerLength >= 21
                        && headerLength <= MAX_AVRO_HEADER
                        && headerLength <= in.available() - 4);
        byte[] header = new byte[headerLength];
        in.readFully(header);
        require(header[0] == 'O' && header[1] == 'b' && header[2] == 'j' && header[3] == 1);
        int blocks = in.readInt();
        require(blocks >= 0 && blocks <= in.available() / 52);
        long nextOffset = headerLength;
        long nextRecord = 0;
        int totalRanges = 0;
        List<Block> selected = new ArrayList<>();
        ByteBuffer view = ByteBuffer.wrap(data);
        for (int i = 0; i < blocks; i++) {
            long offset = in.readLong();
            long length = in.readLong();
            long first = in.readLong();
            long count = in.readLong();
            int ranges = in.readInt();
            require(offset == nextOffset && length > 0 && length <= manifest.fileSize() - offset);
            require(first == nextRecord && count > 0 && count <= entries - first);
            require(
                    ranges > 0
                            && ranges <= settings.maxRanges - totalRanges
                            && ranges <= in.available() / 16);
            totalRanges += ranges;
            int rangesEnd = checksumOffset - in.available() + 16 * ranges;
            long minRowId = in.readLong();
            long firstEnd = in.readLong();
            // Sorted intervals already encode the envelope. Peek at the final endpoint without
            // adding redundant fields to the format or materializing the interval list.
            long maxRowId = ranges == 1 ? firstEnd : view.getLong(rangesEnd - Long.BYTES);
            require(minRowId >= 0 && firstEnd >= minRowId && maxRowId >= firstEnd);
            boolean candidate = query.intersects(minRowId, maxRowId);
            boolean hit = candidate && (ranges == 1 || query.intersects(minRowId, firstEnd));
            long previousEnd = firstEnd;
            for (int j = 1; j < ranges; j++) {
                long start = in.readLong();
                long end = in.readLong();
                // Validate even rejected blocks: a checksummed but malformed interval list must
                // still cause a conservative fallback, not a false negative from its envelope.
                require(start >= 0 && end >= start && start > previousEnd);
                previousEnd = end;
                if (candidate && !hit) {
                    hit = query.intersects(start, end);
                }
            }
            if (hit) {
                selected.add(new Block(offset, length, first, count));
            }
            nextOffset = offset + length;
            nextRecord = first + count;
        }
        require(in.available() == 0 && nextOffset == manifest.fileSize() && nextRecord == entries);
        return new Selection(header, selected);
    }

    /** One bounded GET attempt, without a preceding HEAD. Null means read the original manifest. */
    @Nullable
    public static Selection read(
            FileIO io,
            Path path,
            ManifestFileMeta manifest,
            RowRangeIndex query,
            Settings settings) {
        if (manifest.indexFileName() == null) {
            return null;
        }
        try {
            byte[] data;
            try (InputStream in =
                    io.newInputStream(new Path(path.getParent(), manifest.indexFileName()))) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int n;
                while ((n =
                                in.read(
                                        buffer,
                                        0,
                                        Math.min(
                                                buffer.length, settings.maxBytes + 1 - out.size())))
                        != -1) {
                    out.write(buffer, 0, n);
                    require(out.size() <= settings.maxBytes);
                }
                data = out.toByteArray();
            }
            return select(data, manifest, query, settings);
        } catch (CancellationException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause instanceof CancellationException) {
                    throw (CancellationException) cause;
                }
                if (cause instanceof InterruptedException
                        || cause instanceof ClosedByInterruptException
                        || (cause instanceof InterruptedIOException
                                && !(cause instanceof SocketTimeoutException))) {
                    Thread.currentThread().interrupt();
                    throw interrupted(failure);
                }
            }
            if (Thread.currentThread().isInterrupted()) {
                throw interrupted(failure);
            }
            LOG.debug("Cannot use row-id block index for {}; reading manifest", path, failure);
            return null;
        }
    }

    private static UncheckedIOException interrupted(Throwable failure) {
        InterruptedIOException interrupted =
                new InterruptedIOException("Interrupted reading row-id index");
        interrupted.initCause(failure);
        return new UncheckedIOException(interrupted);
    }

    static InputStream openManifest(FileIO io, Path path, @Nullable Selection selected)
            throws IOException {
        SeekableInputStream input = io.newInputStream(path);
        return selected == null ? input : new SelectedBlockInput(input, selected);
    }

    /** An OCF stream comprising the original header and selected complete compressed blocks. */
    private static final class SelectedBlockInput extends InputStream {
        private final SeekableInputStream input;
        private final Selection selected;
        private int headerPosition;
        private int blockPosition;
        private long remaining;
        private long previousEnd = -1;

        private SelectedBlockInput(SeekableInputStream input, Selection selected) {
            this.input = input;
            this.selected = selected;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (headerPosition < selected.header.length) {
                int n = Math.min(length, selected.header.length - headerPosition);
                System.arraycopy(selected.header, headerPosition, bytes, offset, n);
                headerPosition += n;
                return n;
            }
            if (remaining == 0) {
                if (blockPosition == selected.blocks.size()) {
                    return -1;
                }
                Block block = selected.blocks.get(blockPosition++);
                if (block.offset != previousEnd) {
                    input.seek(block.offset);
                }
                previousEnd = block.offset + block.length;
                remaining = block.length;
            }
            int n = input.read(bytes, offset, (int) Math.min(length, remaining));
            if (n < 0) {
                throw new EOFException("Truncated manifest block");
            }
            remaining -= n;
            return n;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static void require(boolean valid) throws IOException {
        if (!valid) {
            throw new IOException(
                    "Invalid, unsupported, mismatched or over-budget manifest row-id block index");
        }
    }

    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
