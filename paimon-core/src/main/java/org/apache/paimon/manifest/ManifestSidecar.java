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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SerializationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiPredicate;

/** Independently usable partition, row-id and bucket coverage for each manifest block. */
public final class ManifestSidecar {
    public static final String SUFFIX = ".avro.sidecar";
    private static final Logger LOG = LoggerFactory.getLogger(ManifestSidecar.class);
    private static final long MAGIC = 0x5041494d53434152L;
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_BYTES = 60;
    private static final int BLOCK_BYTES = 27;
    private static final byte[] EMPTY = new byte[0];
    private static final int DIGEST_BYTES = 32;
    private static final int READ_BUFFER_BYTES = 1024 * 1024;
    private static final ProjectedManifestEntry.Projection BLOCK_INDEX_PROJECTION =
            createBlockIndexProjection();

    private ManifestSidecar() {}

    public static Path path(Path manifest) {
        return new Path(manifest.toString() + SUFFIX);
    }

    @Nullable
    public static String fileName(ManifestFileMeta manifest) {
        if (manifest.extraFiles() != null) {
            for (String extraFile : manifest.extraFiles()) {
                if (extraFile.endsWith(SUFFIX)) {
                    return extraFile;
                }
            }
        }
        return null;
    }

    /** Construction bounds and independently enabled payloads, supplied by the caller. */
    public static final class Settings {
        public final int maxBytes;
        public final boolean partitionEnabled;
        public final boolean rowIdEnabled;
        public final boolean bucketEnabled;

        public Settings(
                long maxBytes,
                boolean partitionEnabled,
                boolean rowIdEnabled,
                boolean bucketEnabled) {
            if (maxBytes < 0) {
                throw new IllegalArgumentException(
                        "Manifest sidecar byte budget must be nonnegative");
            }
            // A sidecar fits in one byte array; reserve one byte for the overflow probe.
            this.maxBytes = (int) Math.min(maxBytes, Integer.MAX_VALUE - 1L);
            this.partitionEnabled = partitionEnabled;
            this.rowIdEnabled = rowIdEnabled;
            this.bucketEnabled = bucketEnabled;
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

    /**
     * Builds complete block descriptors even when either optional dimension becomes unavailable.
     */
    public static final class Builder {
        private final Settings settings;
        private final byte[] header;
        private final TreeMap<Long, Long> ranges = new TreeMap<>();
        private final Map<ByteBuffer, Integer> dictionary = new LinkedHashMap<>();
        private final TreeSet<Integer> partitionIds = new TreeSet<>();
        private final TreeSet<Long> bucketPairs = new TreeSet<>();
        private final List<IndexedBlock> blocks = new ArrayList<>();
        private boolean complete;
        private long nextOffset;
        private long nextRecord;
        private Block current;
        private long entriesInBlock;
        private boolean rowAvailable;
        private boolean partitionAvailable;
        private boolean bucketAvailable;
        private boolean coarse;
        private long min;
        private long max;
        private int dictionaryBytes;
        private int optionalBytes;

        public Builder(Settings settings, @Nullable byte[] header) {
            this.settings = settings;
            this.header = header;
            complete =
                    header != null
                            && HEADER_BYTES + DIGEST_BYTES + 12L + header.length
                                    <= settings.maxBytes;
            nextOffset = header == null ? 0 : header.length;
        }

        public boolean complete() {
            return complete;
        }

        public void beginBlock(long offset, long length, long records) throws IOException {
            if (!complete) {
                return;
            }
            require(current == null && offset == nextOffset && length > 0 && records > 0);
            // Optional payloads can be discarded later, but descriptors must never be truncated.
            if (HEADER_BYTES
                            + DIGEST_BYTES
                            + 12L
                            + header.length
                            + (blocks.size() + 1L) * BLOCK_BYTES
                    > settings.maxBytes) {
                complete = false;
                blocks.clear();
                dictionary.clear();
                return;
            }
            current = new Block(offset, length, nextRecord, records);
            entriesInBlock = 0;
            rowAvailable = settings.rowIdEnabled;
            partitionAvailable = settings.partitionEnabled;
            bucketAvailable = settings.bucketEnabled;
            coarse = false;
            min = Long.MAX_VALUE;
            max = -1;
            ranges.clear();
            partitionIds.clear();
            bucketPairs.clear();
        }

        @VisibleForTesting
        public void add(@Nullable Long first, long count) {
            add(first, count, null);
        }

        @VisibleForTesting
        public void add(@Nullable Long first, long count, @Nullable byte[] partition) {
            add(first, count, partition, null, null);
        }

        public void add(
                @Nullable Long first,
                long count,
                @Nullable byte[] partition,
                @Nullable Integer bucket,
                @Nullable Integer totalBuckets) {
            if (!complete) {
                return;
            }
            if (current == null) {
                throw new IllegalStateException("No current Avro block");
            }
            entriesInBlock++;
            addPartition(partition);
            addBucket(bucket, totalBuckets);
            if (!rowAvailable) {
                return;
            }
            if (first == null || first < 0 || count <= 0 || count - 1 > Long.MAX_VALUE - first) {
                rowAvailable = false;
                ranges.clear();
                return;
            }
            long end = first + (count - 1);
            min = Math.min(min, first);
            max = Math.max(max, end);
            // Keep checking subsequent entries, including missing row IDs, after coarsening.
            if (coarse) {
                return;
            }
            long start = first;
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
            if (8L + 16L * (ranges.size() + 1L) > settings.maxBytes - optionalBytes) {
                coarse = true;
                ranges.clear();
            } else {
                ranges.put(start, end);
            }
        }

        private void addBucket(@Nullable Integer bucket, @Nullable Integer totalBuckets) {
            if (!bucketAvailable) {
                return;
            }
            if (bucket == null
                    || totalBuckets == null
                    || bucket < 0
                    || totalBuckets <= 0
                    || bucket >= totalBuckets) {
                bucketAvailable = false;
                bucketPairs.clear();
                return;
            }
            long pair = ((long) bucket << 32) | totalBuckets;
            if (!bucketPairs.contains(pair)
                    && 8L + 8L * (bucketPairs.size() + 1L) > settings.maxBytes - optionalBytes) {
                bucketAvailable = false;
                bucketPairs.clear();
            } else {
                bucketPairs.add(pair);
            }
        }

        private void addPartition(@Nullable byte[] bytes) {
            if (!partitionAvailable) {
                return;
            }
            if (bytes == null) {
                partitionAvailable = false;
                partitionIds.clear();
                return;
            }
            Integer id = dictionary.get(ByteBuffer.wrap(bytes));
            if (id == null) {
                if (bytes.length + 4L > settings.maxBytes - dictionaryBytes) {
                    partitionAvailable = false;
                    partitionIds.clear();
                    return;
                }
                id = dictionary.size();
                dictionary.put(ByteBuffer.wrap(bytes.clone()), id);
                dictionaryBytes += 4 + bytes.length;
            }
            partitionIds.add(id);
        }

        public void endBlock() throws IOException {
            if (!complete) {
                return;
            }
            require(current != null && entriesInBlock == current.recordCount);
            byte[] rowPayload = EMPTY;
            byte[] partitionPayload = EMPTY;
            if (rowAvailable) {
                if (coarse || 8L + 16L * ranges.size() > settings.maxBytes - optionalBytes) {
                    ranges.clear();
                    ranges.put(min, max);
                }
                if (8L + 16L * ranges.size() <= settings.maxBytes - optionalBytes) {
                    ByteBuffer out = ByteBuffer.allocate(4 + 16 * ranges.size());
                    out.putInt(ranges.size());
                    for (Map.Entry<Long, Long> range : ranges.entrySet()) {
                        out.putLong(range.getKey()).putLong(range.getValue());
                    }
                    rowPayload = out.array();
                    optionalBytes += payloadSize(rowPayload);
                }
            }
            if (partitionAvailable
                    && 8L + 4L * partitionIds.size() <= settings.maxBytes - optionalBytes) {
                ByteBuffer out = ByteBuffer.allocate(4 + 4 * partitionIds.size());
                out.putInt(partitionIds.size());
                for (int id : partitionIds) {
                    out.putInt(id);
                }
                partitionPayload = out.array();
                optionalBytes += payloadSize(partitionPayload);
            }
            byte[] bucketPayload = EMPTY;
            if (bucketAvailable
                    && 8L + 8L * bucketPairs.size() <= settings.maxBytes - optionalBytes) {
                ByteBuffer out = ByteBuffer.allocate(4 + 8 * bucketPairs.size());
                out.putInt(bucketPairs.size());
                for (long pair : bucketPairs) {
                    out.putInt((int) (pair >>> 32)).putInt((int) pair);
                }
                bucketPayload = out.array();
                optionalBytes += payloadSize(bucketPayload);
            }
            blocks.add(new IndexedBlock(current, partitionPayload, rowPayload, bucketPayload));
            nextOffset = Math.addExact(current.offset, current.length);
            nextRecord = Math.addExact(current.firstRecord, current.recordCount);
            ranges.clear();
            partitionIds.clear();
            bucketPairs.clear();
            current = null;
        }

        @Nullable
        public byte[] serialize(String name, long fileSize, long entryCount) throws IOException {
            if (!complete) {
                return null;
            }
            require(current == null && nextOffset == fileSize && nextRecord == entryCount);
            long size =
                    HEADER_BYTES
                            + DIGEST_BYTES
                            + 12L
                            + header.length
                            + dictionaryBytes
                            + blocks.size() * (long) BLOCK_BYTES
                            + optionalBytes;
            // Give directory growth priority over optional coverage. Never remove a descriptor.
            for (IndexedBlock block : blocks) {
                if (size <= settings.maxBytes) {
                    break;
                }
                size -= payloadSize(block.rowIds);
                optionalBytes -= payloadSize(block.rowIds);
                block.rowIds = EMPTY;
            }
            for (IndexedBlock block : blocks) {
                if (size <= settings.maxBytes) {
                    break;
                }
                size -= payloadSize(block.buckets);
                optionalBytes -= payloadSize(block.buckets);
                block.buckets = EMPTY;
            }
            if (size > settings.maxBytes) {
                size -= dictionaryBytes;
                dictionaryBytes = 0;
                dictionary.clear();
                for (IndexedBlock block : blocks) {
                    size -= payloadSize(block.partitions);
                    optionalBytes -= payloadSize(block.partitions);
                    block.partitions = EMPTY;
                }
            }
            require(size <= settings.maxBytes);
            ByteArrayOutputStream buffer = new ByteArrayOutputStream((int) size);
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeLong(MAGIC);
            out.writeInt(FORMAT_VERSION);
            out.write(digest(name.getBytes(StandardCharsets.UTF_8)));
            out.writeLong(fileSize);
            out.writeLong(entryCount);
            out.writeInt(header.length);
            out.write(header);
            out.writeInt(dictionary.size());
            for (ByteBuffer bytes : dictionary.keySet()) {
                out.writeInt(bytes.remaining());
                out.write(bytes.array());
            }
            out.writeInt(blocks.size());
            for (IndexedBlock block : blocks) {
                out.writeLong(block.block.offset);
                out.writeLong(block.block.length);
                out.writeLong(block.block.recordCount);
                writePayload(out, block.partitions);
                writePayload(out, block.rowIds);
                writePayload(out, block.buckets);
            }
            out.write(digest(buffer.toByteArray()));
            return buffer.toByteArray();
        }

        private static int payloadSize(byte[] payload) {
            return payload.length == 0 ? 0 : Integer.BYTES + payload.length;
        }

        private static void writePayload(DataOutputStream out, byte[] payload) throws IOException {
            out.writeByte(payload.length == 0 ? 0 : 1);
            if (payload.length > 0) {
                out.writeInt(payload.length);
                out.write(payload);
            }
        }
    }

    private static final class IndexedBlock {
        private final Block block;
        private byte[] partitions;
        private byte[] rowIds;
        private byte[] buckets;

        private IndexedBlock(Block block, byte[] partitions, byte[] rowIds, byte[] buckets) {
            this.block = block;
            this.partitions = partitions;
            this.rowIds = rowIds;
            this.buckets = buckets;
        }
    }

    private static ProjectedManifestEntry.Projection createBlockIndexProjection() {
        List<DataField> fields =
                new ArrayList<>(
                        ProjectedManifestEntry.ROW_RANGE_PROJECTION.projectedType().getFields());
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.BUCKET));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.TOTAL_BUCKETS));
        return ProjectedManifestEntry.Projection.create(new RowType(false, fields));
    }

    /** Rebuild from the final physical blocks, including raw-copy and encoded rewrite paths. */
    @Nullable
    public static byte[] build(FileIO io, Path path, long size, long records, Settings settings)
            throws IOException {
        if (settings.maxBytes < 128) {
            return null;
        }
        try (ManifestAvroReader reader = new ManifestAvroReader(io.newInputStream(path))) {
            Builder builder = new Builder(settings, reader.headerBytes());
            ProjectedManifestEntry.Projection projection = BLOCK_INDEX_PROJECTION;
            ProjectedManifestEntry entry = projection.createEntry();
            while (builder.complete() && reader.hasNext()) {
                ManifestAvroReader.RawBlock block = reader.next();
                builder.beginBlock(reader.blockOffset(), reader.blockLength(), block.recordCount());
                ManifestAvroReader.RowIterator rows = block.toRows(projection.projectedType());
                while (builder.complete() && rows.hasNext()) {
                    entry.replace(rows.next());
                    builder.add(
                            settings.rowIdEnabled ? entry.file().firstRowId() : null,
                            settings.rowIdEnabled ? entry.file().rowCount() : 0,
                            settings.partitionEnabled ? entry.partitionBytes() : null,
                            settings.bucketEnabled ? entry.bucket() : null,
                            settings.bucketEnabled ? entry.totalBuckets() : null);
                }
                builder.endBlock();
            }
            return builder.serialize(path.getName(), size, records);
        }
    }

    /** Selects blocks using row-ID coverage. A null query retains every block after validation. */
    public static Selection select(
            byte[] data,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            Settings settings)
            throws IOException {
        return select(data, manifest, query, null, null, settings);
    }

    /** Validates framing and tests row ID, partition, then bucket coverage. */
    public static Selection select(
            byte[] data,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable RowType partitionType,
            Settings settings)
            throws IOException {
        return select(data, manifest, query, partitionFilter, partitionType, null, settings);
    }

    /**
     * Selects blocks using independent filters. The bucket predicate must conservatively test only
     * the bucket and recorded total bucket count; omit it if filtering requires an entry partition.
     */
    public static Selection select(
            byte[] data,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable RowType partitionType,
            @Nullable BiPredicate<Integer, Integer> bucketFilter,
            Settings settings)
            throws IOException {
        require(data.length >= 128 && data.length <= settings.maxBytes);
        int limit = data.length - DIGEST_BYTES;
        require(
                MessageDigest.isEqual(
                        digest(data, limit), Arrays.copyOfRange(data, limit, data.length)));
        ByteBuffer in = ByteBuffer.wrap(data, 0, limit).slice();
        require(in.getLong() == MAGIC);
        require(in.getInt() == FORMAT_VERSION);
        byte[] hash = new byte[DIGEST_BYTES];
        in.get(hash);
        require(
                MessageDigest.isEqual(
                        hash, digest(manifest.fileName().getBytes(StandardCharsets.UTF_8))));
        require(in.getLong() == manifest.fileSize());
        long entries = Math.addExact(manifest.numAddedFiles(), manifest.numDeletedFiles());
        require(in.getLong() == entries);
        int headerLength = in.getInt();
        require(headerLength >= 21 && headerLength <= in.remaining() - 8);
        byte[] header = new byte[headerLength];
        in.get(header);
        require(header[0] == 'O' && header[1] == 'b' && header[2] == 'j' && header[3] == 1);
        int partitions = in.getInt();
        require(partitions >= 0 && partitions <= in.remaining() / 16);
        boolean[] matches = partitionFilter == null ? null : new boolean[partitions];
        Set<ByteBuffer> unique = new java.util.HashSet<>();
        for (int id = 0; id < partitions; id++) {
            require(in.remaining() >= 4);
            int length = in.getInt();
            require(length >= 12 && length <= in.remaining());
            ByteBuffer encoded = in.slice();
            encoded.limit(length);
            int arity = encoded.getInt(0);
            require(arity >= 0 && 4L + ((arity + 71L) / 64) * 8 + arity * 8L <= length);
            require(partitionType == null || arity == partitionType.getFieldCount());
            require(unique.add(encoded.asReadOnlyBuffer()));
            if (partitionFilter != null) {
                byte[] bytes = new byte[length];
                encoded.get(bytes);
                BinaryRow partition = SerializationUtils.deserializeBinaryRow(bytes);
                matches[id] = partitionFilter.test(partition);
            }
            in.position(in.position() + length);
        }
        require(in.remaining() >= 4);
        int count = in.getInt();
        require(count >= 0 && count <= in.remaining() / BLOCK_BYTES);
        long nextOffset = headerLength;
        long firstRecord = 0;
        List<Block> selected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            require(in.remaining() >= BLOCK_BYTES);
            long offset = in.getLong();
            long length = in.getLong();
            long records = in.getLong();
            require(offset == nextOffset && length > 0 && length <= manifest.fileSize() - offset);
            require(records > 0 && records <= entries - firstRecord);
            ByteBuffer partitionPayload = payload(in, Integer.BYTES);
            ByteBuffer rowPayload = payload(in, 2 * Long.BYTES);
            ByteBuffer bucketPayload = payload(in, 2 * Integer.BYTES);
            long blockFirstRecord = firstRecord;
            nextOffset = offset + length;
            firstRecord += records;

            if (query != null && rowPayload != null) {
                boolean singleRange = rowPayload.remaining() == 2 * Long.BYTES;
                long min = rowPayload.getLong();
                long firstEnd = rowPayload.getLong();
                long max =
                        singleRange
                                ? firstEnd
                                : rowPayload.getLong(rowPayload.limit() - Long.BYTES);
                require(min >= 0 && firstEnd >= min && max >= firstEnd);
                if (!query.intersects(min, max)) {
                    continue;
                }
                boolean rowHit = singleRange || query.intersects(min, firstEnd);
                long previous = firstEnd;
                while (!rowHit && rowPayload.hasRemaining()) {
                    long rangeStart = rowPayload.getLong();
                    long rangeEnd = rowPayload.getLong();
                    require(rangeStart >= 0 && rangeEnd >= rangeStart && rangeStart > previous);
                    previous = rangeEnd;
                    rowHit = query.intersects(rangeStart, rangeEnd);
                }
                if (!rowHit) {
                    continue;
                }
            }

            if (partitionFilter != null && partitionPayload != null) {
                boolean partitionHit = false;
                int previous = -1;
                while (!partitionHit && partitionPayload.hasRemaining()) {
                    int id = partitionPayload.getInt();
                    require(id > previous && id < partitions);
                    previous = id;
                    partitionHit = matches[id];
                }
                if (!partitionHit) {
                    continue;
                }
            }

            if (bucketFilter != null && bucketPayload != null) {
                boolean bucketHit = false;
                long previous = -1;
                while (!bucketHit && bucketPayload.hasRemaining()) {
                    int bucket = bucketPayload.getInt();
                    int totalBuckets = bucketPayload.getInt();
                    require(bucket >= 0 && totalBuckets > bucket);
                    long pair = ((long) bucket << 32) | totalBuckets;
                    require(pair > previous);
                    previous = pair;
                    bucketHit = bucketFilter.test(bucket, totalBuckets);
                }
                if (!bucketHit) {
                    continue;
                }
            }
            selected.add(new Block(offset, length, blockFirstRecord, records));
        }
        require(!in.hasRemaining() && nextOffset == manifest.fileSize() && firstRecord == entries);
        return new Selection(header, selected);
    }

    /** Reads framing and exposes known payload elements without decoding their contents. */
    @Nullable
    private static ByteBuffer payload(ByteBuffer in, int elementBytes) throws IOException {
        require(in.hasRemaining());
        int encoding = Byte.toUnsignedInt(in.get());
        if (encoding == 0) {
            return null;
        }
        require(in.remaining() >= Integer.BYTES);
        int length = in.getInt();
        require(length >= 0 && length <= in.remaining());
        ByteBuffer result = in.slice();
        result.limit(length);
        in.position(in.position() + length);
        if (encoding != 1) {
            return null;
        }
        require(result.remaining() >= Integer.BYTES);
        int count = result.getInt();
        require(count > 0 && result.remaining() == (long) elementBytes * count);
        return result;
    }

    /** Bounded, bulk sidecar reads. Null means read the original manifest. */
    @Nullable
    public static Selection read(
            FileIO io,
            Path path,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            Settings settings) {
        return read(io, path, manifest, query, null, null, settings);
    }

    @Nullable
    public static Selection read(
            FileIO io,
            Path path,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable RowType partitionType,
            Settings settings) {
        return read(
                io, path, manifest, query, partitionFilter, partitionType, null, settings, null);
    }

    @Nullable
    public static Selection read(
            FileIO io,
            Path path,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable RowType partitionType,
            @Nullable BiPredicate<Integer, Integer> bucketFilter,
            Settings settings,
            @Nullable SegmentsCache<Object> cache) {
        String sidecarFileName = fileName(manifest);
        if (sidecarFileName == null || settings.maxBytes < 128) {
            return null;
        }
        try {
            Path sidecarPath = new Path(path.getParent(), sidecarFileName);
            Segments cached = cache == null ? null : cache.getIfPresents(sidecarPath);
            boolean cacheHit = cached instanceof ManifestSidecarSegment;
            byte[] data =
                    cacheHit
                            ? ((ManifestSidecarSegment) cached).bytes()
                            : readBytes(io, sidecarPath, settings.maxBytes);
            Selection selection =
                    select(
                            data,
                            manifest,
                            query,
                            partitionFilter,
                            partitionType,
                            bucketFilter,
                            settings);
            if (cache != null && !cacheHit && data.length <= cache.maxElementSize()) {
                cache.put(sidecarPath, new ManifestSidecarSegment(data));
            }
            return selection;
        } catch (IOException failure) {
            if (Thread.currentThread().isInterrupted()) {
                throw new UncheckedIOException(failure);
            }
            LOG.debug("Cannot use manifest sidecar for {}; reading manifest", path, failure);
            return null;
        }
    }

    /** Complete sidecar bytes stored in the shared manifest cache. */
    static final class ManifestSidecarSegment implements Segments {
        private final byte[] bytes;

        public ManifestSidecarSegment(byte[] bytes) {
            this.bytes = bytes;
        }

        public byte[] bytes() {
            return bytes;
        }

        @Override
        public long totalMemorySize() {
            return bytes.length;
        }
    }

    private static byte[] readBytes(FileIO io, Path path, int maxBytes) throws IOException {
        try (InputStream in = io.newInputStream(path)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[Math.min(READ_BUFFER_BYTES, maxBytes + 1)];
            int n;
            while ((n = in.read(buffer, 0, Math.min(buffer.length, maxBytes + 1 - out.size())))
                    != -1) {
                require(n <= maxBytes - out.size());
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        }
    }

    static InputStream openManifest(FileIO io, Path path, @Nullable Selection selected)
            throws IOException {
        return openManifest(io, path, selected, null);
    }

    static InputStream openManifest(
            FileIO io,
            Path path,
            @Nullable Selection selected,
            @Nullable SegmentsCache<Object> cache)
            throws IOException {
        return selected == null
                ? io.newInputStream(path)
                : new SelectedBlockInput(io, path, selected, cache);
    }

    /** Separates physical byte ranges from whole-file cache keys. */
    static final class BlockCacheKey {
        private final Path path;
        private final long offset;
        private final long length;

        BlockCacheKey(Path path, long offset, long length) {
            this.path = Objects.requireNonNull(path);
            this.offset = offset;
            this.length = length;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BlockCacheKey)) {
                return false;
            }
            BlockCacheKey that = (BlockCacheKey) other;
            return path.equals(that.path) && offset == that.offset && length == that.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, offset, length);
        }
    }

    /** Complete encoded Avro blocks, distinct from cached manifest entries and sidecar bytes. */
    private static final class ManifestBlockSegment implements Segments {
        private final byte[] bytes;

        private ManifestBlockSegment(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long totalMemorySize() {
            return bytes.length;
        }
    }

    /** An OCF stream comprising the original header and selected complete compressed blocks. */
    private static final class SelectedBlockInput extends InputStream {
        private final FileIO io;
        private final Path path;
        private final Selection selected;
        @Nullable private final SegmentsCache<Object> cache;
        @Nullable private SeekableInputStream input;
        private boolean closed;
        private int headerPosition;
        private int blockPosition;
        private long remaining;
        private byte[] buffer;
        private int bufferPosition;
        private int bufferLimit;

        private SelectedBlockInput(
                FileIO io, Path path, Selection selected, @Nullable SegmentsCache<Object> cache) {
            this.io = io;
            this.path = path;
            this.selected = selected;
            this.cache = cache;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            if (headerPosition < selected.header.length) {
                return selected.header[headerPosition++] & 255;
            }
            return fillBuffer() ? buffer[bufferPosition++] & 255 : -1;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            ensureOpen();
            if (length == 0) {
                return 0;
            }
            if (headerPosition < selected.header.length) {
                int n = Math.min(length, selected.header.length - headerPosition);
                System.arraycopy(selected.header, headerPosition, bytes, offset, n);
                headerPosition += n;
                return n;
            }
            if (!fillBuffer()) {
                return -1;
            }
            int copied = Math.min(length, bufferLimit - bufferPosition);
            System.arraycopy(buffer, bufferPosition, bytes, offset, copied);
            bufferPosition += copied;
            return copied;
        }

        private boolean fillBuffer() throws IOException {
            if (bufferPosition < bufferLimit) {
                return true;
            }
            if (remaining == 0) {
                if (blockPosition == selected.blocks.size()) {
                    return false;
                }
                Block next = selected.blocks.get(blockPosition);
                if (cache != null && next.length <= cache.maxElementSize()) {
                    readCachedBlocks(next);
                    return true;
                }
                Block block = selected.blocks.get(blockPosition++);
                long end = block.offset + block.length;
                while (blockPosition < selected.blocks.size()
                        && selected.blocks.get(blockPosition).offset == end
                        && (cache == null
                                || selected.blocks.get(blockPosition).length
                                        > cache.maxElementSize())) {
                    end += selected.blocks.get(blockPosition++).length;
                }
                seekInput(block.offset);
                remaining = end - block.offset;
            }
            int requested = (int) Math.min(READ_BUFFER_BYTES, remaining);
            // A previous buffer may be shared with other readers through the block cache.
            if (cache != null || buffer == null || buffer.length < requested) {
                buffer = new byte[requested];
            }
            bufferPosition = 0;
            bufferLimit = 0;
            readFully(buffer, requested);
            bufferLimit = requested;
            remaining -= requested;
            return true;
        }

        private void readCachedBlocks(Block first) throws IOException {
            byte[] cached = cachedBlock(first);
            if (cached != null) {
                blockPosition++;
                buffer = cached;
            } else {
                int firstPosition = blockPosition++;
                long end = first.offset + first.length;
                while (blockPosition < selected.blocks.size()) {
                    Block next = selected.blocks.get(blockPosition);
                    if (next.offset != end
                            || next.length > cache.maxElementSize()
                            || end - first.offset + next.length > READ_BUFFER_BYTES
                            || cachedBlock(next) != null) {
                        break;
                    }
                    end += next.length;
                    blockPosition++;
                }

                byte[] bytes = new byte[(int) (end - first.offset)];
                seekInput(first.offset);
                readFully(bytes, bytes.length);
                // Publish only complete reads, and use individual block keys so overlapping
                // selections can share data even when their coalesced read spans differ.
                int offset = 0;
                for (int i = firstPosition; i < blockPosition; i++) {
                    Block block = selected.blocks.get(i);
                    int length = (int) block.length;
                    byte[] blockBytes =
                            length == bytes.length
                                    ? bytes
                                    : Arrays.copyOfRange(bytes, offset, offset + length);
                    cache.put(
                            new BlockCacheKey(path, block.offset, block.length),
                            new ManifestBlockSegment(blockBytes));
                    offset += length;
                }
                buffer = bytes;
            }
            bufferPosition = 0;
            bufferLimit = buffer.length;
        }

        @Nullable
        private byte[] cachedBlock(Block block) {
            Segments cached =
                    cache.getIfPresents(new BlockCacheKey(path, block.offset, block.length));
            if (cached instanceof ManifestBlockSegment) {
                byte[] bytes = ((ManifestBlockSegment) cached).bytes;
                if (bytes.length == block.length) {
                    return bytes;
                }
            }
            return null;
        }

        private void seekInput(long offset) throws IOException {
            if (input == null) {
                input = io.newInputStream(path);
            }
            input.seek(offset);
        }

        private void readFully(byte[] bytes, int length) throws IOException {
            int position = 0;
            while (position < length) {
                int count =
                        input.read(bytes, position, Math.min(READ_BUFFER_BYTES, length - position));
                if (count < 0) {
                    throw new EOFException("Truncated manifest block");
                }
                position += count;
            }
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("Manifest stream is closed");
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                if (input != null) {
                    input.close();
                }
            }
        }
    }

    private static void require(boolean valid) throws IOException {
        if (!valid) {
            throw new IOException(
                    "Invalid, unsupported, mismatched or over-budget manifest sidecar");
        }
    }

    private static byte[] digest(byte[] bytes) {
        return digest(bytes, bytes.length);
    }

    private static byte[] digest(byte[] bytes, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, 0, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
