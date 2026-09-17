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
import org.apache.paimon.utils.DeltaVarintCodec;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.zip.CRC32;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/** Independently usable partition, row-id and bucket coverage for each manifest block. */
public final class ManifestSidecar {

    public static final String SUFFIX = ".avro.sidecar";
    private static final Logger LOG = LoggerFactory.getLogger(ManifestSidecar.class);
    private static final int MAGIC = 0x504d5343;
    private static final int FORMAT_VERSION = 1;
    private static final int MIN_HEADER_BYTES = 29;
    private static final int MIN_BLOCK_BYTES = 6;
    private static final byte[] EMPTY = new byte[0];
    private static final int CHECKSUM_BYTES = Integer.BYTES;
    private static final int SIDECAR_READ_BUFFER_BYTES = 1024 * 1024;
    private static final int BLOCK_READ_BUFFER_BYTES = 4 * 1024 * 1024;
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

        Selection(byte[] header, List<Block> blocks) {
            this.header = header;
            this.blocks = Collections.unmodifiableList(new ArrayList<>(blocks));
        }

        byte[] header() {
            return header;
        }

        public List<Block> blocks() {
            return blocks;
        }
    }

    /** Builds a complete block directory with independently available coverage. */
    public static final class Builder {
        private final boolean rowIdEnabled;
        private final boolean bucketEnabled;
        private final byte[] header;
        private final TreeMap<Long, Long> ranges = new TreeMap<>();
        private final Map<ByteBuffer, Integer> dictionary = new LinkedHashMap<>();
        private final TreeSet<Integer> partitionIds = new TreeSet<>();
        private final TreeSet<Long> bucketPairs = new TreeSet<>();
        private final List<IndexedBlock> blocks = new ArrayList<>();
        private long nextOffset;
        private long nextRecord;
        private Block current;
        private long entriesInBlock;
        private boolean rowAvailable;
        private boolean partitionAvailable;
        private boolean bucketAvailable;

        public Builder(byte[] header, boolean rowIdEnabled, boolean bucketEnabled) {
            this.rowIdEnabled = rowIdEnabled;
            this.bucketEnabled = bucketEnabled;
            this.header = Objects.requireNonNull(header);
            nextOffset = header.length;
        }

        public void beginBlock(long offset, long length, long records) throws IOException {
            require(current == null && offset == nextOffset && length > 0 && records > 0);
            current = new Block(offset, length, nextRecord, records);
            entriesInBlock = 0;
            rowAvailable = rowIdEnabled;
            partitionAvailable = true;
            bucketAvailable = bucketEnabled;
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
            ranges.put(start, end);
        }

        private void addBucket(@Nullable Integer bucket, @Nullable Integer totalBuckets) {
            if (!bucketAvailable) {
                return;
            }
            if (bucket == null || totalBuckets == null || bucket < 0 || totalBuckets <= bucket) {
                bucketAvailable = false;
                bucketPairs.clear();
                return;
            }
            bucketPairs.add(((long) bucket << 32) | totalBuckets);
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
                id = dictionary.size();
                dictionary.put(ByteBuffer.wrap(bytes.clone()), id);
            }
            partitionIds.add(id);
        }

        public void endBlock() throws IOException {
            require(current != null && entriesInBlock == current.recordCount);
            blocks.add(
                    new IndexedBlock(
                            current,
                            partitionAvailable
                                    ? encodeValues(partitionIds, partitionIds.size())
                                    : EMPTY,
                            rowAvailable ? encodeRanges() : EMPTY,
                            bucketAvailable ? encodeBuckets() : EMPTY));
            nextOffset = Math.addExact(current.offset, current.length);
            nextRecord = Math.addExact(current.firstRecord, current.recordCount);
            ranges.clear();
            partitionIds.clear();
            bucketPairs.clear();
            current = null;
        }

        private byte[] encodeRanges() throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            long min = ranges.firstKey();
            long max = ranges.lastEntry().getValue();
            out.writeLong(min);
            out.writeLong(max);
            // The envelope supplies the first start and last end. Encode only interior endpoints.
            DeltaVarintCodec.Writer encoder =
                    new DeltaVarintCodec.Writer(out, Math.multiplyExact(ranges.size() - 1, 2), min);
            int index = 0;
            for (Map.Entry<Long, Long> range : ranges.entrySet()) {
                if (index > 0) {
                    encoder.write(range.getKey());
                }
                if (++index < ranges.size()) {
                    encoder.write(range.getValue());
                }
            }
            return buffer.toByteArray();
        }

        private byte[] encodeBuckets() throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            DeltaVarintCodec.Writer buckets =
                    new DeltaVarintCodec.Writer(out, bucketPairs.size(), 0);
            for (long pair : bucketPairs) {
                buckets.write(pair >>> 32);
            }
            DeltaVarintCodec.Writer totals =
                    new DeltaVarintCodec.Writer(out, bucketPairs.size(), 0, true);
            for (long pair : bucketPairs) {
                totals.write((int) pair);
            }
            return buffer.toByteArray();
        }

        public byte[] serialize(long fileSize, long entryCount) throws IOException {
            require(current == null && nextOffset == fileSize && nextRecord == entryCount);
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt(MAGIC);
            encodeLong(out, FORMAT_VERSION);
            encodeLong(out, header.length);
            out.write(header);
            encodeLong(out, dictionary.size());
            for (ByteBuffer bytes : dictionary.keySet()) {
                encodeLong(out, bytes.remaining());
                out.write(bytes.array());
            }
            encodeLong(out, blocks.size());
            for (IndexedBlock block : blocks) {
                encodeLong(out, block.block.offset);
                encodeLong(out, block.block.length);
                encodeLong(out, block.block.recordCount);
                writePayload(out, block.partitions);
                writePayload(out, block.rowIds);
                writePayload(out, block.buckets);
            }
            CRC32 crc = new CRC32();
            crc.update(buffer.toByteArray());
            out.writeInt((int) crc.getValue());
            return buffer.toByteArray();
        }

        private static byte[] encodeValues(Iterable<? extends Number> values, int count)
                throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            DeltaVarintCodec.Writer encoder = new DeltaVarintCodec.Writer(out, count, 0);
            for (Number value : values) {
                encoder.write(value.longValue());
            }
            return buffer.toByteArray();
        }

        private static void writePayload(DataOutputStream out, byte[] payload) throws IOException {
            out.writeByte(payload.length == 0 ? 0 : 1);
            if (payload.length > 0) {
                encodeLong(out, payload.length);
                out.write(payload);
            }
        }
    }

    private static final class IndexedBlock {
        private final Block block;
        private final byte[] partitions;
        private final byte[] rowIds;
        private final byte[] buckets;

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

    /**
     * Rebuild from the final physical blocks, including raw-copy and encoded rewrite paths. Callers
     * decide whether to build a sidecar. Partition coverage is always generated.
     */
    public static byte[] build(
            FileIO io,
            Path path,
            long size,
            long records,
            boolean rowIdEnabled,
            boolean bucketEnabled)
            throws IOException {
        try (ManifestAvroReader reader = new ManifestAvroReader(io.newInputStream(path))) {
            Builder builder = new Builder(reader.headerBytes(), rowIdEnabled, bucketEnabled);
            ProjectedManifestEntry.Projection projection = BLOCK_INDEX_PROJECTION;
            ProjectedManifestEntry entry = projection.createEntry();
            while (reader.hasNext()) {
                ManifestAvroReader.RawBlock block = reader.next();
                builder.beginBlock(reader.blockOffset(), reader.blockLength(), block.recordCount());
                ManifestAvroReader.RowIterator rows = block.toRows(projection.projectedType());
                while (rows.hasNext()) {
                    entry.replace(rows.next());
                    builder.add(
                            rowIdEnabled ? entry.file().firstRowId() : null,
                            rowIdEnabled ? entry.file().rowCount() : 0,
                            entry.partitionBytes(),
                            bucketEnabled ? entry.bucket() : null,
                            bucketEnabled ? entry.totalBuckets() : null);
                }
                builder.endBlock();
            }
            return builder.serialize(size, records);
        }
    }

    /** Selects blocks using row-ID coverage. A null query retains every block after validation. */
    public static Selection select(
            byte[] data, ManifestFileMeta manifest, @Nullable RowRangeIndex query)
            throws IOException {
        return select(data, manifest, query, null, null);
    }

    /** Validates framing and tests row ID, partition, then bucket coverage. */
    public static Selection select(
            byte[] data,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable RowType partitionType)
            throws IOException {
        return select(data, manifest, query, partitionFilter, partitionType, null);
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
            @Nullable BiPredicate<Integer, Integer> bucketFilter)
            throws IOException {
        require(data.length >= MIN_HEADER_BYTES + CHECKSUM_BYTES);
        int limit = data.length - CHECKSUM_BYTES;
        CRC32 crc = new CRC32();
        crc.update(data, 0, limit);
        require((int) crc.getValue() == ByteBuffer.wrap(data, limit, CHECKSUM_BYTES).getInt());
        ByteBuffer in = ByteBuffer.wrap(data, 0, limit).slice();
        require(in.getInt() == MAGIC);
        require(decodeInt(in) == FORMAT_VERSION);
        long entries = Math.addExact(manifest.numAddedFiles(), manifest.numDeletedFiles());
        require(entries >= 0);
        int headerLength = decodeInt(in);
        require(headerLength >= 21 && headerLength <= in.remaining() - 2);
        require(headerLength <= manifest.fileSize());
        byte[] header = new byte[headerLength];
        in.get(header);
        require(header[0] == 'O' && header[1] == 'b' && header[2] == 'j' && header[3] == 1);
        int partitions = decodeInt(in);
        require(partitions <= in.remaining() / 13);
        boolean[] matches = partitionFilter == null ? null : new boolean[partitions];
        Set<ByteBuffer> unique = new java.util.HashSet<>();
        for (int id = 0; id < partitions; id++) {
            int length = decodeInt(in);
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
        int count = decodeInt(in);
        require(count <= in.remaining() / MIN_BLOCK_BYTES);
        long nextOffset = headerLength;
        long firstRecord = 0;
        List<Block> selected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            require(in.remaining() >= MIN_BLOCK_BYTES);
            long offset = VarLengthIntUtils.decodeLong(in);
            long length = VarLengthIntUtils.decodeLong(in);
            long records = VarLengthIntUtils.decodeLong(in);
            require(offset == nextOffset && length > 0 && length <= manifest.fileSize() - offset);
            require(records > 0 && records <= entries - firstRecord);
            ByteBuffer partitionPayload = payload(in);
            ByteBuffer rowPayload = payload(in);
            ByteBuffer bucketPayload = payload(in);
            DeltaVarintCodec.Reader ids = null;
            if (partitionPayload != null) {
                ids = new DeltaVarintCodec.Reader(partitionPayload, 0, partitions - 1L);
                require(ids.count() > 0 && ids.count() <= records && ids.count() <= partitions);
            }
            long min = 0;
            long max = 0;
            DeltaVarintCodec.Reader endpoints = null;
            if (rowPayload != null) {
                require(rowPayload.remaining() >= 2 * Long.BYTES + 1);
                min = rowPayload.getLong();
                max = rowPayload.getLong();
                endpoints = new DeltaVarintCodec.Reader(rowPayload, min, max);
                require(endpoints.count() % 2 == 0 && endpoints.count() / 2L < records);
                require(endpoints.count() != 0 || !rowPayload.hasRemaining());
            }
            if (bucketPayload != null) {
                ByteBuffer prefix = bucketPayload.duplicate();
                int pairs = decodeInt(prefix);
                require(pairs > 0 && pairs <= records && 2L * pairs + 1 <= prefix.remaining());
            }
            long blockFirstRecord = firstRecord;
            nextOffset = offset + length;
            firstRecord += records;

            if (query != null && rowPayload != null) {
                if (!query.intersects(min, max)) {
                    continue;
                }
                int rangeCount = endpoints.count() / 2 + 1;
                boolean rowHit = rangeCount == 1;
                long start = min;
                for (int range = 0; !rowHit && range < rangeCount; range++) {
                    long end = range + 1 == rangeCount ? max : endpoints.next();
                    require(end >= start);
                    rowHit = query.intersects(start, end);
                    if (!rowHit && range + 1 < rangeCount) {
                        start = endpoints.next();
                        require(start > end);
                    }
                    require(endpoints.hasNext() || !rowPayload.hasRemaining());
                }
                if (!rowHit) {
                    continue;
                }
            }

            if (partitionFilter != null && partitionPayload != null) {
                boolean partitionHit = false;
                long previous = -1;
                while (!partitionHit && ids.hasNext()) {
                    long id = ids.next();
                    require(id > previous);
                    require(ids.hasNext() || !partitionPayload.hasRemaining());
                    previous = id;
                    partitionHit = matches[(int) id];
                }
                if (!partitionHit) {
                    continue;
                }
            }

            if (bucketFilter != null && bucketPayload != null) {
                // Locate the second count-prefixed sequence without allocating value arrays.
                ByteBuffer totalsData = bucketPayload.duplicate();
                DeltaVarintCodec.Reader directory =
                        new DeltaVarintCodec.Reader(totalsData, 0, Integer.MAX_VALUE);
                while (directory.hasNext()) {
                    directory.next();
                }
                bucketPayload.limit(totalsData.position());
                DeltaVarintCodec.Reader buckets =
                        new DeltaVarintCodec.Reader(bucketPayload, 0, Integer.MAX_VALUE);
                DeltaVarintCodec.Reader totals =
                        new DeltaVarintCodec.Reader(totalsData, 0, Integer.MAX_VALUE, true);
                require(totals.count() == buckets.count());
                boolean bucketHit = false;
                int previousBucket = -1;
                int previousTotal = -1;
                while (!bucketHit && buckets.hasNext()) {
                    int bucket = (int) buckets.next();
                    int totalBuckets = (int) totals.next();
                    require(totalBuckets > bucket);
                    require(
                            bucket > previousBucket
                                    || (bucket == previousBucket && totalBuckets > previousTotal));
                    require(
                            buckets.hasNext()
                                    || (!bucketPayload.hasRemaining()
                                            && !totalsData.hasRemaining()));
                    previousBucket = bucket;
                    previousTotal = totalBuckets;
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

    /** Reads framing without expanding the compressed contents. */
    @Nullable
    private static ByteBuffer payload(ByteBuffer in) throws IOException {
        require(in.hasRemaining());
        int encoding = Byte.toUnsignedInt(in.get());
        if (encoding == 0) {
            return null;
        }
        int length = decodeInt(in);
        require(length <= in.remaining());
        ByteBuffer result = in.slice();
        result.limit(length);
        in.position(in.position() + length);
        if (encoding != 1) {
            return null;
        }
        return result;
    }

    /** Reads the complete sidecar. Null means read the original manifest. */
    @Nullable
    public static Selection read(
            FileIO io, Path path, ManifestFileMeta manifest, @Nullable RowRangeIndex query) {
        return read(io, path, manifest, query, null, null);
    }

    @Nullable
    public static Selection read(
            FileIO io,
            Path path,
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable RowType partitionType) {
        return read(io, path, manifest, query, partitionFilter, partitionType, null, null);
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
            @Nullable SegmentsCache<Path> cache) {
        String sidecarFileName = fileName(manifest);
        if (sidecarFileName == null) {
            return null;
        }
        try {
            Path sidecarPath = new Path(path.getParent(), sidecarFileName);
            Segments cached = cache == null ? null : cache.getIfPresents(sidecarPath);
            boolean cacheHit = cached instanceof ManifestSidecarSegment;
            byte[] data =
                    cacheHit
                            ? ((ManifestSidecarSegment) cached).bytes()
                            : readBytes(io, sidecarPath);
            Selection selection =
                    select(data, manifest, query, partitionFilter, partitionType, bucketFilter);
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

    /** Complete sidecar bytes stored in the configured sidecar cache. */
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

    private static byte[] readBytes(FileIO io, Path path) throws IOException {
        try (InputStream in = io.newInputStream(path)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[SIDECAR_READ_BUFFER_BYTES];
            int n;
            while ((n = in.read(buffer, 0, buffer.length)) != -1) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        }
    }

    static InputStream openManifest(FileIO io, Path path, @Nullable Selection selected)
            throws IOException {
        return selected == null
                ? io.newInputStream(path)
                : new SelectedBlockInput(io, path, selected);
    }

    /** An OCF stream comprising the original header and selected complete compressed blocks. */
    private static final class SelectedBlockInput extends InputStream {

        private final FileIO io;
        private final Path path;
        private final Selection selected;
        @Nullable private SeekableInputStream input;
        private boolean closed;
        private int headerPosition;
        private int blockPosition;
        private long remaining;
        private byte[] buffer;
        private int bufferPosition;
        private int bufferLimit;

        private SelectedBlockInput(FileIO io, Path path, Selection selected) {
            this.io = io;
            this.path = path;
            this.selected = selected;
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
                Block block = selected.blocks.get(blockPosition++);
                long end = block.offset + block.length;
                while (blockPosition < selected.blocks.size()
                        && selected.blocks.get(blockPosition).offset == end) {
                    end += selected.blocks.get(blockPosition++).length;
                }
                seekInput(block.offset);
                remaining = end - block.offset;
            }
            int requested = (int) Math.min(BLOCK_READ_BUFFER_BYTES, remaining);
            if (buffer == null || buffer.length < requested) {
                buffer = new byte[requested];
            }
            bufferPosition = 0;
            bufferLimit = 0;
            readFully(buffer, requested);
            bufferLimit = requested;
            remaining -= requested;
            return true;
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
                        input.read(
                                bytes,
                                position,
                                Math.min(BLOCK_READ_BUFFER_BYTES, length - position));
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
            throw new IOException("Invalid, unsupported or mismatched manifest sidecar");
        }
    }
}
