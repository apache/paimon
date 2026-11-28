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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;
import org.apache.paimon.operation.ManifestEntryExternalSort.BinaryRowSorter;
import org.apache.paimon.operation.ManifestEntryExternalSort.ExternalSortConfig;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * Spillable equivalent of validating delta entries and then merging base and delta entries with
 * {@link FileEntry#mergeEntries(Iterable)}. Survivors retain the original insertion order.
 *
 * <p>The two sorters share one configured sort-buffer budget. Reduction retains only the current
 * identity's state and the first invalid histories, rather than a map of all file identities. The
 * encoded-size limit covers input and survivor rows; it is a logical metadata budget, not a bound
 * on physical spill I/O (merging runs may temporarily coexist). A single entry still has to fit in
 * the sort buffer. The caller owns this buffer until consumption finishes, including on failure.
 */
public final class FileEntryMergeBuffer implements AutoCloseable {

    private static final int MIN_SORT_BUFFER_PAGES = 6;

    private static final RowType ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.BYTES(), // complete identity, including DV subtype
                    DataTypes.INT(), // base before delta
                    DataTypes.BIGINT(), // original ordinal within source
                    DataTypes.TINYINT(), // kind
                    DataTypes.INT(), // total buckets (not part of identity)
                    DataTypes.BYTES(), // min key
                    DataTypes.BYTES(), // max key
                    DataTypes.BIGINT(), // row count
                    DataTypes.BIGINT()); // nullable first row id

    private final IOManager ioManager;
    private final boolean ownedIOManager;
    private final BinaryRowSorter identities;
    private final BinaryRowSorter survivors;
    private final InternalRowSerializer serializer = new InternalRowSerializer(ROW_TYPE);
    private final long maxEncodedSize;
    private final long[] ordinals = new long[2];
    private long encodedSize;
    private boolean finished;
    private boolean closed;
    @Nullable private Failure deltaFailure;
    @Nullable private Failure combinedFailure;

    public FileEntryMergeBuffer(CoreOptions options, @Nullable IOManager ioManager)
            throws Exception {
        maxEncodedSize = options.dataEvolutionMetadataMaxDiskSize();
        ExternalSortConfig configured = ExternalSortConfig.from(options, ioManager);
        long pages = configured.bufferSize / configured.pageSize;
        checkArgument(
                pages >= MIN_SORT_BUFFER_PAGES,
                "Metadata merge requires at least six sort-buffer pages.");
        this.ownedIOManager = ioManager == null;
        this.ioManager =
                ownedIOManager ? IOManager.create(System.getProperty("java.io.tmpdir")) : ioManager;
        BinaryRowSorter first = null;
        try {
            first =
                    new BinaryRowSorter(
                            ROW_TYPE, new int[] {0, 1, 2}, config(configured, pages / 2));
            survivors =
                    new BinaryRowSorter(
                            ROW_TYPE, new int[] {1, 2}, config(configured, pages - pages / 2));
            identities = first;
        } catch (Throwable failure) {
            try {
                IOUtils.closeAll(first, ownedIOManager ? this.ioManager : null);
            } catch (Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Whether one configured budget can supply both sorters with at least three full pages. */
    public static boolean supportsBufferSize(CoreOptions options) {
        return options.sortSpillBufferSize() / options.pageSize() >= MIN_SORT_BUFFER_PAGES;
    }

    private ExternalSortConfig config(ExternalSortConfig configured, long pages) {
        return new ExternalSortConfig(
                pages * configured.pageSize,
                configured.pageSize,
                configured.maxNumFileHandles,
                configured.compression,
                configured.maxDiskSize,
                ioManager);
    }

    public void writeBase(SimpleFileEntry entry) throws IOException {
        write(entry, 0);
    }

    public void writeDelta(SimpleFileEntry entry) throws IOException {
        write(entry, 1);
    }

    private void write(SimpleFileEntry entry, int source) throws IOException {
        checkWritable();
        // Copy into the sorter immediately. No complete decoded entry is retained by this buffer.
        BinaryRow row =
                serializer.toBinaryRow(
                        GenericRow.of(
                                identity(entry),
                                source,
                                ordinals[source],
                                entry.kind().toByteValue(),
                                entry.totalBuckets(),
                                entry.minKey() == null ? null : serializeBinaryRow(entry.minKey()),
                                entry.maxKey() == null ? null : serializeBinaryRow(entry.maxKey()),
                                entry.rowCount(),
                                entry.firstRowId()));
        writeEncoded(identities, row);
        ordinals[source] = Math.incrementExact(ordinals[source]);
    }

    private void writeEncoded(BinaryRowSorter sorter, BinaryRow row) throws IOException {
        long bytes = row.getSizeInBytes();
        if (bytes > maxEncodedSize - encodedSize) {
            throw new IOException("Metadata merge encoded limit exceeded: " + maxEncodedSize);
        }
        sorter.write(row);
        encodedSize += bytes;
    }

    /** Validates both histories before invoking the consumer, exactly once. */
    public void forEachMerged(ThrowingConsumer<SimpleFileEntry, Exception> consumer)
            throws Exception {
        forEachMerged(false, consumer);
    }

    /**
     * Optionally keeps the first base record per identity, as the ordinary DE supplemental-file
     * scan does. Delta records are never deduplicated. The caller must select this only when that
     * scan branch would deduplicate its base input, even if the supplemental scan returns no rows.
     */
    public void forEachMerged(
            boolean deduplicateBase, ThrowingConsumer<SimpleFileEntry, Exception> consumer)
            throws Exception {
        forEachMerged(deduplicateBase, null, consumer);
    }

    /**
     * Observes the first base record and last delta DELETE for each base identity before they can
     * cancel. This is intended for current-ADD base scans. Observations arrive in identity order;
     * the original base ordinal is supplied to preserve conflict selection order.
     *
     * <p>The observer may only prepare discardable local state: identity validation can still fail
     * after an observation. Semantic errors from these observations must be decided only after the
     * merge's own validation succeeds. Observer failures propagate to the caller, who must close
     * this buffer. Ordinary consumers still run only after identity validation.
     */
    public void forEachMerged(
            boolean deduplicateBase,
            @Nullable BaseEntryConsumer baseConsumer,
            ThrowingConsumer<SimpleFileEntry, Exception> consumer)
            throws Exception {
        checkWritable();
        finished = true;
        MutableObjectIterator<BinaryRow> iterator = identities.sortedIterator();
        BinaryRow reuse = new BinaryRow(ROW_TYPE.getFieldCount());
        byte[] currentIdentity = null;
        State delta = new State();
        State combined = new State();
        BinaryRow base = null;
        BinaryRow lastDelete = null;
        BinaryRow row;
        while ((row = iterator.next(reuse)) != null) {
            byte[] identity = row.getBinary(0);
            if (!Arrays.equals(identity, currentIdentity)) {
                observeBase(baseConsumer, base, lastDelete);
                emitSurvivor(combined);
                delta = new State();
                combined = new State();
                base = null;
                lastDelete = null;
                currentIdentity = identity;
            }
            if (baseConsumer != null) {
                if (row.getInt(1) == 0 && base == null) {
                    base = row.copy();
                } else if (row.getInt(1) == 1
                        && FileKind.fromByteValue(row.getByte(3)) == FileKind.DELETE) {
                    lastDelete = row.copy();
                }
            }
            if (row.getInt(1) == 1) {
                deltaFailure = transition(delta, row, deltaFailure);
            }
            if (!deduplicateBase || row.getInt(1) == 1 || combined.row == null) {
                combinedFailure = transition(combined, row, combinedFailure);
            }
        }
        observeBase(baseConsumer, base, lastDelete);
        emitSurvivor(combined);
        // Delta-only validation precedes combined validation in the original conflict checker.
        if (deltaFailure != null) {
            throw deltaFailure.exception();
        }
        if (combinedFailure != null) {
            throw combinedFailure.exception();
        }
        iterator = survivors.sortedIterator();
        while ((row = iterator.next(reuse)) != null) {
            consumer.accept(decode(row));
        }
    }

    private static void observeBase(
            @Nullable BaseEntryConsumer consumer,
            @Nullable BinaryRow base,
            @Nullable BinaryRow lastDelete)
            throws Exception {
        if (consumer != null && base != null) {
            consumer.accept(
                    decode(base), base.getLong(2), lastDelete == null ? null : decode(lastDelete));
        }
    }

    /** Receives independent decoded metadata for one current base identity. */
    @FunctionalInterface
    public interface BaseEntryConsumer {
        void accept(
                SimpleFileEntry base, long originalOrdinal, @Nullable SimpleFileEntry lastDelete)
                throws Exception;
    }

    @Nullable
    private Failure transition(State state, BinaryRow row, @Nullable Failure firstFailure) {
        if (state.invalid) {
            return firstFailure;
        }
        if (FileKind.fromByteValue(row.getByte(3)) == FileKind.ADD && state.row != null) {
            state.invalid = true;
            if (firstFailure == null || earlier(row, firstFailure.addition)) {
                return new Failure(state.row, row.copy());
            }
        } else {
            state.row = state.row == null ? row.copy() : null;
        }
        return firstFailure;
    }

    private static boolean earlier(BinaryRow left, BinaryRow right) {
        return left.getInt(1) < right.getInt(1)
                || (left.getInt(1) == right.getInt(1) && left.getLong(2) < right.getLong(2));
    }

    private void emitSurvivor(State state) throws IOException {
        if (!state.invalid && state.row != null) {
            writeEncoded(survivors, state.row);
        }
    }

    private void checkWritable() {
        checkState(!closed && !finished, "Metadata merge buffer is already finished or closed.");
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            deltaFailure = null;
            combinedFailure = null;
            IOUtils.closeAll(identities, survivors, ownedIOManager ? ioManager : null);
        }
    }

    private static byte[] identity(SimpleFileEntry entry) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        writeBytes(out, serializeBinaryRow(entry.partition()));
        out.writeInt(entry.bucket());
        out.writeInt(entry.level());
        writeString(out, entry.fileName());
        out.writeInt(entry.extraFiles().size());
        for (String extra : entry.extraFiles()) {
            writeString(out, extra);
        }
        writeBytes(out, entry.embeddedIndex());
        writeString(out, entry.externalPath());
        boolean dv = entry instanceof SimpleFileEntryWithDV;
        out.writeBoolean(dv);
        if (dv) {
            writeString(out, ((SimpleFileEntryWithDV) entry).dvFileName());
        }
        return bytes.toByteArray();
    }

    private static SimpleFileEntry decode(BinaryRow row) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(row.getBinary(0)));
        BinaryRow partition = deserializeBinaryRow(readBytes(in));
        int bucket = in.readInt();
        int level = in.readInt();
        String name = readString(in);
        int extraCount = in.readInt();
        List<String> extra = new ArrayList<>(extraCount);
        for (int i = 0; i < extraCount; i++) {
            extra.add(readString(in));
        }
        byte[] embedded = readBytes(in);
        String external = readString(in);
        boolean dv = in.readBoolean();
        String dvName = dv ? readString(in) : null;
        SimpleFileEntry entry =
                new SimpleFileEntry(
                        FileKind.fromByteValue(row.getByte(3)),
                        partition,
                        bucket,
                        row.getInt(4),
                        level,
                        name,
                        extra,
                        embedded,
                        row.isNullAt(5) ? null : deserializeBinaryRow(row.getBinary(5)),
                        row.isNullAt(6) ? null : deserializeBinaryRow(row.getBinary(6)),
                        external,
                        row.getLong(7),
                        row.isNullAt(8) ? null : row.getLong(8));
        return dv ? new SimpleFileEntryWithDV(entry, dvName) : entry;
    }

    private static void writeString(DataOutputStream out, @Nullable String value)
            throws IOException {
        // String.equals compares UTF-16 code units, including unpaired surrogates. Charset
        // encoders can replace those units and incorrectly collapse distinct file identities.
        out.writeInt(value == null ? -1 : value.length());
        if (value != null) {
            for (int i = 0; i < value.length(); i++) {
                out.writeChar(value.charAt(i));
            }
        }
    }

    @Nullable
    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > in.available() / Character.BYTES) {
            throw new IOException("Invalid metadata merge string length: " + length);
        }
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = in.readChar();
        }
        return new String(chars);
    }

    private static void writeBytes(DataOutputStream out, @Nullable byte[] bytes)
            throws IOException {
        out.writeInt(bytes == null ? -1 : bytes.length);
        if (bytes != null) {
            out.write(bytes);
        }
    }

    @Nullable
    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > in.available()) {
            throw new IOException("Invalid metadata merge identity length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static final class State {
        @Nullable private BinaryRow row;
        private boolean invalid;
    }

    private static final class Failure {
        private final BinaryRow previous;
        private final BinaryRow addition;

        private Failure(BinaryRow previous, BinaryRow addition) {
            this.previous = previous;
            this.addition = addition;
        }

        private IllegalStateException exception() throws IOException {
            return new IllegalStateException(
                    String.format(
                            "Trying to add file %s which is already in the the map: %s",
                            decode(addition).identifier(), decode(previous)));
        }
    }
}
