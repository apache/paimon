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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import static org.apache.paimon.utils.IOUtils.readFully;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.readCount;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Persisted row-id mappings and allocation bounds copied from a reassignment attempt. */
public final class SerializationAssignment {

    /** A snapshot-local marker and reference to the plan in the manifest directory. */
    public static final String PLAN_FILE_PROPERTY = "row-id-reassign.plan";

    public static final String REASSIGN_SNAPSHOT_ID = "reassign-snapshot-id";

    private static final int VERSION = 1;
    private static final String FILE_PREFIX = "snapshot-";

    private final long snapshotId;
    private final Map<BinaryRow, RowRangeMappingIndex> rowIdMappings;
    private final long firstAssignedRowId;
    private final long nextRowId;

    private SerializationAssignment(
            long snapshotId,
            Map<BinaryRow, RowRangeMappingIndex> rowIdMappings,
            long firstAssignedRowId,
            long nextRowId) {
        checkArgument(
                snapshotId >= Snapshot.FIRST_SNAPSHOT_ID,
                "Invalid reassignment snapshot ID: %s.",
                snapshotId);
        checkArgument(!rowIdMappings.isEmpty(), "Reassignment mappings must not be empty.");
        checkArgument(
                firstAssignedRowId >= 0 && nextRowId > firstAssignedRowId,
                "Invalid assigned row-id range [%s, %s).",
                firstAssignedRowId,
                nextRowId);
        this.snapshotId = snapshotId;
        this.rowIdMappings = Collections.unmodifiableMap(new LinkedHashMap<>(rowIdMappings));
        this.firstAssignedRowId = firstAssignedRowId;
        this.nextRowId = nextRowId;
    }

    /** The reassignment snapshot that references this plan file. */
    public long snapshotId() {
        return snapshotId;
    }

    public long firstAssignedRowId() {
        return firstAssignedRowId;
    }

    public long nextRowId() {
        return nextRowId;
    }

    /** Maps a complete file range, preserving every row's offset within the file. */
    public Range mapRowRange(BinaryRow partition, Range range) {
        RowRangeMappingIndex mapping = rowIdMappings.get(partition);
        if (mapping == null || !mapping.overlaps(range)) {
            return range;
        }
        Range mapped =
                mapping.map(range)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot reuse compaction across reassignment: range "
                                                        + range
                                                        + " is only partially or non-contiguously mapped."));
        checkState(
                mapped.from >= firstAssignedRowId
                        && mapped.to < nextRowId
                        && mapped.count() == range.count(),
                "Invalid reassignment of compaction range %s to %s.",
                range,
                mapped);
        return mapped;
    }

    /** Returns a plan only for the snapshot that committed it, ignoring inherited properties. */
    @Nullable
    public static String planFile(Snapshot snapshot) {
        Map<String, String> properties = snapshot.properties();
        if (properties == null
                || !Long.toString(snapshot.id()).equals(properties.get(REASSIGN_SNAPSHOT_ID))) {
            return null;
        }
        return properties.get(PLAN_FILE_PROPERTY);
    }

    /** Persists the assignment and adds its reference to this commit's snapshot properties. */
    static Map<String, String> writeProperties(
            FileStoreTable table,
            Snapshot snapshot,
            Map<BinaryRow, RowRangeMappingIndex> rowIdMappings,
            long firstAssignedRowId,
            long nextRowId) {
        long snapshotId = snapshot.id() + 1;
        String planFile;
        try {
            planFile =
                    new SerializationAssignment(
                                    snapshotId, rowIdMappings, firstAssignedRowId, nextRowId)
                            .write(table.fileIO(), table.store().pathFactory());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist row-id reassignment plan.", e);
        }
        Map<String, String> properties =
                snapshot.properties() == null
                        ? new HashMap<>()
                        : new HashMap<>(snapshot.properties());
        properties.put(PLAN_FILE_PROPERTY, planFile);
        properties.put(REASSIGN_SNAPSHOT_ID, Long.toString(snapshotId));
        return properties;
    }

    /** Streams the effective mappings without materializing another copy of the plan. */
    private String write(FileIO fileIO, FileStorePathFactory pathFactory) throws IOException {
        String fileName = FILE_PREFIX + snapshotId + "-" + UUID.randomUUID() + ".reassign-plan";
        Path path = pathFactory.toManifestFilePath(fileName);
        // A failed create may mean another attempt owns this path. Do not delete its plan.
        OutputStream fileOut = fileIO.newOutputStream(path, false);
        try (DataOutputViewStreamWrapper out =
                new DataOutputViewStreamWrapper(new BufferedOutputStream(fileOut))) {
            CRC32 checksum = new CRC32();
            DataOutputViewStreamWrapper payload =
                    new DataOutputViewStreamWrapper(new CheckedOutputStream(out, checksum));
            payload.writeInt(VERSION);
            payload.writeLong(snapshotId);
            payload.writeLong(firstAssignedRowId);
            payload.writeLong(nextRowId);
            payload.writeInt(rowIdMappings.size());
            for (Map.Entry<BinaryRow, RowRangeMappingIndex> entry : rowIdMappings.entrySet()) {
                serializeBinaryRow(entry.getKey(), payload);
                entry.getValue().serialize(payload);
            }
            payload.flush();
            out.writeLong(checksum.getValue());
        } catch (IOException | RuntimeException e) {
            fileIO.deleteQuietly(path);
            throw e;
        }
        return fileName;
    }

    public static SerializationAssignment readPlan(
            FileIO fileIO, FileStorePathFactory pathFactory, String fileName) throws IOException {
        Path path = pathFactory.toManifestFilePath(fileName);
        byte[] bytes = readFully(fileIO.newInputStream(path), true);
        if (bytes.length < Long.BYTES) {
            throw new IOException("Truncated row-id reassignment plan.");
        }
        int payloadSize = bytes.length - Long.BYTES;
        CRC32 checksum = new CRC32();
        checksum.update(bytes, 0, payloadSize);
        if (ByteBuffer.wrap(bytes).getLong(payloadSize) != checksum.getValue()) {
            throw new IOException("Row-id reassignment plan checksum mismatch.");
        }
        try {
            DataInputDeserializer in = new DataInputDeserializer(bytes, 0, payloadSize);
            int version = in.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported row-id reassignment plan version: " + version);
            }
            long snapshotId = in.readLong();
            long firstAssignedRowId = in.readLong();
            long nextRowId = in.readLong();
            int partitions = readCount(in, "reassignment partitions");
            Map<BinaryRow, RowRangeMappingIndex> mappings = new LinkedHashMap<>();
            for (int i = 0; i < partitions; i++) {
                BinaryRow partition = deserializeBinaryRow(in);
                if (mappings.put(partition, RowRangeMappingIndex.deserialize(in)) != null) {
                    throw new IOException("Duplicate partition in row-id reassignment plan.");
                }
            }
            if (in.available() != 0) {
                throw new IOException("Unexpected trailing bytes in row-id reassignment plan.");
            }
            return new SerializationAssignment(snapshotId, mappings, firstAssignedRowId, nextRowId);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid row-id reassignment plan " + fileName, e);
        }
    }
}
