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
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.readCount;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** The absolute row-id mappings applied by one committed reassignment snapshot. */
public final class DataEvolutionRowIdReassignPlan {

    /** A snapshot-local marker and reference to the plan in the manifest directory. */
    public static final String PLAN_FILE_PROPERTY = "row-id-reassign.plan";

    private static final int VERSION = 1;
    private static final String FILE_PREFIX = "row-id-reassign-plan-";

    private final long sourceSnapshotId;
    private final long snapshotId;
    private final Map<BinaryRow, RowRangeMappingIndex> mappings;

    DataEvolutionRowIdReassignPlan(
            long sourceSnapshotId, long snapshotId, Map<BinaryRow, RowRangeMappingIndex> mappings) {
        checkArgument(
                sourceSnapshotId >= Snapshot.FIRST_SNAPSHOT_ID
                        && snapshotId == Math.addExact(sourceSnapshotId, 1L),
                "Invalid reassignment snapshot transition %s -> %s.",
                sourceSnapshotId,
                snapshotId);
        checkArgument(!mappings.isEmpty(), "Reassignment mappings must not be empty.");
        this.sourceSnapshotId = sourceSnapshotId;
        this.snapshotId = snapshotId;
        this.mappings = Collections.unmodifiableMap(new LinkedHashMap<>(mappings));
    }

    public long sourceSnapshotId() {
        return sourceSnapshotId;
    }

    public long snapshotId() {
        return snapshotId;
    }

    /** Returns a mapping only when the entire range maps to a contiguous range. */
    public Optional<Range> map(BinaryRow partition, Range range) {
        RowRangeMappingIndex mapping = mappings.get(partition);
        return mapping == null ? Optional.empty() : mapping.map(range);
    }

    public boolean overlaps(BinaryRow partition, Range range) {
        RowRangeMappingIndex mapping = mappings.get(partition);
        return mapping != null && mapping.overlaps(range);
    }

    @Nullable
    public static String planFile(Snapshot snapshot) {
        return snapshot.properties() == null ? null : snapshot.properties().get(PLAN_FILE_PROPERTY);
    }

    /** Reassignment describes a transition and must not be inherited by another commit. */
    @Nullable
    public static Map<String, String> withoutPlan(@Nullable Map<String, String> properties) {
        if (properties == null || !properties.containsKey(PLAN_FILE_PROPERTY)) {
            return properties;
        }
        Map<String, String> result = new HashMap<>(properties);
        result.remove(PLAN_FILE_PROPERTY);
        return result.isEmpty() ? null : result;
    }

    /** Streams the effective mappings without materializing another copy of the plan. */
    String write(FileIO fileIO, FileStorePathFactory pathFactory) throws IOException {
        String fileName = FILE_PREFIX + UUID.randomUUID();
        Path path = pathFactory.toManifestFilePath(fileName);
        try (DataOutputViewStreamWrapper out =
                new DataOutputViewStreamWrapper(
                        new BufferedOutputStream(fileIO.newOutputStream(path, false)))) {
            CRC32 checksum = new CRC32();
            DataOutputViewStreamWrapper payload =
                    new DataOutputViewStreamWrapper(new CheckedOutputStream(out, checksum));
            payload.writeInt(VERSION);
            payload.writeLong(sourceSnapshotId);
            payload.writeLong(snapshotId);
            payload.writeInt(mappings.size());
            for (Map.Entry<BinaryRow, RowRangeMappingIndex> entry : mappings.entrySet()) {
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

    public static DataEvolutionRowIdReassignPlan read(
            FileIO fileIO, FileStorePathFactory pathFactory, Snapshot snapshot) throws IOException {
        String fileName = planFile(snapshot);
        checkArgument(fileName != null, "Snapshot %s has no reassignment plan.", snapshot.id());
        try (DataInputViewStreamWrapper in =
                new DataInputViewStreamWrapper(
                        new BufferedInputStream(
                                fileIO.newInputStream(pathFactory.toManifestFilePath(fileName))))) {
            CRC32 checksum = new CRC32();
            DataInputViewStreamWrapper payload =
                    new DataInputViewStreamWrapper(new CheckedInputStream(in, checksum));
            int version = payload.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported row-id reassignment plan version: " + version);
            }
            long sourceSnapshotId = payload.readLong();
            long snapshotId = payload.readLong();
            if (snapshotId != snapshot.id()) {
                throw new IOException("Row-id reassignment plan belongs to snapshot " + snapshotId);
            }
            int partitions = readCount(payload, "reassignment partitions");
            Map<BinaryRow, RowRangeMappingIndex> mappings = new LinkedHashMap<>();
            for (int i = 0; i < partitions; i++) {
                BinaryRow partition = deserializeBinaryRow(payload);
                if (mappings.put(partition, RowRangeMappingIndex.deserialize(payload)) != null) {
                    throw new IOException("Duplicate partition in row-id reassignment plan.");
                }
            }
            long actualChecksum = checksum.getValue();
            if (in.readLong() != actualChecksum) {
                throw new IOException("Row-id reassignment plan checksum mismatch.");
            }
            if (in.read() != -1) {
                throw new IOException("Unexpected trailing bytes in row-id reassignment plan.");
            }
            return new DataEvolutionRowIdReassignPlan(sourceSnapshotId, snapshotId, mappings);
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new IOException("Invalid row-id reassignment plan " + fileName, e);
        }
    }
}
