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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Metadata of a manifest file.
 *
 * @since 0.9.0
 */
@Public
public class ManifestFileMeta {

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(
                                    0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)),
                            new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)),
                            new DataField(4, "_PARTITION_STATS", SimpleStats.SCHEMA),
                            new DataField(5, "_SCHEMA_ID", new BigIntType(false)),
                            new DataField(6, "_MIN_BUCKET", new IntType(true)),
                            new DataField(7, "_MAX_BUCKET", new IntType(true)),
                            new DataField(8, "_MIN_LEVEL", new IntType(true)),
                            new DataField(9, "_MAX_LEVEL", new IntType(true)),
                            new DataField(10, "_MIN_ROW_ID", new BigIntType(true)),
                            new DataField(11, "_MAX_ROW_ID", new BigIntType(true))));

    private final String fileName;
    private final long fileSize;
    private final long numAddedFiles;
    private final long numDeletedFiles;
    private final SimpleStats partitionStats;
    private final long schemaId;
    private final @Nullable Integer minBucket;
    private final @Nullable Integer maxBucket;
    private final @Nullable Integer minLevel;
    private final @Nullable Integer maxLevel;
    private final @Nullable Long minRowId;
    private final @Nullable Long maxRowId;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            SimpleStats partitionStats,
            long schemaId,
            @Nullable Integer minBucket,
            @Nullable Integer maxBucket,
            @Nullable Integer minLevel,
            @Nullable Integer maxLevel,
            @Nullable Long minRowId,
            @Nullable Long maxRowId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
        this.minBucket = minBucket;
        this.maxBucket = maxBucket;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.minRowId = minRowId;
        this.maxRowId = maxRowId;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long numAddedFiles() {
        return numAddedFiles;
    }

    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    public SimpleStats partitionStats() {
        return partitionStats;
    }

    public long schemaId() {
        return schemaId;
    }

    public @Nullable Integer minBucket() {
        return minBucket;
    }

    public @Nullable Integer maxBucket() {
        return maxBucket;
    }

    public @Nullable Integer minLevel() {
        return minLevel;
    }

    public @Nullable Integer maxLevel() {
        return maxLevel;
    }

    public @Nullable Long minRowId() {
        return minRowId;
    }

    public @Nullable Long maxRowId() {
        return maxRowId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId
                && Objects.equals(minBucket, that.minBucket)
                && Objects.equals(maxBucket, that.maxBucket)
                && Objects.equals(minLevel, that.minLevel)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(minRowId, that.minRowId)
                && Objects.equals(maxRowId, that.maxRowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                partitionStats,
                schemaId,
                minBucket,
                maxBucket,
                minLevel,
                maxLevel,
                minRowId,
                maxRowId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d, %s, %s, %s, %s, %s, %s}",
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                partitionStats,
                schemaId,
                minBucket,
                maxBucket,
                minLevel,
                maxLevel,
                minRowId,
                maxRowId);
    }

    // ----------------------- Serialization -----------------------------

    private static final ThreadLocal<ManifestFileMetaSerializer> SERIALIZER_THREAD_LOCAL =
            ThreadLocal.withInitial(ManifestFileMetaSerializer::new);

    public byte[] toBytes() throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().serializeToBytes(this);
    }

    public ManifestFileMeta fromBytes(byte[] bytes) throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().deserializeFromBytes(bytes);
    }
}
