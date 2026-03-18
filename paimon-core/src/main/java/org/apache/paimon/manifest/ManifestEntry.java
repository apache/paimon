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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/**
 * Entry of a manifest file, representing an addition / deletion of a data file.
 *
 * @since 0.9.0
 */
@Public
public interface ManifestEntry extends FileEntry {

    RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_KIND", new TinyIntType(false)),
                            new DataField(1, "_PARTITION", newBytesType(false)),
                            new DataField(2, "_BUCKET", new IntType(false)),
                            new DataField(3, "_TOTAL_BUCKETS", new IntType(false)),
                            new DataField(4, "_FILE", DataFileMeta.SCHEMA)));

    static ManifestEntry create(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        return new PojoManifestEntry(kind, partition, bucket, totalBuckets, file);
    }

    DataFileMeta file();

    ManifestEntry copyWithoutStats();

    ManifestEntry assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber);

    ManifestEntry assignFirstRowId(long firstRowId);

    ManifestEntry upgrade(int newLevel);

    static long recordCount(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().mapToLong(manifest -> manifest.file().rowCount()).sum();
    }

    @Nullable
    static Long nullableRecordCount(List<ManifestEntry> manifestEntries) {
        if (manifestEntries.isEmpty()) {
            return null;
        }
        return manifestEntries.stream().mapToLong(manifest -> manifest.file().rowCount()).sum();
    }

    static long recordCountAdd(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.ADD.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }

    static long recordCountDelete(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.DELETE.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }
}
