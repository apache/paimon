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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/**
 * Entry of a manifest file, representing an addition / deletion of a data file.
 *
 * @since 0.9.0
 */
@Public
public class ManifestEntry implements FileEntry {

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_KIND", new TinyIntType(false)),
                            new DataField(1, "_PARTITION", newBytesType(false)),
                            new DataField(2, "_BUCKET", new IntType(false)),
                            new DataField(3, "_TOTAL_BUCKETS", new IntType(false)),
                            new DataField(4, "_FILE", DataFileMeta.SCHEMA)));

    private final FileKind kind;
    // for tables without partition this field should be a row with 0 columns (not null)
    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final DataFileMeta file;

    public ManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    @Override
    public FileKind kind() {
        return kind;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public int level() {
        return file.level();
    }

    @Override
    public String fileName() {
        return file.fileName();
    }

    @Override
    public String externalPath() {
        return file.externalPath();
    }

    @Override
    public BinaryRow minKey() {
        return file.minKey();
    }

    @Override
    public BinaryRow maxKey() {
        return file.maxKey();
    }

    @Override
    public List<String> extraFiles() {
        return file.extraFiles();
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public DataFileMeta file() {
        return file;
    }

    @Override
    public Identifier identifier() {
        return new Identifier(
                partition,
                bucket,
                file.level(),
                file.fileName(),
                file.extraFiles(),
                file.embeddedIndex(),
                file.externalPath());
    }

    public ManifestEntry copyWithoutStats() {
        return new ManifestEntry(kind, partition, bucket, totalBuckets, file.copyWithoutStats());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestEntry)) {
            return false;
        }
        ManifestEntry that = (ManifestEntry) o;
        return Objects.equals(kind, that.kind)
                && Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, totalBuckets, file);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, totalBuckets, file);
    }

    public static long recordCount(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().mapToLong(manifest -> manifest.file().rowCount()).sum();
    }

    public static long recordCountAdd(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.ADD.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }

    public static long recordCountDelete(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.DELETE.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }

    // ----------------------- Serialization -----------------------------

    private static final ThreadLocal<ManifestEntrySerializer> SERIALIZER_THREAD_LOCAL =
            ThreadLocal.withInitial(ManifestEntrySerializer::new);

    public byte[] toBytes() throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().serializeToBytes(this);
    }

    public ManifestEntry fromBytes(byte[] bytes) throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().deserializeFromBytes(bytes);
    }
}
