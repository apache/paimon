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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.store.file.utils.SerializationUtils.newBytesType;

/** Entry of a manifest file, representing an addition / deletion of a data file. */
public class ManifestEntry {

    private final FileKind kind;
    // for tables without partition this field should be a row with 0 columns (not null)
    private final BinaryRowData partition;
    private final int bucket;
    private final int totalBuckets;
    private final DataFileMeta file;

    public ManifestEntry(
            FileKind kind,
            BinaryRowData partition,
            int bucket,
            int totalBuckets,
            DataFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    public FileKind kind() {
        return kind;
    }

    public BinaryRowData partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public DataFileMeta file() {
        return file;
    }

    public Identifier identifier() {
        return new Identifier(partition, bucket, file.level(), file.fileName());
    }

    public static RowType schema() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_KIND", new TinyIntType(false)));
        fields.add(new RowType.RowField("_PARTITION", newBytesType(false)));
        fields.add(new RowType.RowField("_BUCKET", new IntType(false)));
        fields.add(new RowType.RowField("_TOTAL_BUCKETS", new IntType(false)));
        fields.add(new RowType.RowField("_FILE", DataFileMeta.schema()));
        return new RowType(fields);
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

    public static Collection<ManifestEntry> mergeEntries(List<ManifestEntry> entries) {
        LinkedHashMap<Identifier, ManifestEntry> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    public static void mergeEntries(
            List<ManifestEntry> entries, Map<Identifier, ManifestEntry> map) {
        for (ManifestEntry entry : entries) {
            ManifestEntry.Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added. Manifest might be corrupted.",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // each dataFile will only be added once and deleted once,
                    // if we know that it is added before then both add and delete entry can be
                    // removed because there won't be further operations on this file,
                    // otherwise we have to keep the delete entry because the add entry must be
                    // in the previous manifest files
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    public static void assertNoDelete(Collection<ManifestEntry> entries) {
        for (ManifestEntry entry : entries) {
            Preconditions.checkState(
                    entry.kind() != FileKind.DELETE,
                    "Trying to delete file %s which is not previously added. Manifest might be corrupted.",
                    entry.file().fileName());
        }
    }

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    public static class Identifier {
        public final BinaryRowData partition;
        public final int bucket;
        public final int level;
        public final String fileName;

        private Identifier(BinaryRowData partition, int bucket, int level, String fileName) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Identifier)) {
                return false;
            }
            Identifier that = (Identifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level, fileName);
        }

        @Override
        public String toString() {
            return String.format("{%s, %d, %d, %s}", partition, bucket, level, fileName);
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName;
        }
    }
}
