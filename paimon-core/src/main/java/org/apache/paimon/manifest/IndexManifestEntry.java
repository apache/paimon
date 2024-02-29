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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Manifest entry for index file. */
public class IndexManifestEntry {

    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final IndexFileMeta indexFile;

    public IndexManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, IndexFileMeta indexFile) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.indexFile = indexFile;
    }

    public IndexManifestEntry toDeleteEntry() {
        checkArgument(kind == FileKind.ADD);
        return new IndexManifestEntry(FileKind.DELETE, partition, bucket, indexFile);
    }

    public FileKind kind() {
        return kind;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public IndexFileMeta indexFile() {
        return indexFile;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_KIND", new TinyIntType(false)));
        fields.add(new DataField(1, "_PARTITION", newBytesType(false)));
        fields.add(new DataField(2, "_BUCKET", new IntType(false)));
        fields.add(new DataField(3, "_INDEX_TYPE", newStringType(false)));
        fields.add(new DataField(4, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(5, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(6, "_ROW_COUNT", new BigIntType(false)));
        fields.add(
                new DataField(
                        7,
                        "_DELETIONS_VECTORS_RANGES",
                        new ArrayType(
                                true,
                                RowType.of(
                                        newStringType(false),
                                        new IntType(false),
                                        new IntType(false)))));
        return new RowType(fields);
    }

    public Identifier identifier() {
        return new Identifier(partition, bucket, indexFile.indexType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexManifestEntry entry = (IndexManifestEntry) o;
        return bucket == entry.bucket
                && kind == entry.kind
                && Objects.equals(partition, entry.partition)
                && Objects.equals(indexFile, entry.indexFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, indexFile);
    }

    @Override
    public String toString() {
        return "IndexManifestEntry{"
                + "kind="
                + kind
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", indexFile="
                + indexFile
                + '}';
    }

    /** The {@link Identifier} of a {@link IndexFileMeta}. */
    public static class Identifier {

        public final BinaryRow partition;
        public final int bucket;
        public final String indexType;

        private Integer hash;

        private Identifier(BinaryRow partition, int bucket, String indexType) {
            this.partition = partition;
            this.bucket = bucket;
            this.indexType = indexType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Identifier that = (Identifier) o;
            return bucket == that.bucket
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(indexType, that.indexType);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, indexType);
            }
            return hash;
        }

        @Override
        public String toString() {
            return "Identifier{"
                    + "partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", indexType='"
                    + indexType
                    + '\''
                    + '}';
        }
    }
}
