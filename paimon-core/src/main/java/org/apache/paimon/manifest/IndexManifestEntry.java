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
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * Manifest entry for index file.
 *
 * @since 0.9.0
 */
@Public
public class IndexManifestEntry {

    public static final String KIND = "_KIND";
    public static final String PARTITION = "_PARTITION";
    public static final String BUCKET = "_BUCKET";
    public static final String INDEX_TYPE = "_INDEX_TYPE";
    public static final String FILE_NAME = "_FILE_NAME";
    public static final String FILE_SIZE = "_FILE_SIZE";
    public static final String ROW_COUNT = "_ROW_COUNT";
    public static final String DELETION_VECTORS_RANGES = "_DELETIONS_VECTORS_RANGES";
    public static final String EXTERNAL_PATH = "_EXTERNAL_PATH";
    public static final String GLOBAL_INDEX = "_GLOBAL_INDEX";
    public static final String SCHEMA_ID = "_SCHEMA_ID";

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, KIND, new TinyIntType(false)),
                            new DataField(1, PARTITION, newBytesType(false)),
                            new DataField(2, BUCKET, new IntType(false)),
                            new DataField(3, INDEX_TYPE, newStringType(false)),
                            new DataField(4, FILE_NAME, newStringType(false)),
                            new DataField(5, FILE_SIZE, new BigIntType(false)),
                            new DataField(6, ROW_COUNT, new BigIntType(false)),
                            new DataField(
                                    7,
                                    DELETION_VECTORS_RANGES,
                                    new ArrayType(true, DeletionVectorMeta.SCHEMA)),
                            new DataField(8, EXTERNAL_PATH, newStringType(true)),
                            new DataField(9, GLOBAL_INDEX, GlobalIndexMeta.SCHEMA),
                            new DataField(10, SCHEMA_ID, new BigIntType(true))));

    public static final RowType MANIFEST_ROW_TYPE =
            ManifestSchemaUtils.withFormatIdentifier(SCHEMA);

    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final IndexFileMeta indexFile;
    @Nullable private final Long schemaId;

    public IndexManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, IndexFileMeta indexFile) {
        this(kind, partition, bucket, indexFile, indexFile.schemaId());
    }

    public IndexManifestEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            IndexFileMeta indexFile,
            @Nullable Long schemaId) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.indexFile = indexFile.withSchemaId(schemaId);
        this.schemaId = schemaId;
    }

    public IndexManifestEntry toDeleteEntry() {
        checkArgument(kind == FileKind.ADD);
        return new IndexManifestEntry(FileKind.DELETE, partition, bucket, indexFile, schemaId);
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

    @Nullable
    public Long schemaId() {
        return schemaId;
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
                && Objects.equals(indexFile, entry.indexFile)
                && Objects.equals(schemaId, entry.schemaId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, indexFile, schemaId);
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
                + ", schemaId="
                + schemaId
                + '}';
    }
}
