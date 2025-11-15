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

package org.apache.paimon.index;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * Metadata of index file.
 *
 * @since 0.9.0
 */
@Public
public class IndexFileMeta {

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_INDEX_TYPE", newStringType(false)),
                            new DataField(1, "_FILE_NAME", newStringType(false)),
                            new DataField(2, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(3, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(
                                    4,
                                    "_DELETIONS_VECTORS_RANGES",
                                    new ArrayType(true, DeletionVectorMeta.SCHEMA)),
                            new DataField(5, "_EXTERNAL_PATH", newStringType(true)),
                            new DataField(6, "_SHARD", new IntType(true)),
                            new DataField(7, "_INDEX_FIELD_ID", new IntType(true)),
                            new DataField(8, "_INDEX_META", new BinaryType())));

    private final String indexType;
    private final String fileName;
    private final long fileSize;
    private final long rowCount;

    @Nullable private final Integer shard;
    @Nullable private final Integer indexFieldId;
    @Nullable private final byte[] indexMeta;

    /**
     * Metadata only used by {@link DeletionVectorsIndexFile}, use LinkedHashMap to ensure that the
     * order of DeletionVectorMetas and the written DeletionVectors is consistent.
     */
    private final @Nullable LinkedHashMap<String, DeletionVectorMeta> dvRanges;

    private final @Nullable String externalPath;

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable LinkedHashMap<String, DeletionVectorMeta> dvRanges,
            @Nullable String externalPath) {
        this(indexType, fileName, fileSize, rowCount, dvRanges, externalPath, null, null, null);
    }

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable Integer shard,
            @Nullable Integer indexFieldId,
            @Nullable byte[] indexMeta) {
        this(indexType, fileName, fileSize, rowCount, null, null, shard, indexFieldId, indexMeta);
    }

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable LinkedHashMap<String, DeletionVectorMeta> dvRanges,
            @Nullable String externalPath,
            @Nullable Integer shard,
            @Nullable Integer indexFieldId,
            @Nullable byte[] indexMeta) {
        this.indexType = indexType;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.dvRanges = dvRanges;
        this.externalPath = externalPath;
        this.shard = shard;
        this.indexFieldId = indexFieldId;
        this.indexMeta = indexMeta;
    }

    public String indexType() {
        return indexType;
    }

    public Integer getShard() {
        return shard;
    }

    public Integer indexFieldId() {
        return indexFieldId;
    }

    public byte[] indexMeta() {
        return indexMeta;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long rowCount() {
        return rowCount;
    }

    public @Nullable LinkedHashMap<String, DeletionVectorMeta> dvRanges() {
        return dvRanges;
    }

    @Nullable
    public String externalPath() {
        return externalPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexFileMeta that = (IndexFileMeta) o;
        return Objects.equals(indexType, that.indexType)
                && Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && rowCount == that.rowCount
                && Objects.equals(dvRanges, that.dvRanges)
                && Objects.equals(externalPath, that.externalPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexType, fileName, fileSize, rowCount, dvRanges, externalPath);
    }

    @Override
    public String toString() {
        return "IndexManifestEntry{"
                + "indexType="
                + indexType
                + ", fileName='"
                + fileName
                + '\''
                + ", fileSize="
                + fileSize
                + ", rowCount="
                + rowCount
                + ", dvRanges="
                + dvRanges
                + ", externalPath='"
                + externalPath
                + '}';
    }
}
