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
import org.apache.paimon.types.DataField;
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

    public static final String DATA_FILE = "INDEX";
    public static final String ROUTING_MODEL = "ROUTING_MODEL";

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
                            new DataField(6, "_GLOBAL_INDEX", GlobalIndexMeta.SCHEMA),
                            new DataField(7, "_INDEX_FILE_KIND", newStringType(true))));

    private final String indexType;
    private final String fileName;
    private final long fileSize;
    private final long rowCount;
    private final String fileKind;

    @Nullable private final GlobalIndexMeta globalIndexMeta;

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
        this(indexType, fileName, fileSize, rowCount, dvRanges, externalPath, null);
    }

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable LinkedHashMap<String, DeletionVectorMeta> dvRanges,
            @Nullable String externalPath,
            @Nullable GlobalIndexMeta globalIndexMeta) {
        this(
                indexType,
                fileName,
                fileSize,
                rowCount,
                dvRanges,
                externalPath,
                globalIndexMeta,
                DATA_FILE);
    }

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable LinkedHashMap<String, DeletionVectorMeta> dvRanges,
            @Nullable String externalPath,
            @Nullable GlobalIndexMeta globalIndexMeta,
            @Nullable String fileKind) {
        this.indexType = indexType;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.dvRanges = dvRanges;
        this.externalPath = externalPath;
        this.globalIndexMeta = globalIndexMeta;
        this.fileKind = fileKind == null ? DATA_FILE : fileKind;
        if (!DATA_FILE.equals(this.fileKind) && !ROUTING_MODEL.equals(this.fileKind)) {
            throw new IllegalArgumentException("Unknown index file kind: " + fileKind);
        }
    }

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable GlobalIndexMeta globalIndexMeta,
            @Nullable String externalPath) {
        this(indexType, fileName, fileSize, rowCount, null, externalPath, globalIndexMeta);
    }

    public String indexType() {
        return indexType;
    }

    public String fileKind() {
        return fileKind;
    }

    public boolean isRoutingModel() {
        return ROUTING_MODEL.equals(fileKind);
    }

    @Nullable
    public GlobalIndexMeta globalIndexMeta() {
        return globalIndexMeta;
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
                && Objects.equals(fileKind, that.fileKind)
                && Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && rowCount == that.rowCount
                && Objects.equals(dvRanges, that.dvRanges)
                && Objects.equals(externalPath, that.externalPath)
                && Objects.equals(globalIndexMeta, that.globalIndexMeta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                indexType,
                fileName,
                fileSize,
                rowCount,
                dvRanges,
                externalPath,
                globalIndexMeta,
                fileKind);
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
