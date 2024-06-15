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

import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Metadata of index file. */
public class IndexFileMeta {

    private final String indexType;
    private final String fileName;
    private final long fileSize;
    private final long rowCount;

    /**
     * Metadata only used by {@link DeletionVectorsIndexFile}, use LinkedHashMap to ensure that the
     * order of DeletionVectorRanges and the written DeletionVectors is consistent.
     */
    private final @Nullable LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorsRanges;

    public IndexFileMeta(String indexType, String fileName, long fileSize, long rowCount) {
        this(indexType, fileName, fileSize, rowCount, null);
    }

    public IndexFileMeta(
            String indexType,
            String fileName,
            long fileSize,
            long rowCount,
            @Nullable LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorsRanges) {
        this.indexType = indexType;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.deletionVectorsRanges = deletionVectorsRanges;
    }

    public String indexType() {
        return indexType;
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

    public @Nullable LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorsRanges() {
        return deletionVectorsRanges;
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
                && Objects.equals(deletionVectorsRanges, that.deletionVectorsRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexType, fileName, fileSize, rowCount, deletionVectorsRanges);
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
                + ", deletionVectorsRanges="
                + deletionVectorsRanges
                + '}';
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_INDEX_TYPE", newStringType(false)));
        fields.add(new DataField(1, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(2, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(3, "_ROW_COUNT", new BigIntType(false)));
        fields.add(
                new DataField(
                        4,
                        "_DELETION_VECTORS_RANGES",
                        new ArrayType(
                                true,
                                RowType.of(
                                        newStringType(false),
                                        new IntType(false),
                                        new IntType(false)))));
        return new RowType(fields);
    }
}
