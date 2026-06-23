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

package org.apache.paimon.table.source;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Split to scan raw vectors. */
public class RawVectorSearchSplit extends VectorSearchSplit {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private static final ThreadLocal<IndexFileMetaSerializer> INDEX_SERIALIZER =
            ThreadLocal.withInitial(IndexFileMetaSerializer::new);

    private final List<Range> rowRanges;
    @Nullable private final String indexType;
    private transient List<IndexFileMeta> scalarIndexFiles;

    public RawVectorSearchSplit(
            List<Range> rowRanges,
            List<IndexFileMeta> scalarIndexFiles,
            @Nullable String indexType) {
        this.rowRanges = Collections.unmodifiableList(new ArrayList<>(rowRanges));
        this.scalarIndexFiles = Collections.unmodifiableList(new ArrayList<>(scalarIndexFiles));
        this.indexType = indexType;
    }

    public List<Range> rowRanges() {
        return rowRanges;
    }

    public List<IndexFileMeta> scalarIndexFiles() {
        return scalarIndexFiles;
    }

    @Nullable
    public String indexType() {
        return indexType;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(VERSION);
        IndexFileMetaSerializer serializer = INDEX_SERIALIZER.get();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serializer.serializeList(scalarIndexFiles, view);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported RawVectorSearchSplit version: " + version);
        }
        IndexFileMetaSerializer serializer = INDEX_SERIALIZER.get();
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        this.scalarIndexFiles =
                Collections.unmodifiableList(new ArrayList<>(serializer.deserializeList(view)));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RawVectorSearchSplit that = (RawVectorSearchSplit) o;
        return Objects.equals(rowRanges, that.rowRanges)
                && Objects.equals(scalarIndexFiles, that.scalarIndexFiles)
                && Objects.equals(indexType, that.indexType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowRanges, scalarIndexFiles, indexType);
    }

    @Override
    public String toString() {
        return "RawVectorSearchSplit{"
                + "rowRanges="
                + rowRanges
                + ", scalarIndexFiles="
                + scalarIndexFiles
                + ", indexType='"
                + indexType
                + '\''
                + '}';
    }
}
