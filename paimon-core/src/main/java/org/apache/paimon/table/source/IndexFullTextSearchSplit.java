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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Split to read full-text index files. */
public class IndexFullTextSearchSplit extends FullTextSearchSplit {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private static final ThreadLocal<IndexFileMetaSerializer> INDEX_SERIALIZER =
            ThreadLocal.withInitial(IndexFileMetaSerializer::new);

    private String columnName;
    private long rowRangeStart;
    private long rowRangeEnd;
    private transient List<IndexFileMeta> fullTextIndexFiles;

    public IndexFullTextSearchSplit(
            long rowRangeStart, long rowRangeEnd, List<IndexFileMeta> fullTextIndexFiles) {
        this(null, rowRangeStart, rowRangeEnd, fullTextIndexFiles);
    }

    public IndexFullTextSearchSplit(
            String columnName,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> fullTextIndexFiles) {
        this.columnName = columnName;
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.fullTextIndexFiles = Collections.unmodifiableList(new ArrayList<>(fullTextIndexFiles));
    }

    public String columnName() {
        return columnName;
    }

    public long rowRangeStart() {
        return rowRangeStart;
    }

    public long rowRangeEnd() {
        return rowRangeEnd;
    }

    public List<IndexFileMeta> fullTextIndexFiles() {
        return fullTextIndexFiles;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(VERSION);
        IndexFileMetaSerializer serializer = INDEX_SERIALIZER.get();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serializer.serializeList(fullTextIndexFiles, view);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported IndexFullTextSearchSplit version: " + version);
        }
        IndexFileMetaSerializer serializer = INDEX_SERIALIZER.get();
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        this.fullTextIndexFiles =
                Collections.unmodifiableList(new ArrayList<>(serializer.deserializeList(view)));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexFullTextSearchSplit that = (IndexFullTextSearchSplit) o;
        return rowRangeStart == that.rowRangeStart
                && rowRangeEnd == that.rowRangeEnd
                && Objects.equals(columnName, that.columnName)
                && Objects.equals(fullTextIndexFiles, that.fullTextIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, rowRangeStart, rowRangeEnd, fullTextIndexFiles);
    }

    @Override
    public String toString() {
        return "IndexFullTextSearchSplit{"
                + "columnName='"
                + columnName
                + '\''
                + ", rowRangeStart="
                + rowRangeStart
                + ", rowRangeEnd="
                + rowRangeEnd
                + ", fullTextIndexFiles="
                + fullTextIndexFiles
                + '}';
    }
}
