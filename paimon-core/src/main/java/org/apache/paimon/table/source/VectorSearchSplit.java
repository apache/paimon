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
import java.io.Serializable;
import java.util.List;

/** Split of vector search. */
public class VectorSearchSplit implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private static final ThreadLocal<IndexFileMetaSerializer> INDEX_SERIALIZER =
            ThreadLocal.withInitial(IndexFileMetaSerializer::new);

    private transient long rowRangeStart;
    private transient long rowRangeEnd;
    private transient List<IndexFileMeta> vectorIndexFiles;
    private transient List<IndexFileMeta> scalarIndexFiles;

    public VectorSearchSplit(
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> vectorIndexFiles,
            List<IndexFileMeta> scalarIndexFiles) {
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.vectorIndexFiles = vectorIndexFiles;
        this.scalarIndexFiles = scalarIndexFiles;
    }

    public long rowRangeStart() {
        return rowRangeStart;
    }

    public long rowRangeEnd() {
        return rowRangeEnd;
    }

    public List<IndexFileMeta> vectorIndexFiles() {
        return vectorIndexFiles;
    }

    public List<IndexFileMeta> scalarIndexFiles() {
        return scalarIndexFiles;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(VERSION);
        IndexFileMetaSerializer serializer = INDEX_SERIALIZER.get();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeLong(rowRangeStart);
        view.writeLong(rowRangeEnd);
        serializer.serializeList(vectorIndexFiles, view);
        serializer.serializeList(scalarIndexFiles, view);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported VectorSearchSplit version: " + version);
        }
        IndexFileMetaSerializer serializer = INDEX_SERIALIZER.get();
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        this.rowRangeStart = view.readLong();
        this.rowRangeEnd = view.readLong();
        this.vectorIndexFiles = serializer.deserializeList(view);
        this.scalarIndexFiles = serializer.deserializeList(view);
    }
}
