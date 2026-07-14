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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** All active data files and vector payloads for one snapshot bucket. */
public class BucketVectorSearchSplit extends VectorSearchSplit {

    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;

    private DataSplit dataSplit;
    private transient List<IndexFileMeta> payloadFiles;
    private Map<String, List<Range>> rowRangesByFile;

    public BucketVectorSearchSplit(DataSplit dataSplit, List<IndexFileMeta> payloadFiles) {
        this(dataSplit, payloadFiles, Collections.emptyMap());
    }

    public BucketVectorSearchSplit(
            DataSplit dataSplit,
            List<IndexFileMeta> payloadFiles,
            Map<String, List<Range>> rowRangesByFile) {
        this.dataSplit = dataSplit;
        for (IndexFileMeta payload : payloadFiles) {
            checkArgument(
                    payload.globalIndexMeta() != null
                            && payload.globalIndexMeta().sourceMeta() != null,
                    "Primary-key vector payload %s has no source metadata.",
                    payload.fileName());
        }
        this.payloadFiles = Collections.unmodifiableList(new ArrayList<>(payloadFiles));
        Map<String, List<Range>> ranges = new LinkedHashMap<>();
        for (Map.Entry<String, List<Range>> entry : rowRangesByFile.entrySet()) {
            ranges.put(
                    entry.getKey(),
                    Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        this.rowRangesByFile = Collections.unmodifiableMap(ranges);
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    public List<IndexFileMeta> payloadFiles() {
        return payloadFiles;
    }

    public Map<String, List<Range>> rowRangesByFile() {
        return rowRangesByFile;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(VERSION);
        out.writeInt(payloadFiles.size());
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        for (IndexFileMeta payloadFile : payloadFiles) {
            serializer.serialize(payloadFile, new DataOutputViewStreamWrapper(out));
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported BucketVectorSearchSplit version: " + version);
        }
        int payloadFileCount = in.readInt();
        if (payloadFileCount < 0) {
            throw new IOException("Negative primary-key vector payload file count.");
        }
        List<IndexFileMeta> payloadFiles = new ArrayList<>(payloadFileCount);
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        for (int i = 0; i < payloadFileCount; i++) {
            payloadFiles.add(serializer.deserialize(new DataInputViewStreamWrapper(in)));
        }
        this.payloadFiles = Collections.unmodifiableList(payloadFiles);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketVectorSearchSplit that = (BucketVectorSearchSplit) o;
        return Objects.equals(dataSplit, that.dataSplit)
                && Objects.equals(payloadFiles, that.payloadFiles)
                && Objects.equals(rowRangesByFile, that.rowRangesByFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSplit, payloadFiles, rowRangesByFile);
    }
}
