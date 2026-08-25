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
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
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
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** All active data files and vector payloads for one snapshot bucket. */
public class BucketVectorSearchSplit extends VectorSearchSplit {

    private static final long serialVersionUID = 1L;

    private static final long MAGIC = 0x504B5653504C4954L;
    private static final int VERSION = 1;

    private DataSplit dataSplit;
    private List<IndexFileMeta> payloadFiles;
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

    /**
     * Serialize to the byte form a reader outside the JVM consumes, following {@code IndexedSplit}:
     * magic and version, then the nested {@link DataSplit}, then this split's own state.
     *
     * <p>Row-range entries are written sorted by file name, so two splits that compare equal
     * serialize to the same bytes even though the map's iteration order is its construction order.
     *
     * <p>{@link #VERSION} pins this envelope and the layout of what it nests, not the nested bytes
     * themselves: {@link DataSplit#serialize} carries its own version, while {@link
     * IndexFileMetaSerializer} carries none, so a change to {@code IndexFileMeta.SCHEMA} has to
     * bump this version too.
     */
    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        dataSplit.serialize(out);
        new IndexFileMetaSerializer().serializeList(payloadFiles, out);
        out.writeInt(rowRangesByFile.size());
        for (Map.Entry<String, List<Range>> entry : new TreeMap<>(rowRangesByFile).entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Range range : entry.getValue()) {
                out.writeLong(range.from);
                out.writeLong(range.to);
            }
        }
    }

    /** Reverse of {@link #serialize(DataOutputView)}. */
    public static BucketVectorSearchSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        if (magic != MAGIC) {
            throw new IOException("Corrupted BucketVectorSearchSplit: wrong magic number " + magic);
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported BucketVectorSearchSplit version: " + version);
        }
        DataSplit dataSplit = DataSplit.deserialize(in);
        List<IndexFileMeta> payloadFiles = new IndexFileMetaSerializer().deserializeList(in);
        int rangeFileCount = in.readInt();
        Map<String, List<Range>> rowRangesByFile = new LinkedHashMap<>();
        for (int i = 0; i < rangeFileCount; i++) {
            String fileName = in.readUTF();
            int rangeCount = in.readInt();
            List<Range> ranges = new ArrayList<>(rangeCount);
            for (int j = 0; j < rangeCount; j++) {
                ranges.add(new Range(in.readLong(), in.readLong()));
            }
            rowRangesByFile.put(fileName, ranges);
        }
        return new BucketVectorSearchSplit(dataSplit, payloadFiles, rowRangesByFile);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        BucketVectorSearchSplit other = deserialize(new DataInputViewStreamWrapper(in));

        this.dataSplit = other.dataSplit;
        this.payloadFiles = other.payloadFiles;
        this.rowRangesByFile = other.rowRangesByFile;
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

    @Override
    public String toString() {
        return "BucketVectorSearchSplit{"
                + "dataSplit="
                + dataSplit
                + ", payloadFiles="
                + payloadFiles
                + ", rowRangesByFile="
                + rowRangesByFile
                + '}';
    }
}
