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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * A complete data split paired with an index query to execute when the reader opens the split.
 *
 * <p>Recovery retains this split and re-evaluates the same index files before skipping previously
 * read records. Index evaluation produces an {@link IndexedSplit} for the existing data read path.
 */
public class IndexQuerySplit implements Split {

    private static final long serialVersionUID = 1L;

    /** Binary serialization format version. */
    private static final int VERSION = 1;

    /** Keeps the snapshot ID, complete column-merge file groups and deletion metadata. */
    private DataSplit dataSplit;

    /**
     * Index query pruned to this split's data ranges, with original index row-ID offsets intact.
     */
    private GlobalIndexQuery indexQuery;

    /** Planning-time index options, retained so recovery uses the same query configuration. */
    private Map<String, String> indexOptions;

    /**
     * Global row-ID ranges to scan without index pruning, clipped to this split; empty in fast
     * mode.
     */
    private List<Range> unindexedRanges;

    IndexQuerySplit(
            DataSplit dataSplit,
            GlobalIndexQuery indexQuery,
            Map<String, String> indexOptions,
            List<Range> unindexedRanges) {
        this.dataSplit = dataSplit;
        this.indexQuery = indexQuery;
        this.indexOptions = new HashMap<>(indexOptions);
        this.unindexedRanges = new ArrayList<>(unindexedRanges);
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    public IndexedSplit evaluate(FileIO fileIO) throws IOException {
        List<Range> ranges =
                GlobalIndexBuilderUtils.calcRowRanges(Collections.singletonList(dataSplit));
        GlobalIndexResult matches =
                indexQuery.evaluate(fileIO, Options.fromMap(indexOptions), ranges);
        List<Range> candidates = new ArrayList<>(matches.results().toRangeList());
        candidates.addAll(unindexedRanges);
        return new IndexedSplit(dataSplit, Range.sortAndMergeOverlap(candidates, true), null);
    }

    @Override
    public long rowCount() {
        return dataSplit.rowCount();
    }

    @Override
    public OptionalLong mergedRowCount() {
        // The number of rows surviving the index predicate is not known at planning time.
        return OptionalLong.empty();
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeInt(VERSION);
        dataSplit.serialize(out);
        indexQuery.serialize(out);
        out.writeInt(indexOptions.size());
        for (Map.Entry<String, String> entry : indexOptions.entrySet()) {
            GlobalIndexQuery.writeString(out, entry.getKey());
            GlobalIndexQuery.writeString(out, entry.getValue());
        }
        out.writeInt(unindexedRanges.size());
        for (Range range : unindexedRanges) {
            out.writeLong(range.from);
            out.writeLong(range.to);
        }
    }

    public static IndexQuerySplit deserialize(DataInputView in) throws IOException {
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported IndexQuerySplit version: " + version);
        }
        DataSplit dataSplit = DataSplit.deserialize(in);
        GlobalIndexQuery query = GlobalIndexQuery.deserialize(in);
        Map<String, String> options = new HashMap<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            options.put(GlobalIndexQuery.readString(in), GlobalIndexQuery.readString(in));
        }
        List<Range> unindexed = new ArrayList<>();
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            unindexed.add(new Range(in.readLong(), in.readLong()));
        }
        return new IndexQuerySplit(dataSplit, query, options, unindexed);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException {
        IndexQuerySplit restored = deserialize(new DataInputViewStreamWrapper(in));
        this.dataSplit = restored.dataSplit;
        this.indexQuery = restored.indexQuery;
        this.indexOptions = restored.indexOptions;
        this.unindexedRanges = restored.unindexedRanges;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexQuerySplit)) {
            return false;
        }
        IndexQuerySplit that = (IndexQuerySplit) obj;
        return dataSplit.equals(that.dataSplit)
                && indexQuery.equals(that.indexQuery)
                && indexOptions.equals(that.indexOptions)
                && unindexedRanges.equals(that.unindexedRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSplit, indexQuery, indexOptions, unindexedRanges);
    }
}
