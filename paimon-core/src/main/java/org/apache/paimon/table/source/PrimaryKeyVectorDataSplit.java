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

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Selected physical row positions and vector scores in one primary-key table data file. */
public class PrimaryKeyVectorDataSplit implements Split {

    private static final long serialVersionUID = 1L;
    private static final long MAGIC = 0x504B5653504C4954L; // "PKVSPLIT"
    private static final int VERSION = 1;

    private DataSplit dataSplit;
    private RoaringBitmap32 rowPositions;
    private Map<Integer, Float> scores;

    PrimaryKeyVectorDataSplit(
            DataSplit dataSplit, RoaringBitmap32 rowPositions, Map<Integer, Float> scores) {
        checkArgument(
                dataSplit.dataFiles().size() == 1,
                "Primary-key vector data split must contain exactly one data file.");
        checkArgument(
                !rowPositions.isEmpty(),
                "Primary-key vector data split must select at least one row position.");
        checkArgument(
                rowPositions.getCardinality() == scores.size(),
                "Primary-key vector positions and scores must have the same size.");
        Iterator<Integer> positionIterator = rowPositions.iterator();
        while (positionIterator.hasNext()) {
            int rowPosition = positionIterator.next();
            checkArgument(
                    rowPosition >= 0 && rowPosition < dataSplit.dataFiles().get(0).rowCount(),
                    "Selected row position %s is outside data file %s row count %s.",
                    rowPosition,
                    dataSplit.dataFiles().get(0).fileName(),
                    dataSplit.dataFiles().get(0).rowCount());
            checkArgument(
                    scores.containsKey(rowPosition),
                    "Selected row position %s has no vector score.",
                    rowPosition);
        }
        this.dataSplit = dataSplit;
        this.rowPositions = rowPositions.clone();
        this.scores = Collections.unmodifiableMap(new HashMap<>(scores));
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    public RoaringBitmap32 rowPositions() {
        return rowPositions.clone();
    }

    public float score(int rowPosition) {
        Float score = scores.get(rowPosition);
        checkArgument(score != null, "Row position %s is not selected.", rowPosition);
        return score;
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        dataSplit.serialize(out);
        rowPositions.serialize(out);
        out.writeInt(scores.size());
        Iterator<Integer> positions = rowPositions.iterator();
        while (positions.hasNext()) {
            int position = positions.next();
            out.writeInt(position);
            out.writeFloat(scores.get(position));
        }
    }

    public static PrimaryKeyVectorDataSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        if (magic != MAGIC) {
            throw new IOException(
                    "Corrupted PrimaryKeyVectorDataSplit: wrong magic number " + magic);
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported PrimaryKeyVectorDataSplit version: " + version);
        }
        DataSplit dataSplit = DataSplit.deserialize(in);
        RoaringBitmap32 positions = new RoaringBitmap32();
        positions.deserialize(in);
        int scoreCount = in.readInt();
        Map<Integer, Float> scores = new HashMap<>(scoreCount);
        for (int i = 0; i < scoreCount; i++) {
            scores.put(in.readInt(), in.readFloat());
        }
        return new PrimaryKeyVectorDataSplit(dataSplit, positions, scores);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        PrimaryKeyVectorDataSplit restored = deserialize(new DataInputViewStreamWrapper(in));
        this.dataSplit = restored.dataSplit;
        this.rowPositions = restored.rowPositions;
        this.scores = restored.scores;
    }

    @Override
    public long rowCount() {
        return rowPositions.getCardinality();
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.of(rowCount());
    }

    @Override
    public Optional<List<DeletionFile>> deletionFiles() {
        return dataSplit.deletionFiles();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrimaryKeyVectorDataSplit that = (PrimaryKeyVectorDataSplit) o;
        return dataSplit.equals(that.dataSplit)
                && rowPositions.equals(that.rowPositions)
                && scores.equals(that.scores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSplit, rowPositions, scores);
    }
}
