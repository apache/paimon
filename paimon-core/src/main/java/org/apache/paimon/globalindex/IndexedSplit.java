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

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.FloatUtils;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Indexed split for global index. */
public class IndexedSplit implements Split {

    private static final long serialVersionUID = 1L;
    private static final long MAGIC = -938472394838495695L;
    private static final int VERSION = 1;

    private DataSplit split;
    private List<Range> rowRanges;
    @Nullable private float[] scores;

    public IndexedSplit(DataSplit split, List<Range> rowRanges, @Nullable float[] scores) {
        this.split = split;
        this.rowRanges = rowRanges;
        this.scores = scores;
    }

    public DataSplit dataSplit() {
        return split;
    }

    public List<Range> rowRanges() {
        return rowRanges;
    }

    @Nullable
    public float[] scores() {
        return scores;
    }

    @Override
    public long rowCount() {
        return rowRanges.stream().mapToLong(r -> r.to - r.from + 1).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexedSplit that = (IndexedSplit) o;

        return split.equals(that.split)
                && rowRanges.equals(that.rowRanges)
                && FloatUtils.equals(scores, that.scores, 0.00001f);
    }

    @Override
    public String toString() {
        return "IndexedSplit{"
                + "split="
                + split
                + ", rowRanges="
                + rowRanges
                + ", scores="
                + Arrays.toString(scores)
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(split, rowRanges, Arrays.hashCode(scores));
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        split.serialize(out);
        out.writeInt(rowRanges.size());
        for (Range range : rowRanges) {
            out.writeLong(range.from);
            out.writeLong(range.to);
        }
        if (scores != null) {
            out.writeBoolean(true);
            out.writeInt(scores.length);
            for (Float score : scores) {
                out.writeFloat(score);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public static IndexedSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        if (magic != MAGIC) {
            throw new IOException("Corrupted IndexedSplit: wrong magic number " + magic);
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported IndexedSplit version: " + version);
        }
        DataSplit split = DataSplit.deserialize(in);
        int rangeSize = in.readInt();
        List<Range> rowRanges = new java.util.ArrayList<>(rangeSize);
        for (int i = 0; i < rangeSize; i++) {
            long from = in.readLong();
            long to = in.readLong();
            rowRanges.add(new Range(from, to));
        }
        float[] scores = null;
        boolean hasScores = in.readBoolean();
        if (hasScores) {
            int scoresLength = in.readInt();
            scores = new float[scoresLength];
            for (int i = 0; i < scoresLength; i++) {
                scores[i] = in.readFloat();
            }
        }
        return new IndexedSplit(split, rowRanges, scores);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        IndexedSplit other = deserialize(new DataInputViewStreamWrapper(in));

        this.split = other.split;
        this.rowRanges = other.rowRanges;
        this.scores = other.scores;
    }
}
