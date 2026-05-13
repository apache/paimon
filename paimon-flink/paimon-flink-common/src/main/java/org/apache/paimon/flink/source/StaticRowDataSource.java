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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.SplittableIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/** A bounded source that returns a precomputed list of rows. */
class StaticRowDataSource
        implements Source<
                        RowData,
                        StaticRowDataSource.StaticRowsSplit,
                        Collection<StaticRowDataSource.StaticRowsSplit>>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final List<InternalRow> rows;
    private final org.apache.paimon.types.RowType paimonRowType;

    StaticRowDataSource(List<InternalRow> rows, org.apache.paimon.types.RowType paimonRowType) {
        this.rows = rows;
        this.paimonRowType = paimonRowType;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(paimonRowType));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, StaticRowsSplit> createReader(SourceReaderContext readerContext) {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<StaticRowsSplit, Collection<StaticRowsSplit>> createEnumerator(
            SplitEnumeratorContext<StaticRowsSplit> enumContext) {
        List<StaticRowsSplit> splits =
                rows.isEmpty()
                        ? Collections.emptyList()
                        : Collections.singletonList(new StaticRowsSplit("1", rows, 0));
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<StaticRowsSplit, Collection<StaticRowsSplit>> restoreEnumerator(
            SplitEnumeratorContext<StaticRowsSplit> enumContext,
            Collection<StaticRowsSplit> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<StaticRowsSplit> getSplitSerializer() {
        return new SplitSerializer(paimonRowType);
    }

    @Override
    public SimpleVersionedSerializer<Collection<StaticRowsSplit>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer(paimonRowType);
    }

    static class StaticRowsSplit implements IteratorSourceSplit<RowData, StaticRowsIterator> {

        private static final long serialVersionUID = 1L;

        private final String splitId;
        private final List<InternalRow> rows;
        private final int nextIndex;

        StaticRowsSplit(String splitId, List<InternalRow> rows, int nextIndex) {
            this.splitId = splitId;
            this.rows = rows;
            this.nextIndex = nextIndex;
        }

        @Override
        public String splitId() {
            return splitId;
        }

        @Override
        public StaticRowsIterator getIterator() {
            return new StaticRowsIterator(rows, nextIndex);
        }

        @Override
        public IteratorSourceSplit<RowData, StaticRowsIterator> getUpdatedSplitForIterator(
                StaticRowsIterator iterator) {
            return new StaticRowsSplit(splitId, rows, iterator.nextIndex());
        }
    }

    static class StaticRowsIterator extends SplittableIterator<RowData> {

        private static final long serialVersionUID = 1L;

        private final List<InternalRow> rows;
        private int nextIndex;

        StaticRowsIterator(List<InternalRow> rows, int nextIndex) {
            this.rows = rows;
            this.nextIndex = nextIndex;
        }

        int nextIndex() {
            return nextIndex;
        }

        @Override
        public boolean hasNext() {
            return nextIndex < rows.size();
        }

        @Override
        public RowData next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new FlinkRowData(rows.get(nextIndex++));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StaticRowsIterator[] split(int numPartitions) {
            if (numPartitions < 1) {
                throw new IllegalArgumentException("The number of partitions must be at least 1.");
            }
            return new StaticRowsIterator[] {new StaticRowsIterator(rows, nextIndex)};
        }

        @Override
        public int getMaximumNumberOfSplits() {
            return rows.size() - nextIndex;
        }
    }

    private static class SplitSerializer implements SimpleVersionedSerializer<StaticRowsSplit> {

        private static final int CURRENT_VERSION = 1;

        private final InternalRowSerializer rowSerializer;

        private SplitSerializer(org.apache.paimon.types.RowType rowType) {
            this.rowSerializer = new InternalRowSerializer(rowType);
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(StaticRowsSplit split) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
            out.writeUTF(split.splitId);
            out.writeInt(split.nextIndex);
            out.writeInt(split.rows.size());
            for (InternalRow row : split.rows) {
                rowSerializer.serialize(row, out);
            }
            return baos.toByteArray();
        }

        @Override
        public StaticRowsSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            DataInputViewStreamWrapper in =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized));
            String splitId = in.readUTF();
            int nextIndex = in.readInt();
            int numRows = in.readInt();
            List<InternalRow> rows = new ArrayList<>(numRows);
            for (int i = 0; i < numRows; i++) {
                rows.add(rowSerializer.deserialize(in));
            }
            return new StaticRowsSplit(splitId, rows, nextIndex);
        }
    }

    private static class CheckpointSerializer
            implements SimpleVersionedSerializer<Collection<StaticRowsSplit>> {

        private static final int CURRENT_VERSION = 1;

        private final SplitSerializer splitSerializer;

        private CheckpointSerializer(org.apache.paimon.types.RowType rowType) {
            this.splitSerializer = new SplitSerializer(rowType);
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(Collection<StaticRowsSplit> checkpoint) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
            out.writeInt(checkpoint.size());
            for (StaticRowsSplit split : checkpoint) {
                byte[] bytes = splitSerializer.serialize(split);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            return baos.toByteArray();
        }

        @Override
        public Collection<StaticRowsSplit> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            DataInputViewStreamWrapper in =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized));
            int numSplits = in.readInt();
            List<StaticRowsSplit> splits = new ArrayList<>(numSplits);
            for (int i = 0; i < numSplits; i++) {
                byte[] bytes = new byte[in.readInt()];
                in.readFully(bytes);
                splits.add(splitSerializer.deserialize(CURRENT_VERSION, bytes));
            }
            return splits;
        }
    }
}
