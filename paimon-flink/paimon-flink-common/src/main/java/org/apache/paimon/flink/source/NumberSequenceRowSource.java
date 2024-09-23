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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.SplittableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A data source that produces a sequence of numbers (longs) to {@link RowData}. */
public class NumberSequenceRowSource
        implements Source<
                        RowData,
                        NumberSequenceRowSource.NumberSequenceSplit,
                        Collection<NumberSequenceRowSource.NumberSequenceSplit>>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    /** The starting number in the sequence, inclusive. */
    private final long from;

    /** The end number in the sequence, inclusive. */
    private final long to;

    /**
     * Creates a new {@code NumberSequenceSource} that produces parallel sequences covering the
     * range {@code from} to {@code to} (both boundaries are inclusive).
     */
    public NumberSequenceRowSource(long from, long to) {
        checkArgument(from <= to, "'from' must be <= 'to'");
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    // ------------------------------------------------------------------------
    //  source methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(RowType.of(new BigIntType(false)));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, NumberSequenceSplit> createReader(
            SourceReaderContext readerContext) {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> createEnumerator(
            final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {

        final List<NumberSequenceSplit> splits =
                splitNumberRange(from, to, enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> restoreEnumerator(
            final SplitEnumeratorContext<NumberSequenceSplit> enumContext,
            Collection<NumberSequenceSplit> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NumberSequenceSplit> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    protected List<NumberSequenceSplit> splitNumberRange(long from, long to, int numSplits) {
        final NumberSequenceIterator[] subSequences =
                new NumberSequenceIterator(from, to).split(numSplits);
        final ArrayList<NumberSequenceSplit> splits = new ArrayList<>(subSequences.length);

        int splitId = 1;
        for (NumberSequenceIterator seq : subSequences) {
            if (seq.hasNext()) {
                splits.add(
                        new NumberSequenceSplit(
                                String.valueOf(splitId++), seq.getCurrent(), seq.getTo()));
            }
        }

        return splits;
    }

    // ------------------------------------------------------------------------
    //  splits & checkpoint
    // ------------------------------------------------------------------------

    /** A split of the source, representing a number sub-sequence. */
    public static class NumberSequenceSplit
            implements IteratorSourceSplit<
                    RowData, NumberSequenceRowSource.NumberSequenceIterator> {

        private final String splitId;
        private final long from;
        private final long to;

        public NumberSequenceSplit(String splitId, long from, long to) {
            checkArgument(from <= to, "'from' must be <= 'to'");
            this.splitId = checkNotNull(splitId);
            this.from = from;
            this.to = to;
        }

        @Override
        public String splitId() {
            return splitId;
        }

        public long from() {
            return from;
        }

        public long to() {
            return to;
        }

        @Override
        public NumberSequenceRowSource.NumberSequenceIterator getIterator() {
            return new NumberSequenceRowSource.NumberSequenceIterator(from, to);
        }

        @Override
        public IteratorSourceSplit<RowData, NumberSequenceRowSource.NumberSequenceIterator>
                getUpdatedSplitForIterator(
                        final NumberSequenceRowSource.NumberSequenceIterator iterator) {
            return new NumberSequenceSplit(splitId, iterator.getCurrent(), iterator.getTo());
        }

        @Override
        public String toString() {
            return String.format("NumberSequenceSplit [%d, %d] (%s)", from, to, splitId);
        }
    }

    private static final class SplitSerializer
            implements SimpleVersionedSerializer<NumberSequenceSplit> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(NumberSequenceSplit split) throws IOException {
            checkArgument(
                    split.getClass() == NumberSequenceSplit.class, "cannot serialize subclasses");

            // We will serialize 2 longs (16 bytes) plus the UFT representation of the string (2 +
            // length)
            final DataOutputSerializer out =
                    new DataOutputSerializer(split.splitId().length() + 18);
            serializeV1(out, split);
            return out.getCopyOfBuffer();
        }

        @Override
        public NumberSequenceSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return deserializeV1(in);
        }

        static void serializeV1(DataOutputView out, NumberSequenceSplit split) throws IOException {
            out.writeUTF(split.splitId());
            out.writeLong(split.from());
            out.writeLong(split.to());
        }

        static NumberSequenceSplit deserializeV1(DataInputView in) throws IOException {
            return new NumberSequenceSplit(in.readUTF(), in.readLong(), in.readLong());
        }
    }

    private static final class CheckpointSerializer
            implements SimpleVersionedSerializer<Collection<NumberSequenceSplit>> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(Collection<NumberSequenceSplit> checkpoint) throws IOException {
            // Each split needs 2 longs (16 bytes) plus the UFT representation of the string (2 +
            // length)
            // Assuming at most 4 digit split IDs, 22 bytes per split avoids any intermediate array
            // resizing.
            // plus four bytes for the length field
            final DataOutputSerializer out = new DataOutputSerializer(checkpoint.size() * 22 + 4);
            out.writeInt(checkpoint.size());
            for (NumberSequenceSplit split : checkpoint) {
                SplitSerializer.serializeV1(out, split);
            }
            return out.getCopyOfBuffer();
        }

        @Override
        public Collection<NumberSequenceSplit> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final int num = in.readInt();
            final ArrayList<NumberSequenceSplit> result = new ArrayList<>(num);
            for (int remaining = num; remaining > 0; remaining--) {
                result.add(SplitSerializer.deserializeV1(in));
            }
            return result;
        }
    }

    private static class NumberSequenceIterator extends SplittableIterator<RowData> {

        private static final long serialVersionUID = 1L;

        /** The last number returned by the iterator. */
        private final long to;

        /** The next number to be returned. */
        private long current;

        /**
         * Creates a new splittable iterator, returning the range [from, to]. Both boundaries of the
         * interval are inclusive.
         *
         * @param from The first number returned by the iterator.
         * @param to The last number returned by the iterator.
         */
        public NumberSequenceIterator(long from, long to) {
            if (from > to) {
                throw new IllegalArgumentException(
                        "The 'to' value must not be smaller than the 'from' value.");
            }

            this.current = from;
            this.to = to;
        }

        /**
         * Internal constructor to allow for empty iterators.
         *
         * @param from The first number returned by the iterator.
         * @param to The last number returned by the iterator.
         * @param unused A dummy parameter to disambiguate the constructor.
         */
        private NumberSequenceIterator(long from, long to, boolean unused) {
            this.current = from;
            this.to = to;
        }

        public long getCurrent() {
            return this.current;
        }

        public long getTo() {
            return this.to;
        }

        @Override
        public boolean hasNext() {
            return current <= to;
        }

        @Override
        public RowData next() {
            if (current <= to) {
                return GenericRowData.of(current++);
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumberSequenceIterator[] split(int numPartitions) {
            if (numPartitions < 1) {
                throw new IllegalArgumentException("The number of partitions must be at least 1.");
            }

            if (numPartitions == 1) {
                return new NumberSequenceIterator[] {new NumberSequenceIterator(current, to)};
            }

            // here, numPartitions >= 2 !!!

            long elementsPerSplit;

            if (to - current + 1 >= 0) {
                elementsPerSplit = (to - current + 1) / numPartitions;
            } else {
                // long overflow of the range.
                // we compute based on half the distance, to prevent the overflow.
                // in most cases it holds that: current < 0 and to > 0, except for: to == 0 and
                // current
                // == Long.MIN_VALUE
                // the later needs a special case
                final long halfDiff; // must be positive

                if (current == Long.MIN_VALUE) {
                    // this means to >= 0
                    halfDiff = (Long.MAX_VALUE / 2 + 1) + to / 2;
                } else {
                    long posFrom = -current;
                    if (posFrom > to) {
                        halfDiff = to + ((posFrom - to) / 2);
                    } else {
                        halfDiff = posFrom + ((to - posFrom) / 2);
                    }
                }
                elementsPerSplit = halfDiff / numPartitions * 2;
            }

            if (elementsPerSplit < Long.MAX_VALUE) {
                // figure out how many get one in addition
                long numWithExtra = -(elementsPerSplit * numPartitions) + to - current + 1;

                // based on rounding errors, we may have lost one)
                if (numWithExtra > numPartitions) {
                    elementsPerSplit++;
                    numWithExtra -= numPartitions;

                    if (numWithExtra > numPartitions) {
                        throw new RuntimeException(
                                "Bug in splitting logic. To much rounding loss.");
                    }
                }

                NumberSequenceIterator[] iters = new NumberSequenceIterator[numPartitions];
                long curr = current;
                int i = 0;
                for (; i < numWithExtra; i++) {
                    long next = curr + elementsPerSplit + 1;
                    iters[i] = new NumberSequenceIterator(curr, next - 1);
                    curr = next;
                }
                for (; i < numPartitions; i++) {
                    long next = curr + elementsPerSplit;
                    iters[i] = new NumberSequenceIterator(curr, next - 1, true);
                    curr = next;
                }

                return iters;
            } else {
                // this can only be the case when there are two partitions
                if (numPartitions != 2) {
                    throw new RuntimeException("Bug in splitting logic.");
                }

                return new NumberSequenceIterator[] {
                    new NumberSequenceIterator(current, current + elementsPerSplit),
                    new NumberSequenceIterator(current + elementsPerSplit, to)
                };
            }
        }

        @Override
        public int getMaximumNumberOfSplits() {
            if (to >= Integer.MAX_VALUE
                    || current <= Integer.MIN_VALUE
                    || to - current + 1 >= Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            } else {
                return (int) (to - current + 1);
            }
        }
    }
}
