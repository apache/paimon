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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.memory.MemorySegmentWritable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Serializer for {@link BinaryRow}. */
public class BinaryRowSerializer extends AbstractRowDataSerializer<BinaryRow> {

    private static final long serialVersionUID = 1L;
    public static final int LENGTH_SIZE_IN_BYTES = 4;

    private final int numFields;
    private final int fixedLengthPartSize;

    public BinaryRowSerializer(int numFields) {
        this.numFields = numFields;
        this.fixedLengthPartSize = BinaryRow.calculateFixPartSizeInBytes(numFields);
    }

    @Override
    public BinaryRowSerializer duplicate() {
        return new BinaryRowSerializer(numFields);
    }

    public BinaryRow createInstance() {
        return new BinaryRow(numFields);
    }

    @Override
    public BinaryRow copy(BinaryRow from) {
        return from.copy();
    }

    @Override
    public void serialize(BinaryRow record, DataOutputView target) throws IOException {
        target.writeInt(record.getSizeInBytes());
        if (target instanceof MemorySegmentWritable) {
            serializeWithoutLength(record, (MemorySegmentWritable) target);
        } else {
            MemorySegmentUtils.copyToView(
                    record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
        }
    }

    @Override
    public BinaryRow deserialize(DataInputView source) throws IOException {
        BinaryRow row = new BinaryRow(numFields);
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        row.pointTo(MemorySegment.wrap(bytes), 0, length);
        return row;
    }

    public BinaryRow deserialize(BinaryRow reuse, DataInputView source) throws IOException {
        MemorySegment[] segments = reuse.getSegments();
        checkArgument(
                segments == null || (segments.length == 1 && reuse.getOffset() == 0),
                "Reuse BinaryRow should have no segments or only one segment and offset start at 0.");

        int length = source.readInt();
        if (segments == null || segments[0].size() < length) {
            segments = new MemorySegment[] {MemorySegment.wrap(new byte[length])};
        }
        source.readFully(segments[0].getArray(), 0, length);
        reuse.pointTo(segments, 0, length);
        return reuse;
    }

    @Override
    public int getArity() {
        return numFields;
    }

    @Override
    public BinaryRow toBinaryRow(BinaryRow rowData) throws IOException {
        return rowData;
    }

    // ============================ Page related operations ===================================

    @Override
    public BinaryRow createReuseInstance() {
        return new BinaryRow(numFields);
    }

    @Override
    public int serializeToPages(BinaryRow record, AbstractPagedOutputView headerLessView)
            throws IOException {
        int skip = checkSkipWriteForFixLengthPart(headerLessView);
        headerLessView.writeInt(record.getSizeInBytes());
        serializeWithoutLength(record, headerLessView);
        return skip;
    }

    private static void serializeWithoutLength(BinaryRow record, MemorySegmentWritable writable)
            throws IOException {
        if (record.getSegments().length == 1) {
            writable.write(record.getSegments()[0], record.getOffset(), record.getSizeInBytes());
        } else {
            serializeWithoutLengthSlow(record, writable);
        }
    }

    public static void serializeWithoutLengthSlow(BinaryRow record, MemorySegmentWritable out)
            throws IOException {
        int remainSize = record.getSizeInBytes();
        int posInSegOfRecord = record.getOffset();
        int segmentSize = record.getSegments()[0].size();
        for (MemorySegment segOfRecord : record.getSegments()) {
            int nWrite = Math.min(segmentSize - posInSegOfRecord, remainSize);
            assert nWrite > 0;
            out.write(segOfRecord, posInSegOfRecord, nWrite);

            // next new segment.
            posInSegOfRecord = 0;
            remainSize -= nWrite;
            if (remainSize == 0) {
                break;
            }
        }
        checkArgument(remainSize == 0);
    }

    @Override
    public BinaryRow deserializeFromPages(AbstractPagedInputView headerLessView)
            throws IOException {
        return deserializeFromPages(new BinaryRow(getArity()), headerLessView);
    }

    @Override
    public BinaryRow deserializeFromPages(BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkSkipReadForFixLengthPart(headerLessView);
        return deserialize(reuse, headerLessView);
    }

    @Override
    public BinaryRow mapFromPages(BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkSkipReadForFixLengthPart(headerLessView);
        pointTo(headerLessView.readInt(), reuse, headerLessView);
        return reuse;
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView headerLessView) throws IOException {
        checkSkipReadForFixLengthPart(headerLessView);
        headerLessView.skipBytes(headerLessView.readInt());
    }

    /**
     * Copy a binaryRow which stored in paged input view to output view.
     *
     * @param source source paged input view where the binary row stored
     * @param target the target output view.
     */
    public void copyFromPagesToView(AbstractPagedInputView source, DataOutputView target)
            throws IOException {
        checkSkipReadForFixLengthPart(source);
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    /**
     * Point row to memory segments with offset(in the AbstractPagedInputView) and length.
     *
     * @param length row length.
     * @param reuse reuse BinaryRow object.
     * @param headerLessView source memory segments container.
     */
    public void pointTo(int length, BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        if (length < 0) {
            throw new IOException(
                    String.format(
                            "Read unexpected bytes in source of positionInSegment[%d] and limitInSegment[%d]",
                            headerLessView.getCurrentPositionInSegment(),
                            headerLessView.getCurrentSegmentLimit()));
        }

        int remainInSegment =
                headerLessView.getCurrentSegmentLimit()
                        - headerLessView.getCurrentPositionInSegment();
        MemorySegment currSeg = headerLessView.getCurrentSegment();
        int currPosInSeg = headerLessView.getCurrentPositionInSegment();
        if (remainInSegment >= length) {
            // all in one segment, that's good.
            reuse.pointTo(currSeg, currPosInSeg, length);
            headerLessView.skipBytesToRead(length);
        } else {
            pointToMultiSegments(
                    reuse, headerLessView, length, length - remainInSegment, currSeg, currPosInSeg);
        }
    }

    private void pointToMultiSegments(
            BinaryRow reuse,
            AbstractPagedInputView source,
            int sizeInBytes,
            int remainLength,
            MemorySegment currSeg,
            int currPosInSeg)
            throws IOException {

        int segmentSize = currSeg.size();
        int div = remainLength / segmentSize;
        int remainder = remainLength - segmentSize * div; // equal to p % q
        int varSegSize = remainder == 0 ? div : div + 1;

        MemorySegment[] segments = new MemorySegment[varSegSize + 1];
        segments[0] = currSeg;
        for (int i = 1; i <= varSegSize; i++) {
            source.advance();
            segments[i] = source.getCurrentSegment();
        }

        // The remaining is 0. There is no next Segment at this time. The current Segment is
        // all the data of this row, so we need to skip segmentSize bytes to read. We can't
        // jump directly to the next Segment. Because maybe there are no segment in later.
        int remainLenInLastSeg = remainder == 0 ? segmentSize : remainder;
        source.skipBytesToRead(remainLenInLastSeg);
        reuse.pointTo(segments, currPosInSeg, sizeInBytes);
    }

    /**
     * We need skip bytes to write when the remain bytes of current segment is not enough to write
     * binary row fixed part. See {@link BinaryRow}.
     */
    private int checkSkipWriteForFixLengthPart(AbstractPagedOutputView out) throws IOException {
        // skip if there is no enough size.
        int available = out.getSegmentSize() - out.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            out.advance();
            return available;
        }
        return 0;
    }

    /**
     * We need skip bytes to read when the remain bytes of current segment is not enough to write
     * binary row fixed part. See {@link BinaryRow}.
     */
    public void checkSkipReadForFixLengthPart(AbstractPagedInputView source) throws IOException {
        // skip if there is no enough size.
        // Note: Use currentSegmentLimit instead of segmentSize.
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            source.advance();
        }
    }

    /** Return fixed part length to serialize one row. */
    public int getSerializedRowFixedPartLength() {
        return getFixedLengthPartSize() + LENGTH_SIZE_IN_BYTES;
    }

    public int getFixedLengthPartSize() {
        return fixedLengthPartSize;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BinaryRowSerializer
                && numFields == ((BinaryRowSerializer) obj).numFields;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(numFields);
    }
}
