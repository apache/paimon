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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.lookup.FilePosition;
import org.apache.paimon.mergetree.lookup.PositionedKeyValue;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Wrapper for {@link MergeFunction}s to produce changelog by lookup during the compaction involving
 * level 0 files.
 *
 * <p>Changelog records are generated in the process of the level-0 file participating in the
 * compaction, if during the compaction processing:
 *
 * <ul>
 *   <li>Without level-0 records, no changelog.
 *   <li>With level-0 record, with level-x (x > 0) record, level-x record should be BEFORE, level-0
 *       should be AFTER.
 *   <li>With level-0 record, without level-x record, need to lookup the history value of the upper
 *       level as BEFORE.
 * </ul>
 */
public class LookupChangelogMergeFunctionWrapper<T>
        implements MergeFunctionWrapper<ChangelogResult> {

    private final LookupMergeFunction mergeFunction;
    private final Function<InternalRow, T> lookup;

    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();
    @Nullable private final RecordEqualiser valueEqualiser;
    private final LookupStrategy lookupStrategy;
    private final @Nullable BucketedDvMaintainer deletionVectorsMaintainer;
    private final Comparator<KeyValue> comparator;
    @Nullable private final SequenceFieldOverwriteRow reusedOverwriteRow;

    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            @Nullable RecordEqualiser valueEqualiser,
            LookupStrategy lookupStrategy,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
        this(
                mergeFunctionFactory,
                lookup,
                valueEqualiser,
                lookupStrategy,
                deletionVectorsMaintainer,
                userDefinedSeqComparator,
                null);
    }

    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            @Nullable RecordEqualiser valueEqualiser,
            LookupStrategy lookupStrategy,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator,
            @Nullable int[] preserveFieldIndices) {
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof LookupMergeFunction,
                "Merge function should be a LookupMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        if (lookupStrategy.deletionVector) {
            checkArgument(
                    deletionVectorsMaintainer != null,
                    "deletionVectorsMaintainer should not be null, there is a bug.");
        }
        this.mergeFunction = (LookupMergeFunction) mergeFunction;
        this.lookup = lookup;
        this.valueEqualiser = valueEqualiser;
        this.lookupStrategy = lookupStrategy;
        this.deletionVectorsMaintainer = deletionVectorsMaintainer;
        this.comparator = createSequenceComparator(userDefinedSeqComparator);
        this.reusedOverwriteRow =
                preserveFieldIndices != null && preserveFieldIndices.length > 0
                        ? new SequenceFieldOverwriteRow(preserveFieldIndices)
                        : null;
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    @Override
    public ChangelogResult getResult() {
        // 1. Find the latest high level record and compute containLevel0
        KeyValue highLevel = mergeFunction.pickHighLevel();
        boolean containLevel0 = mergeFunction.containLevel0();

        // 2. Lookup if latest high level record is absent
        if (highLevel == null) {
            T lookupResult = lookup.apply(mergeFunction.key());
            if (lookupResult != null) {
                if (lookupStrategy.deletionVector) {
                    String fileName;
                    long rowPosition;
                    if (lookupResult instanceof PositionedKeyValue) {
                        PositionedKeyValue positionedKeyValue = (PositionedKeyValue) lookupResult;
                        highLevel = positionedKeyValue.keyValue();
                        fileName = positionedKeyValue.fileName();
                        rowPosition = positionedKeyValue.rowPosition();
                    } else {
                        FilePosition position = (FilePosition) lookupResult;
                        fileName = position.fileName();
                        rowPosition = position.rowPosition();
                    }
                    deletionVectorsMaintainer.notifyNewDeletion(fileName, rowPosition);
                } else {
                    highLevel = (KeyValue) lookupResult;
                }
            }
            if (highLevel != null) {
                mergeFunction.insertInto(highLevel, comparator);
            }
        }

        // 3. Calculate result
        KeyValue result = mergeFunction.getResult();

        // 4. Set changelog when there's level-0 records
        reusedResult.reset();
        if (containLevel0 && lookupStrategy.produceChangelog) {
            setChangelog(highLevel, result);
        }

        return reusedResult.setResult(result);
    }

    private void setChangelog(@Nullable KeyValue before, KeyValue after) {
        if (before == null || !before.isAdd()) {
            if (after.isAdd()) {
                reusedResult.addChangelog(replaceAfter(RowKind.INSERT, after));
            }
        } else {
            if (!after.isAdd()) {
                reusedResult.addChangelog(
                        replaceBeforeWithSequenceOverwrite(RowKind.DELETE, before, after));
            } else if (valueEqualiser == null
                    || !valueEqualiser.equals(before.value(), after.value())) {
                reusedResult
                        .addChangelog(
                                replaceBeforeWithSequenceOverwrite(
                                        RowKind.UPDATE_BEFORE, before, after))
                        .addChangelog(replaceAfter(RowKind.UPDATE_AFTER, after));
            }
        }
    }

    private KeyValue replaceBeforeWithSequenceOverwrite(
            RowKind valueKind, KeyValue before, KeyValue after) {
        if (reusedOverwriteRow != null) {
            reusedOverwriteRow.replace(before.value(), after.value());
            return reusedBefore.replace(
                    before.key(), after.sequenceNumber(), valueKind, reusedOverwriteRow);
        }
        return replace(reusedBefore, valueKind, before);
    }

    private KeyValue replaceAfter(RowKind valueKind, KeyValue from) {
        return replace(reusedAfter, valueKind, from);
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }

    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            return Comparator.comparingLong(KeyValue::sequenceNumber);
        }

        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o1.value(), o2.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o1.sequenceNumber(), o2.sequenceNumber());
        };
    }

    /**
     * An {@link InternalRow} that delegates to a primary row for all fields, except for specified
     * sequence field positions which are read from a secondary (event) row. This allows changelog
     * before-image records to carry the incoming event's sequence field value while preserving the
     * rest of the stored row's data.
     */
    static class SequenceFieldOverwriteRow implements InternalRow {

        private final boolean[] isSequenceField;
        private InternalRow primaryRow;
        private InternalRow eventRow;

        SequenceFieldOverwriteRow(int[] sequenceFieldIndices) {
            int maxIndex = 0;
            for (int idx : sequenceFieldIndices) {
                maxIndex = Math.max(maxIndex, idx);
            }
            this.isSequenceField = new boolean[maxIndex + 1];
            for (int idx : sequenceFieldIndices) {
                this.isSequenceField[idx] = true;
            }
        }

        SequenceFieldOverwriteRow replace(InternalRow primaryRow, InternalRow eventRow) {
            this.primaryRow = primaryRow;
            this.eventRow = eventRow;
            return this;
        }

        private InternalRow rowFor(int pos) {
            return pos < isSequenceField.length && isSequenceField[pos] ? eventRow : primaryRow;
        }

        @Override
        public int getFieldCount() {
            return primaryRow.getFieldCount();
        }

        @Override
        public RowKind getRowKind() {
            return primaryRow.getRowKind();
        }

        @Override
        public void setRowKind(RowKind kind) {
            primaryRow.setRowKind(kind);
        }

        @Override
        public boolean isNullAt(int pos) {
            return rowFor(pos).isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return rowFor(pos).getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return rowFor(pos).getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return rowFor(pos).getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return rowFor(pos).getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return rowFor(pos).getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return rowFor(pos).getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return rowFor(pos).getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            return rowFor(pos).getString(pos);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return rowFor(pos).getDecimal(pos, precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return rowFor(pos).getTimestamp(pos, precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return rowFor(pos).getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos) {
            return rowFor(pos).getVariant(pos);
        }

        @Override
        public Blob getBlob(int pos) {
            return rowFor(pos).getBlob(pos);
        }

        @Override
        public InternalArray getArray(int pos) {
            return rowFor(pos).getArray(pos);
        }

        @Override
        public InternalVector getVector(int pos) {
            return rowFor(pos).getVector(pos);
        }

        @Override
        public InternalMap getMap(int pos) {
            return rowFor(pos).getMap(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return rowFor(pos).getRow(pos, numFields);
        }
    }
}
