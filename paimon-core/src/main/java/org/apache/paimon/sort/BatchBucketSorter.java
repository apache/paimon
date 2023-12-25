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

package org.apache.paimon.sort;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.NextIterator;
import org.apache.paimon.utils.OffsetRow;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.schema.SystemColumns.SEQUENCE_NUMBER;

/**
 * Sort the given {@link InternalRow} and bucket with specified field. Order with the sort is, given
 * fields, bucket, orderNumber.
 *
 * <p>For example, table (f0 String, f1 int, f2 int), we sort rows by f2 and bucket field is f1 :
 *
 * <pre>
 *    a, 3, 1
 *    a, 2, 1
 *    a, 3, 100
 *    a, 2, 0
 * </pre>
 *
 * <p>We get:
 *
 * <pre>
 *     a, 2, 0
 *     a, 2, 1
 *     a, 3, 1
 *     a, 3, 100
 * </pre>
 *
 * <p>The sorter will spill the data to disk.
 */
public class BatchBucketSorter {

    private static final String KEY_PREFIX = "_SORT_KEY_";

    private final BinaryExternalSortBuffer binaryExternalSortBuffer;
    private final int fieldsSize;
    private final int fullSize;
    private final KeyValueSerializer serializer;
    private final Projection sortKeyExtract;

    private final JoinedRow reusedJoinedRow;
    private final GenericRow reusedBucketRow;
    private final OffsetRow reusedOffsetRow;

    private long orderNumber = 0;

    public BatchBucketSorter(
            IOManager ioManager,
            RowType rowType,
            int[] fieldsIndex,
            long bufferSize,
            int pageSize,
            int maxNumFileHandles) {
        List<DataField> dataFields = new ArrayList<>();
        List<DataField> keyFields = new ArrayList<>();
        dataFields.add(new DataField(0, "_BUCKET_", DataTypes.INT()));
        dataFields.addAll(rowType.getFields());

        RowType dataRowType = new RowType(dataFields);

        // add bucket field in front of row, and selected field should include the bucket
        int[] sortField = new int[fieldsIndex.length + 1];
        for (int i = 0; i < sortField.length - 1; i++) {
            sortField[i] = fieldsIndex[i] + 1;
        }
        // add bucket field to sort
        sortField[sortField.length - 1] = 0;

        // construct the partition extractor to extract key row from whole row.
        sortKeyExtract = CodeGenUtils.newProjection(dataRowType, sortField);

        // construct the value projection to extract origin row from extended key_value row.
        for (int i : sortField) {
            // we put the KEY_PREFIX ahead of origin field name to avoid conflict
            keyFields.add(dataFields.get(i).newName(KEY_PREFIX + dataFields.get(i).name()));
        }

        // user key + sequenceNumber
        List<DataField> sortKeyTypes = new ArrayList<>(keyFields);
        sortKeyTypes.add(
                new DataField(sortKeyTypes.size(), SEQUENCE_NUMBER, new BigIntType(false)));

        RowType keyRow = new RowType(keyFields);
        RowType wholeRow = KeyValue.schema(keyRow, dataRowType);

        // construct the binary sorter to sort the extended key_value row
        binaryExternalSortBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        new RowType(sortKeyTypes),
                        wholeRow,
                        bufferSize,
                        pageSize,
                        maxNumFileHandles);

        this.fieldsSize = fieldsIndex.length;
        this.fullSize = wholeRow.getFieldCount();
        this.serializer = new KeyValueSerializer(keyRow, dataRowType);

        this.reusedJoinedRow = new JoinedRow();
        this.reusedBucketRow = new GenericRow(1);
        this.reusedOffsetRow = new OffsetRow(dataRowType.getFieldCount(), 1);
    }

    public int fieldsSize() {
        return fieldsSize;
    }

    public int size() {
        return binaryExternalSortBuffer.size();
    }

    public void clear() {
        orderNumber = 0;
        binaryExternalSortBuffer.clear();
    }

    public boolean flushMemory() throws IOException {
        return binaryExternalSortBuffer.flushMemory();
    }

    public boolean write(InternalRow record, int bucket) throws IOException {
        InternalRow bucketRow = toRowWithBucket(record, bucket);
        BinaryRow keyRow = sortKeyExtract.apply(bucketRow);
        return binaryExternalSortBuffer.write(
                serializer.toRow(keyRow, orderNumber++, bucketRow.getRowKind(), bucketRow));
    }

    private InternalRow toRowWithBucket(InternalRow row, int bucket) {
        reusedBucketRow.setField(0, bucket);
        reusedJoinedRow.replace(reusedBucketRow, row);
        reusedJoinedRow.setRowKind(row.getRowKind());
        return reusedJoinedRow;
    }

    private int bucket(InternalRow rowWithBucket) {
        return rowWithBucket.getInt(0);
    }

    private InternalRow toOriginRow(InternalRow row) {
        return reusedOffsetRow.replace(row);
    }

    public NextIterator<Pair<InternalRow, Integer>> sortedIterator() throws IOException {
        MutableObjectIterator<BinaryRow> iterator = binaryExternalSortBuffer.sortedIterator();
        return new NextIterator<Pair<InternalRow, Integer>>() {
            private BinaryRow binaryRow = new BinaryRow(fullSize);
            private final Pair<InternalRow, Integer> reusePair = Pair.of(null, null);

            @Override
            public Pair<InternalRow, Integer> nextOrNull() {
                try {
                    binaryRow = iterator.next(binaryRow);
                    if (binaryRow != null) {
                        KeyValue keyValue = serializer.fromRow(binaryRow);
                        binaryRow.setRowKind(keyValue.valueKind());
                        InternalRow row = keyValue.value();
                        reusePair.setLeft(toOriginRow(row));
                        reusePair.setRight(bucket(row));
                        return reusePair;
                    }
                    return null;
                } catch (IOException e) {
                    throw new RuntimeException("Errors while sorted bucket batch", e);
                }
            }
        };
    }

    public static BatchBucketSorter create(
            IOManager ioManager,
            RowType rowType,
            int[] fields,
            long bufferSize,
            int pageSize,
            int maxNumFileHandles) {
        return new BatchBucketSorter(
                ioManager, rowType, fields, bufferSize, pageSize, maxNumFileHandles);
    }
}
