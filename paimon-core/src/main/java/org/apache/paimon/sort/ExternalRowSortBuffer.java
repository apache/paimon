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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * External sort for Interval Row. This is a wrapper of {@link BinaryExternalSortBuffer}, in
 * difference that, BinaryExternalSortBuffer could only sort the buffer by key and value,
 * ExternalInternalRowSortBuffer could sort row by specified column. We can just use this to avoid
 * complex process about constructing the sort key, dropping the sort key after sorting.
 *
 * <p>For example, table (f0 String, f1 int, f2 int), we sort rows by f2 :
 *
 * <pre>
 *    a, 3, 1
 *    a, 3, 2
 *    a, 3, 100
 *    a, 3, 0
 * </pre>
 *
 * <p>We get:
 *
 * <pre>
 *     a, 3, 0
 *     a, 3, 1
 *     a, 3, 2
 *     a, 3, 100
 * </pre>
 *
 * <p>We don't need to care about constructing the row: KEY_f2, f0, f1, f2, Then sort by KEY_f2,
 * Then drop KEY_f2
 *
 * <p>The whole process is finished by this class. We just need to tell it which columns are sorted
 * by.
 */
public class ExternalRowSortBuffer implements SortBuffer {

    private static final String KEY_PREFIX = "_SORT_KEY_";

    private final BinaryExternalSortBuffer binaryExternalSortBuffer;
    private final int fullSize;
    private final KeyValueSerializer serializer;
    private final Projection rowPartitionKeyExtractor;

    private long sequnceNum = 0;

    public ExternalRowSortBuffer(
            IOManager ioManager,
            RowType rowType,
            int[] fieldsIndex,
            long bufferSize,
            int pageSize,
            int maxNumFileHandles) {
        List<DataField> rowFields = rowType.getFields();
        List<DataField> keyFields = new ArrayList<>();

        // construct the partition extractor to extract partition row from whole row.
        rowPartitionKeyExtractor = CodeGenUtils.newProjection(rowType, fieldsIndex);

        // construct the value projection to extract origin row from extended key_value row.
        for (int i : fieldsIndex) {
            // we put the KEY_PREFIX ahead of origin field name to avoid conflict
            keyFields.add(rowFields.get(i).newName(KEY_PREFIX + rowFields.get(i).name()));
        }

        RowType keyRow = new RowType(keyFields);
        RowType wholeRow = KeyValue.schema(keyRow, rowType);

        // construct the binary sorter to sort the extended key_value row
        binaryExternalSortBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager, keyRow, wholeRow, bufferSize, pageSize, maxNumFileHandles);

        this.fullSize = KeyValue.schema(keyRow, rowType).getFieldCount();
        this.serializer = new KeyValueSerializer(keyRow, rowType);
    }

    @Override
    public int size() {
        return binaryExternalSortBuffer.size();
    }

    @Override
    public void clear() {
        binaryExternalSortBuffer.clear();
    }

    @Override
    public long getOccupancy() {
        return binaryExternalSortBuffer.getOccupancy();
    }

    @Override
    public boolean flushMemory() throws IOException {
        return binaryExternalSortBuffer.flushMemory();
    }

    @Override
    public boolean write(InternalRow record) throws IOException {
        BinaryRow partition = rowPartitionKeyExtractor.apply(record);
        binaryExternalSortBuffer.write(
                serializer.toRow(partition, sequnceNum++, record.getRowKind(), record));
        return false;
    }

    @Override
    public MutableObjectIterator<InternalRow> sortedIterator() throws IOException {
        MutableObjectIterator<BinaryRow> iterator = binaryExternalSortBuffer.sortedIterator();
        return new MutableObjectIterator<InternalRow>() {
            private BinaryRow binaryRow = new BinaryRow(fullSize);

            @Override
            public InternalRow next(InternalRow reuse) throws IOException {
                // use internal reuse object
                return next();
            }

            @Override
            public InternalRow next() throws IOException {
                binaryRow = iterator.next(binaryRow);
                if (binaryRow != null) {
                    KeyValue keyValue = serializer.fromRow(binaryRow);
                    binaryRow.setRowKind(keyValue.valueKind());
                    return keyValue.value();
                }
                return null;
            }
        };
    }

    public static ExternalRowSortBuffer create(
            IOManager ioManager,
            RowType rowType,
            int[] fields,
            long bufferSize,
            int pageSize,
            int maxNumFileHandles) {
        return new ExternalRowSortBuffer(
                ioManager, rowType, fields, bufferSize, pageSize, maxNumFileHandles);
    }
}
