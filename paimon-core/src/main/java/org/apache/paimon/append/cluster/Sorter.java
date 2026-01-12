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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/** Sorter for clustering. */
public abstract class Sorter {

    protected final RecordReaderIterator<InternalRow> reader;
    protected final RowType keyType;
    protected final RowType longRowType;
    protected final int[] valueProjectionMap;
    private final int arity;

    private final transient IOManager ioManager;
    private final transient BinaryExternalSortBuffer buffer;

    public Sorter(
            RecordReaderIterator<InternalRow> reader,
            RowType keyType,
            RowType valueType,
            CoreOptions options,
            IOManager ioManager) {
        this.reader = reader;
        this.keyType = keyType;
        int keyFieldCount = keyType.getFieldCount();
        int valueFieldCount = valueType.getFieldCount();
        this.valueProjectionMap = new int[valueFieldCount];
        for (int i = 0; i < valueFieldCount; i++) {
            this.valueProjectionMap[i] = i + keyFieldCount;
        }
        List<DataField> keyFields = keyType.getFields();
        List<DataField> dataFields = valueType.getFields();
        List<DataField> fields = new ArrayList<>();
        fields.addAll(keyFields);
        fields.addAll(dataFields);
        this.longRowType = new RowType(fields);
        this.arity = longRowType.getFieldCount();

        long maxMemory = options.writeBufferSize();
        int pageSize = options.pageSize();
        int spillSortMaxNumFiles = options.localSortMaxNumFileHandles();
        CompressOptions spillCompression = options.spillCompressOptions();
        MemorySize maxDiskSize = options.writeBufferSpillDiskSize();
        boolean sequenceOrder = options.sequenceFieldSortOrderIsAscending();

        this.ioManager = ioManager;
        this.buffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        longRowType,
                        IntStream.range(0, keyType.getFieldCount()).toArray(),
                        maxMemory,
                        pageSize,
                        spillSortMaxNumFiles,
                        spillCompression,
                        maxDiskSize,
                        sequenceOrder);
    }

    public abstract InternalRow assignSortKey(InternalRow row);

    public InternalRow removeSortKey(InternalRow rowWithKey) {
        KeyProjectedRow keyProjectedRow = new KeyProjectedRow(valueProjectionMap);
        return keyProjectedRow.replaceRow(rowWithKey);
    }

    public MutableObjectIterator<BinaryRow> sort() throws IOException {
        while (reader.hasNext()) {
            InternalRow row = reader.next();
            InternalRow rowWithKey = assignSortKey(row);
            buffer.write(rowWithKey);
        }

        if (buffer.size() > 0) {
            return buffer.sortedIterator();
        } else {
            throw new IllegalStateException("numRecords after sorting is 0.");
        }
    }

    public int arity() {
        return arity;
    }

    public void close() throws Exception {
        if (buffer != null) {
            buffer.clear();
        }
        if (ioManager != null) {
            ioManager.close();
        }
    }

    public static Sorter getSorter(
            RecordReaderIterator<InternalRow> reader,
            IOManager ioManager,
            RowType rowType,
            CoreOptions options) {
        CoreOptions.OrderType clusterCurve =
                options.clusteringStrategy(options.clusteringColumns().size());
        switch (clusterCurve) {
            case HILBERT:
                return new HibertSorter(
                        reader, rowType, options, options.clusteringColumns(), ioManager);
            case ZORDER:
                return new ZorderSorter(
                        reader, rowType, options, options.clusteringColumns(), ioManager);
            case ORDER:
                return new OrderSorter(
                        reader, rowType, options, options.clusteringColumns(), ioManager);
            default:
                throw new IllegalArgumentException("cannot match cluster type: " + clusterCurve);
        }
    }

    /** Abstract key from a row data. */
    public interface KeyAbstract<KEY> extends Serializable {
        default void open() {}

        KEY apply(InternalRow value);
    }
}
