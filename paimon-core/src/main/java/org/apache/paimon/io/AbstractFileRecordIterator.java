/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.io;

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

/**
 * Abstract {@link RecordReader.RecordIterator} implementation for schema evolution.
 *
 * @param <V> the row type.
 */
public abstract class AbstractFileRecordIterator<V> implements RecordReader.RecordIterator<V> {

    @Nullable private final PartitionSettedRow partitionSettedRow;
    @Nullable private final ProjectedRow projectedRow;
    @Nullable private final CastedRow castedRow;

    protected AbstractFileRecordIterator(
            @Nullable int[] indexMapping,
            @Nullable PartitionInfo partitionInfo,
            @Nullable CastFieldGetter[] castMapping) {
        this.partitionSettedRow =
                partitionInfo == null ? null : PartitionSettedRow.from(partitionInfo);
        this.projectedRow = indexMapping == null ? null : ProjectedRow.from(indexMapping);
        this.castedRow = castMapping == null ? null : CastedRow.from(castMapping);
    }

    protected InternalRow mappingRowData(InternalRow rowData) {
        if (rowData == null) {
            return null;
        }
        if (partitionSettedRow != null) {
            rowData = partitionSettedRow.replaceRow(rowData);
        }
        if (projectedRow != null) {
            rowData = projectedRow.replaceRow(rowData);
        }
        if (castedRow != null) {
            rowData = castedRow.replaceRow(rowData);
        }
        return rowData;
    }
}
