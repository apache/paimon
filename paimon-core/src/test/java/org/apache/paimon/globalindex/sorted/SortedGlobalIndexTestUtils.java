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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.calcRowRange;

/** Utilities for building sorted indexes in tests without exposing a test-only production API. */
public final class SortedGlobalIndexTestUtils {

    private SortedGlobalIndexTestUtils() {}

    public static List<CommitMessage> buildIndex(
            FileStoreTable table,
            String indexType,
            String indexFieldName,
            DataSplit dataSplit,
            long scanSnapshotId)
            throws Exception {
        SortedGlobalIndexWriter writer =
                new SortedGlobalIndexWriter(table, indexType).withIndexField(indexFieldName);
        DataField indexField = table.rowType().getField(indexFieldName);
        RowType readRowType =
                SpecialFields.rowTypeWithRowId(table.rowType())
                        .project(Arrays.asList(indexFieldName, SpecialFields.ROW_ID.name()));
        InternalRow.FieldGetter fieldGetter = InternalRow.createFieldGetter(indexField.type(), 0);
        List<Pair<Object, Long>> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                        table.newReadBuilder()
                                .withReadType(readRowType)
                                .newRead()
                                .createReader(Collections.singletonList(dataSplit));
                CloseableIterator<InternalRow> iterator = reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                Object value = fieldGetter.getFieldOrNull(row);
                rows.add(Pair.of(InternalRowUtils.copy(value, indexField.type()), row.getLong(1)));
            }
        }

        Comparator<Object> comparator = KeySerializer.create(indexField.type()).createComparator();
        rows.sort(
                (left, right) -> {
                    if (left.getKey() == null) {
                        return right.getKey() == null ? 0 : -1;
                    }
                    return right.getKey() == null
                            ? 1
                            : comparator.compare(left.getKey(), right.getKey());
                });

        List<InternalRow> sortedRows = new ArrayList<>(rows.size());
        for (Pair<Object, Long> row : rows) {
            sortedRows.add(GenericRow.of(row.getKey(), row.getValue()));
        }
        Range rowRange = calcRowRange(dataSplit);
        return writer.buildForSinglePartition(
                rowRange, dataSplit.partition(), sortedRows.iterator(), scanSnapshotId);
    }
}
