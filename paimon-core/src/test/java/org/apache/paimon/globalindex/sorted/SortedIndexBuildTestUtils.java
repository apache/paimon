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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Test helper to build a sorted global index for a single split, mirroring the engine build
 * topologies: rows are sorted by the index writer's key order (the reversed order for
 * reverse-btree) before they are handed to {@link
 * SortedGlobalIndexBuilder#buildForSinglePartition}.
 */
public class SortedIndexBuildTestUtils {

    private SortedIndexBuildTestUtils() {}

    public static List<CommitMessage> sortAndBuild(
            SortedGlobalIndexBuilder builder,
            FileStoreTable table,
            String indexFieldName,
            DataSplit split)
            throws Exception {
        DataField indexField = table.rowType().getField(indexFieldName);
        RowType readRowType =
                SpecialFields.rowTypeWithRowId(table.rowType())
                        .project(Arrays.asList(indexFieldName, SpecialFields.ROW_ID.name()));
        InternalRowSerializer rowSerializer = new InternalRowSerializer(readRowType);

        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .withReadType(readRowType)
                        .newRead()
                        .createReader(Collections.singletonList(split))) {
            reader.forEachRemaining(row -> rows.add(rowSerializer.copy(row)));
        }

        Comparator<Object> keyComparator =
                builder.sortKeySerializer()
                        .map(KeySerializer::createComparator)
                        .orElseGet(
                                () -> KeySerializer.create(indexField.type()).createComparator());
        InternalRow.FieldGetter indexFieldGetter =
                InternalRow.createFieldGetter(indexField.type(), 0);
        rows.sort(
                Comparator.comparing(
                        indexFieldGetter::getFieldOrNull, Comparator.nullsFirst(keyComparator)));

        return builder.buildForSinglePartition(
                SortedGlobalIndexBuilder.calcRowRange(split), split.partition(), rows.iterator());
    }
}
