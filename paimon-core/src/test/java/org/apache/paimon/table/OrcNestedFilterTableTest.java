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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests nested ORC values with a pushed predicate and the table's residual filter. */
class OrcNestedFilterTableTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNestedValuesWithFilter(boolean useSelected) throws Exception {
        Identifier identifier = identifier("nested_orc");
        RowType payloadType =
                RowType.builder()
                        .field("items", DataTypes.ARRAY(DataTypes.INT()))
                        .field("attributes", DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))
                        .build();
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", payloadType)
                        .option("file.format", "orc")
                        .option("bucket", "-1")
                        .option("orc.sarg.to.filter", "true")
                        .option("orc.filter.use.selected", Boolean.toString(useSelected))
                        .option("read.batch-size", "4")
                        .build(),
                false);
        Table table = catalog.getTable(identifier);
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<Integer> selectedIds = Arrays.asList(1, 3, 6, 8, 9, 12, 15);
        List<BinaryRow> expected = new ArrayList<>();
        GenericRow[] input = new GenericRow[16];
        for (int i = 0; i < input.length; i++) {
            Integer value = i % 4 == 1 ? null : 100 + i;
            input[i] =
                    GenericRow.of(
                            i,
                            i % 5 == 3
                                    ? null
                                    : GenericRow.of(
                                            new GenericArray(new Integer[] {value, i}),
                                            new GenericMap(Collections.singletonMap(i, value))));
            if (selectedIds.contains(i)) {
                expected.add(serializer.toBinaryRow(input[i]).copy());
            }
        }
        write(table, input);
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(
                                new PredicateBuilder(table.rowType())
                                        .in(0, new ArrayList<Object>(selectedIds)));
        List<BinaryRow> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                read.newRead().executeFilter().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> actual.add(serializer.toBinaryRow(row).copy()));
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }
}
