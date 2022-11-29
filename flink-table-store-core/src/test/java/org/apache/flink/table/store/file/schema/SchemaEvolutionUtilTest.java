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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.store.file.predicate.IsNotNull;
import org.apache.flink.table.store.file.predicate.IsNull;
import org.apache.flink.table.store.file.predicate.LeafPredicate;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.utils.Projection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaEvolutionUtil}. */
public class SchemaEvolutionUtilTest {
    private final List<DataField> keyFields =
            Arrays.asList(
                    new DataField(0, "key_1", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(1, "key_2", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(
                            2, "key_3", new AtomicDataType(DataTypes.INT().getLogicalType())));
    private final List<DataField> dataFields =
            Arrays.asList(
                    new DataField(0, "a", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(1, "b", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(2, "c", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(3, "d", new AtomicDataType(DataTypes.INT().getLogicalType())));
    private final List<DataField> tableFields1 =
            Arrays.asList(
                    new DataField(1, "c", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(3, "a", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(5, "d", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(6, "e", new AtomicDataType(DataTypes.INT().getLogicalType())));
    private final List<DataField> tableFields2 =
            Arrays.asList(
                    new DataField(1, "c", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(3, "d", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(5, "f", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(7, "a", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(8, "b", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(9, "e", new AtomicDataType(DataTypes.INT().getLogicalType())));

    @Test
    public void testCreateIndexMapping() {
        int[] indexMapping = SchemaEvolutionUtil.createIndexMapping(tableFields1, dataFields);

        assert indexMapping != null;
        assertThat(indexMapping.length).isEqualTo(tableFields1.size()).isEqualTo(4);
        assertThat(indexMapping[0]).isEqualTo(1);
        assertThat(indexMapping[1]).isEqualTo(3);
        assertThat(indexMapping[2]).isLessThan(0);
        assertThat(indexMapping[3]).isLessThan(0);
    }

    @Test
    public void testCreateIndexMappingWithFields() {
        int[] dataProjection = new int[] {1}; // project "b"
        int[] table1Projection = new int[] {2, 0}; // project "d", "c"
        int[] table2Projection = new int[] {4, 2, 0}; // project "b", "f", "c"

        int[] table1DataIndexMapping =
                SchemaEvolutionUtil.createIndexMapping(
                        table1Projection, tableFields1, dataProjection, dataFields);
        assertThat(table1DataIndexMapping).containsExactly(-1, 0);

        int[] table2DataIndexMapping =
                SchemaEvolutionUtil.createIndexMapping(
                        table2Projection, tableFields2, dataProjection, dataFields);
        assertThat(table2DataIndexMapping).containsExactly(-1, -1, 0);

        int[] table2Table1IndexMapping =
                SchemaEvolutionUtil.createIndexMapping(
                        table2Projection, tableFields2, table1Projection, tableFields1);
        assertThat(table2Table1IndexMapping).containsExactly(-1, 0, 1);
    }

    @Test
    public void testCreateIndexMappingWithKeyValueFields() {
        int[] dataProjection =
                new int[] {0, 2, 3, 4, 6}; // project "key_1", "key3", "seq", "kind", "b"
        int[] table1Projection =
                new int[] {0, 2, 3, 4, 7, 5}; // project "key_1", "key3", "seq", "kind", "d", "c"
        int[] table2Projection =
                new int[] {
                    0, 2, 3, 4, 9, 7, 5
                }; // project "key_1", "key3", "seq", "kind", "b", "f", "c"

        int[] table1DataIndexMapping =
                SchemaEvolutionUtil.createIndexMapping(
                        table1Projection,
                        keyFields,
                        tableFields1,
                        dataProjection,
                        keyFields,
                        dataFields);
        assertThat(table1DataIndexMapping).containsExactly(0, 1, 2, 3, -1, 4);

        int[] table2Table1IndexMapping =
                SchemaEvolutionUtil.createIndexMapping(
                        table2Projection,
                        keyFields,
                        tableFields2,
                        table1Projection,
                        keyFields,
                        tableFields1);
        assertThat(table2Table1IndexMapping).containsExactly(0, 1, 2, 3, -1, 4, 5);
    }

    @Test
    public void testCreateDataProjection() {
        int[][] table1Projection =
                new int[][] {new int[] {2}, new int[] {0}}; // project 5->d and 1->c in tableField1
        int[][] table2Projection =
                new int[][] {
                    new int[] {4}, new int[] {2}, new int[] {0}
                }; // project 8->b, 5->f and 1->c in tableField2

        int[][] table1DataProjection =
                SchemaEvolutionUtil.createDataProjection(
                        tableFields1, dataFields, table1Projection);
        assertThat(Projection.of(table1DataProjection).toTopLevelIndexes()).containsExactly(1);

        int[][] table2DataProjection =
                SchemaEvolutionUtil.createDataProjection(
                        tableFields2, dataFields, table2Projection);
        assertThat(Projection.of(table2DataProjection).toTopLevelIndexes()).containsExactly(1);

        int[][] table2Table1Projection =
                SchemaEvolutionUtil.createDataProjection(
                        tableFields2, tableFields1, table2Projection);
        assertThat(Projection.of(table2Table1Projection).toTopLevelIndexes()).containsExactly(2, 0);
    }

    @Test
    public void testCreateDataFilters() {
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(
                new LeafPredicate(
                        IsNull.INSTANCE,
                        DataTypes.INT().getLogicalType(),
                        0,
                        "c",
                        Collections.emptyList()));
        // Field 9->e is not exist in data
        predicates.add(
                new LeafPredicate(
                        IsNotNull.INSTANCE,
                        DataTypes.INT().getLogicalType(),
                        9,
                        "e",
                        Collections.emptyList()));
        // Field 7->a is not exist in data
        predicates.add(
                new LeafPredicate(
                        IsNull.INSTANCE,
                        DataTypes.INT().getLogicalType(),
                        7,
                        "a",
                        Collections.emptyList()));

        List<Predicate> filters =
                SchemaEvolutionUtil.createDataFilters(tableFields2, dataFields, predicates);
        assert filters != null;
        assertThat(filters.size()).isEqualTo(1);

        LeafPredicate child1 = (LeafPredicate) filters.get(0);
        assertThat(child1.function()).isEqualTo(IsNull.INSTANCE);
        assertThat(child1.fieldName()).isEqualTo("b");
        assertThat(child1.index()).isEqualTo(1);
    }
}
