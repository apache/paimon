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

package org.apache.paimon.schema;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaEvolutionUtil}. */
public class SchemaEvolutionUtilTest {
    private final List<DataField> keyFields =
            Arrays.asList(
                    new DataField(0, "key_1", new IntType()),
                    new DataField(1, "key_2", new IntType()),
                    new DataField(2, "key_3", new IntType()));
    private final List<DataField> dataFields =
            Arrays.asList(
                    new DataField(0, "a", new IntType()),
                    new DataField(1, "b", new IntType()),
                    new DataField(2, "c", new IntType()),
                    new DataField(3, "d", new IntType()));
    private final List<DataField> tableFields1 =
            Arrays.asList(
                    new DataField(1, "c", new BigIntType()),
                    new DataField(3, "a", new FloatType()),
                    new DataField(5, "d", new IntType()),
                    new DataField(6, "e", new IntType()));
    private final List<DataField> tableFields2 =
            Arrays.asList(
                    new DataField(1, "c", new DoubleType()),
                    new DataField(3, "d", new DecimalType(10, 2)),
                    new DataField(5, "f", new BigIntType()),
                    new DataField(7, "a", new FloatType()),
                    new DataField(8, "b", new IntType()),
                    new DataField(9, "e", new DoubleType()));

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
        int[] dataProjection = new int[] {1}; // project (1, b, int)
        int[] table1Projection = new int[] {2, 0}; // project (5->d, int), (1, c, bigint)
        int[] table2Projection =
                new int[] {4, 2, 0}; // project (8, b, int), (5, f, bigint), (1, c, double)

        InternalRow dataValue = GenericRow.of(1234);
        IndexCastMapping table1DataIndexMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        table1Projection, tableFields1, dataProjection, dataFields);
        assertThat(table1DataIndexMapping.getIndexMapping()).containsExactly(-1, 0);

        // Get (null, 1234L) from data value
        assertThat(table1DataIndexMapping.getCastMapping().length).isEqualTo(2);
        ProjectedRow projectedDataRow1 =
                ProjectedRow.from(table1DataIndexMapping.getIndexMapping());
        projectedDataRow1.replaceRow(dataValue);
        Object table1Field1Value =
                table1DataIndexMapping.getCastMapping()[0].getFieldOrNull(projectedDataRow1);
        long table1Field2Value =
                table1DataIndexMapping.getCastMapping()[1].getFieldOrNull(projectedDataRow1);
        assertThat(table1Field1Value).isNull();
        assertThat(table1Field2Value).isEqualTo(1234L);

        IndexCastMapping table2DataIndexMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        table2Projection, tableFields2, dataProjection, dataFields);
        assertThat(table2DataIndexMapping.getIndexMapping()).containsExactly(-1, -1, 0);

        // Get (null, null, 1234.0D) from data value
        assertThat(table2DataIndexMapping.getCastMapping().length).isEqualTo(3);
        ProjectedRow projectedDataRow2 =
                ProjectedRow.from(table2DataIndexMapping.getIndexMapping());
        projectedDataRow2.replaceRow(dataValue);
        Object table2Field1Value =
                table2DataIndexMapping.getCastMapping()[0].getFieldOrNull(projectedDataRow2);
        Object table2Field2Value =
                table2DataIndexMapping.getCastMapping()[1].getFieldOrNull(projectedDataRow2);
        Object table2Field3Value =
                table2DataIndexMapping.getCastMapping()[2].getFieldOrNull(projectedDataRow2);
        assertThat(table2Field1Value).isNull();
        assertThat(table2Field2Value).isNull();
        assertThat(table2Field3Value).isEqualTo(1234D);

        IndexCastMapping table2Table1IndexMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        table2Projection, tableFields2, table1Projection, tableFields1);
        assertThat(table2Table1IndexMapping.getIndexMapping()).containsExactly(-1, 0, 1);

        InternalRow table1Data = GenericRow.of(123, 321L);
        ProjectedRow projectedDataRow3 =
                ProjectedRow.from(table2Table1IndexMapping.getIndexMapping());
        projectedDataRow3.replaceRow(table1Data);
        // Get (null, 123L, 321.0D) from table1 data
        assertThat(table2Table1IndexMapping.getCastMapping().length).isEqualTo(3);
        Object table2Table1Field1Value =
                table2Table1IndexMapping.getCastMapping()[0].getFieldOrNull(projectedDataRow3);
        long table2Table1Field2Value =
                table2Table1IndexMapping.getCastMapping()[1].getFieldOrNull(projectedDataRow3);
        double table2Table1Field3Value =
                table2Table1IndexMapping.getCastMapping()[2].getFieldOrNull(projectedDataRow3);
        assertThat(table2Table1Field1Value).isNull();
        assertThat(table2Table1Field2Value).isEqualTo(123L);
        assertThat(table2Table1Field3Value).isEqualTo(321.0D);
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

        IndexCastMapping table1DataIndexMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        table1Projection,
                        keyFields,
                        tableFields1,
                        dataProjection,
                        keyFields,
                        dataFields);
        assertThat(table1DataIndexMapping.getIndexMapping()).containsExactly(0, 1, 2, 3, -1, 4);

        // Get (1, 2, 3, (byte) 4, null, 5L) from data value
        InternalRow dataValue = GenericRow.of(1, 2, 3L, (byte) 4, 5);
        ProjectedRow projectedDataValue =
                ProjectedRow.from(table1DataIndexMapping.getIndexMapping());
        projectedDataValue.replaceRow(dataValue);
        assertThat(table1DataIndexMapping.getCastMapping().length).isEqualTo(6);
        int table1Field1Value =
                table1DataIndexMapping.getCastMapping()[0].getFieldOrNull(projectedDataValue);
        int table1Field2Value =
                table1DataIndexMapping.getCastMapping()[1].getFieldOrNull(projectedDataValue);
        long table1Field3Value =
                table1DataIndexMapping.getCastMapping()[2].getFieldOrNull(projectedDataValue);
        byte table1Field4Value =
                table1DataIndexMapping.getCastMapping()[3].getFieldOrNull(projectedDataValue);
        Object table1Field5Value =
                table1DataIndexMapping.getCastMapping()[4].getFieldOrNull(projectedDataValue);
        long table1Field6Value =
                table1DataIndexMapping.getCastMapping()[5].getFieldOrNull(projectedDataValue);
        assertThat(table1Field1Value).isEqualTo(1);
        assertThat(table1Field2Value).isEqualTo(2);
        assertThat(table1Field3Value).isEqualTo(3L);
        assertThat(table1Field4Value).isEqualTo((byte) 4);
        assertThat(table1Field5Value).isNull();
        assertThat(table1Field6Value).isEqualTo(5L);

        IndexCastMapping table2Table1IndexMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        table2Projection,
                        keyFields,
                        tableFields2,
                        table1Projection,
                        keyFields,
                        tableFields1);
        assertThat(table2Table1IndexMapping.getIndexMapping())
                .containsExactly(0, 1, 2, 3, -1, 4, 5);

        // Get (1, 2, 3, (byte) 4, null, 5L, 6.0D) from data value
        InternalRow table1Value = GenericRow.of(1, 2, 3L, (byte) 4, 5, 6L);
        ProjectedRow projectedTableValue =
                ProjectedRow.from(table2Table1IndexMapping.getIndexMapping());
        projectedTableValue.replaceRow(table1Value);
        assertThat(table2Table1IndexMapping.getCastMapping().length).isEqualTo(7);
        int table2Field1Value =
                table2Table1IndexMapping.getCastMapping()[0].getFieldOrNull(projectedTableValue);
        int table2Field2Value =
                table2Table1IndexMapping.getCastMapping()[1].getFieldOrNull(projectedTableValue);
        long table2Field3Value =
                table2Table1IndexMapping.getCastMapping()[2].getFieldOrNull(projectedTableValue);
        byte table2Field4Value =
                table2Table1IndexMapping.getCastMapping()[3].getFieldOrNull(projectedTableValue);
        Object table2Field5Value =
                table2Table1IndexMapping.getCastMapping()[4].getFieldOrNull(projectedTableValue);
        long table2Field6Value =
                table2Table1IndexMapping.getCastMapping()[5].getFieldOrNull(projectedTableValue);
        double table2Field7Value =
                table2Table1IndexMapping.getCastMapping()[6].getFieldOrNull(projectedTableValue);
        assertThat(table2Field1Value).isEqualTo(1);
        assertThat(table2Field2Value).isEqualTo(2);
        assertThat(table2Field3Value).isEqualTo(3L);
        assertThat(table2Field4Value).isEqualTo((byte) 4);
        assertThat(table2Field5Value).isNull();
        assertThat(table2Field6Value).isEqualTo(5L);
        assertThat(table2Field7Value).isEqualTo(6.0D);
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
    public void testEvolveDataFilters() {
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(
                new LeafPredicate(
                        IsNull.INSTANCE, DataTypes.INT(), 0, "c", Collections.emptyList()));
        // Field 9->e is not exist in data
        predicates.add(
                new LeafPredicate(
                        IsNotNull.INSTANCE, DataTypes.INT(), 9, "e", Collections.emptyList()));
        // Field 7->a is not exist in data
        predicates.add(
                new LeafPredicate(
                        IsNull.INSTANCE, DataTypes.INT(), 7, "a", Collections.emptyList()));

        List<Predicate> filters =
                SchemaEvolutionUtil.devolveDataFilters(tableFields2, dataFields, predicates);
        assert filters != null;
        assertThat(filters.size()).isEqualTo(1);

        LeafPredicate child1 = (LeafPredicate) filters.get(0);
        assertThat(child1.function()).isEqualTo(IsNull.INSTANCE);
        assertThat(child1.fieldName()).isEqualTo("b");
        assertThat(child1.index()).isEqualTo(1);
    }

    @Test
    public void testColumnTypeFilter() {
        // alter d from INT to DECIMAL(10, 2)
        // filter d = 11.01 will be devolved to d = 11
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.DECIMAL(10, 2),
                        0,
                        "d",
                        Collections.singletonList(
                                Decimal.fromBigDecimal(new BigDecimal("11.01"), 10, 2))));
        List<Predicate> filters =
                SchemaEvolutionUtil.devolveDataFilters(tableFields2, dataFields, predicates);
        assert filters != null;
        assertThat(filters.size()).isEqualTo(1);

        LeafPredicate child = (LeafPredicate) filters.get(0);
        // Validate value 11 with index 3
        assertThat(child.test(GenericRow.of(0, 0, 0, 11))).isTrue();
        // Validate value 12 with index 3
        assertThat(child.test(GenericRow.of(1, 0, 0, 12))).isFalse();
    }
}
