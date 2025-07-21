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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaEvolutionUtil}. */
public class SchemaEvolutionUtilTest {

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
    public void testDevolveDataFilters() {
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
}
