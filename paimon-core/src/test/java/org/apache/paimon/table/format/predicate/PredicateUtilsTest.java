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

package org.apache.paimon.table.format.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PredicateUtils}. */
public class PredicateUtilsTest {

    private final RowType partitionType =
            RowType.builder().field("dt", DataTypes.INT()).field("hr", DataTypes.INT()).build();

    private final RowType fullRowType =
            RowType.builder()
                    .field("dt", DataTypes.INT())
                    .field("hr", DataTypes.INT())
                    .field("name", DataTypes.STRING())
                    .field("value", DataTypes.INT())
                    .build();

    @Test
    public void testMultiplePredicatesOnSamePartitionField() {
        PredicateBuilder builder = new PredicateBuilder(fullRowType);
        // hr > 10 AND hr < 20
        Predicate hrGreater = builder.greaterThan(1, 10);
        Predicate hrLess = builder.lessThan(1, 20);
        Predicate dtGreater = builder.greaterThan(0, 0);
        Predicate dtLess = builder.lessThan(0, 10);

        Predicate predicate = PredicateBuilder.and(hrGreater, hrLess, dtGreater, dtLess);

        Map<String, Predicate> result =
                PredicateUtils.splitPartitionPredicate(partitionType, predicate);

        assertThat(result).hasSize(2);
        assertThat(result).containsKey("hr");
        assertThat(result).containsKey("dt");
        // Should combine with AND
        Predicate combined = result.get("hr");
        // Test that the combined predicate works correctly
        assertThat(combined.test(GenericRow.of(15))).isTrue();
        assertThat(combined.test(GenericRow.of(5))).isFalse();
        assertThat(combined.test(GenericRow.of(25))).isFalse();

        combined = result.get("dt");
        // Test that the combined predicate works correctly
        assertThat(combined.test(GenericRow.of(5))).isTrue();
        assertThat(combined.test(GenericRow.of(11))).isFalse();
        assertThat(combined.test(GenericRow.of(-1))).isFalse();
    }
}
