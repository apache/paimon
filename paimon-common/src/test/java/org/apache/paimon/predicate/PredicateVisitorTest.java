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

package org.apache.paimon.predicate;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PredicateVisitor}. */
public class PredicateVisitorTest {

    @Test
    public void testCollectFieldIdsUsesFieldIdInsteadOfFieldPosition() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", DataTypes.INT()),
                                new DataField(20, "b", DataTypes.INT()),
                                new DataField(30, "c", DataTypes.INT())));
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate predicate =
                PredicateBuilder.or(
                        builder.equal(0, 1),
                        PredicateBuilder.and(builder.equal(1, 2), builder.equal(0, 3)));

        assertThat(PredicateVisitor.collectFieldIds(rowType, predicate))
                .containsExactlyInAnyOrder(10, 20);
        assertThat(PredicateVisitor.collectFieldIds(rowType, null)).isEmpty();
    }
}
