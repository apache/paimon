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

package org.apache.paimon.presto;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrestoFilterConverter}. */
public class PrestoFilterConverterTest {

    @Test
    public void testAll() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PrestoFilterConverter converter = new PrestoFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        PrestoColumnHandle idColumn =
                PrestoColumnHandle.create("id", new IntType(), createTestFunctionAndTypeManager());
        TupleDomain<PrestoColumnHandle> isNull =
                TupleDomain.withColumnDomains(ImmutableMap.of(idColumn, Domain.onlyNull(INTEGER)));
        Predicate expectedIsNull = builder.isNull(0);
        Predicate actualIsNull = converter.convert(isNull).get();
        assertThat(actualIsNull).isEqualTo(expectedIsNull);

        TupleDomain<PrestoColumnHandle> isNotNull =
                TupleDomain.withColumnDomains(ImmutableMap.of(idColumn, Domain.notNull(INTEGER)));
        Predicate expectedIsNotNull = builder.isNotNull(0);
        Predicate actualIsNotNull = converter.convert(isNotNull).get();
        assertThat(actualIsNotNull).isEqualTo(expectedIsNotNull);

        TupleDomain<PrestoColumnHandle> lt =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.lessThan(INTEGER, 1L)), false)));
        Predicate expectedLt = builder.lessThan(0, 1);
        Predicate actualLt = converter.convert(lt).get();
        assertThat(actualLt).isEqualTo(expectedLt);

        TupleDomain<PrestoColumnHandle> ltEq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 1L)),
                                        false)));
        Predicate expectedLtEq = builder.lessOrEqual(0, 1);
        Predicate actualLtEq = converter.convert(ltEq).get();
        assertThat(actualLtEq).isEqualTo(expectedLtEq);

        TupleDomain<PrestoColumnHandle> gt =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(INTEGER, 1L)), false)));
        Predicate expectedGt = builder.greaterThan(0, 1);
        Predicate actualGt = converter.convert(gt).get();
        assertThat(actualGt).isEqualTo(expectedGt);

        TupleDomain<PrestoColumnHandle> gtEq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 1L)),
                                        false)));
        Predicate expectedGtEq = builder.greaterOrEqual(0, 1);
        Predicate actualGtEq = converter.convert(gtEq).get();
        assertThat(actualGtEq).isEqualTo(expectedGtEq);

        TupleDomain<PrestoColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(idColumn, Domain.singleValue(INTEGER, 1L)));
        Predicate expectedEq = builder.equal(0, 1);
        Predicate actualEq = converter.convert(eq).get();
        assertThat(actualEq).isEqualTo(expectedEq);

        TupleDomain<PrestoColumnHandle> in =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.multipleValues(INTEGER, Arrays.asList(1L, 2L, 3L))));
        Predicate expectedIn = builder.in(0, Arrays.asList(1, 2, 3));
        Predicate actualIn = converter.convert(in).get();
        assertThat(actualIn).isEqualTo(expectedIn);
    }
}
