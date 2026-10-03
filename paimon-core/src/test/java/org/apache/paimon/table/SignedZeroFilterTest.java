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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Engines treat -0.0 and 0.0 as equal. A predicate on one of the zeros must not skip a file, or
 * drop a row, that holds only the other one.
 */
public class SignedZeroFilterTest extends TableTestBase {

    private static Stream<Arguments> tables() {
        List<Arguments> arguments = new ArrayList<>();
        for (String format : new String[] {"parquet", "orc", "avro"}) {
            arguments.add(Arguments.of(format, false));
            arguments.add(Arguments.of(format, true));
        }
        return arguments.stream();
    }

    @ParameterizedTest(name = "{0}, primary key: {1}")
    @MethodSource("tables")
    public void testStoredNegativeZero(String format, boolean primaryKey) throws Exception {
        assertZeroIsFound(format, primaryKey, -0.0d, -0.0f, 0.0d, 0.0f);
    }

    @ParameterizedTest(name = "{0}, primary key: {1}")
    @MethodSource("tables")
    public void testStoredPositiveZero(String format, boolean primaryKey) throws Exception {
        assertZeroIsFound(format, primaryKey, 0.0d, 0.0f, -0.0d, -0.0f);
    }

    private void assertZeroIsFound(
            String format,
            boolean primaryKey,
            double storedDouble,
            float storedFloat,
            double doubleLiteral,
            float floatLiteral)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("d", DataTypes.DOUBLE())
                        .column("f", DataTypes.FLOAT())
                        .option("file.format", format);
        if (primaryKey) {
            schema.primaryKey("pk").option("bucket", "1");
        }
        catalog.createTable(identifier(), schema.build(), false);
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(1, storedDouble, storedFloat));

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        List<Predicate> predicates = new ArrayList<>();
        predicates.addAll(zeroPredicates(builder, 1, doubleLiteral, 1.0d));
        predicates.addAll(zeroPredicates(builder, 2, floatLiteral, 1.0f));
        for (Predicate predicate : predicates) {
            assertThat(readPks(table, predicate, false))
                    .as(predicate.toString())
                    .containsExactly(1);
            assertThat(readPks(table, predicate, true)).as(predicate.toString()).containsExactly(1);
        }

        // the filter is applied at all: a value outside the file's range skips it
        assertThat(readPks(table, builder.equal(1, 1.0d), false)).isEmpty();
        assertThat(readPks(table, builder.equal(2, 1.0f), false)).isEmpty();
    }

    private static List<Predicate> zeroPredicates(
            PredicateBuilder builder, int field, Object zero, Object one) {
        // more than 20 literals keep IN as a single leaf instead of an OR of equals
        List<Object> manyWithZero = new ArrayList<>();
        for (int i = 1; i <= 21; i++) {
            manyWithZero.add(one instanceof Double ? (Object) (double) i : (Object) (float) i);
        }
        manyWithZero.add(zero);
        return Arrays.asList(
                builder.equal(field, zero),
                builder.in(field, Arrays.asList(zero, one)),
                builder.in(field, manyWithZero),
                builder.greaterOrEqual(field, zero),
                builder.lessOrEqual(field, zero),
                builder.between(field, zero, zero));
    }

    private static List<Integer> readPks(
            FileStoreTable table, Predicate predicate, boolean executeFilter) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        List<Integer> pks = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader =
                    executeFilter
                            ? readBuilder.newRead().executeFilter().createReader(split)
                            : readBuilder.newRead().createReader(split)) {
                reader.forEachRemaining(row -> pks.add(row.getInt(0)));
            }
        }
        return pks;
    }
}
