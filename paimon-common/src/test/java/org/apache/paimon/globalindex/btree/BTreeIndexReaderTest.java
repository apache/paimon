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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Test for {@link BTreeIndexReader} to read a single file. */
@ExtendWith(ParameterizedTestExtension.class)
public class BTreeIndexReaderTest extends AbstractIndexReaderTest {

    public BTreeIndexReaderTest(List<Object> args) {
        super(args);
    }

    @SuppressWarnings("unused")
    @Parameters(name = "dataType&recordNum-{0}")
    public static List<List<Object>> getVarSeg() {
        return Arrays.asList(
                Arrays.asList(new IntType(), 10000),
                Arrays.asList(new VarCharType(VarCharType.MAX_LENGTH), 10000),
                Arrays.asList(new CharType(100), 10000),
                Arrays.asList(new FloatType(), 10000),
                Arrays.asList(new DecimalType(), 10000),
                Arrays.asList(new DoubleType(), 10000),
                Arrays.asList(new BooleanType(), 10000),
                Arrays.asList(new TinyIntType(), 10000),
                Arrays.asList(new SmallIntType(), 10000),
                Arrays.asList(new BigIntType(), 10000),
                Arrays.asList(new DateType(), 10000),
                Arrays.asList(new TimestampType(), 10000));
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
    }

    @TestTemplate
    public void testRangePredicate() throws Exception {
        GlobalIndexIOMeta written = writeData(data);
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader =
                new BTreeIndexReader(keySerializer, fileReader, written, CACHE_MANAGER)) {
            GlobalIndexResult result;
            Random random = new Random();

            for (int i = 0; i < 5; i++) {
                Object literal = data.get(random.nextInt(dataNum)).getKey();

                // 1. test <= literal
                result = reader.visitLessOrEqual(ref, literal).get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) <= 0));

                // 2. test < literal
                result = reader.visitLessThan(ref, literal).get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) < 0));

                // 3. test >= literal
                result = reader.visitGreaterOrEqual(ref, literal).get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) >= 0));

                // 4. test > literal
                result = reader.visitGreaterThan(ref, literal).get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) > 0));

                // 5. test equal
                result = reader.visitEqual(ref, literal).get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) == 0));

                // 6. test not equal
                result = reader.visitNotEqual(ref, literal).get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) != 0));
            }

            // 7. test < min
            Object literal7 = data.get(0).getKey();
            result = reader.visitLessThan(ref, literal7).get();
            Assertions.assertTrue(result.results().isEmpty());

            // 8. test > max
            Object literal8 = data.get(dataNum - 1).getKey();
            result = reader.visitGreaterThan(ref, literal8).get();
            Assertions.assertTrue(result.results().isEmpty());
        }
    }

    @TestTemplate
    public void testIsNull() throws Exception {
        // set nulls
        for (int i = dataNum - 1; i >= dataNum * 0.9; i--) {
            data.get(i).setLeft(null);
        }
        GlobalIndexIOMeta written = writeData(data);
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader =
                new BTreeIndexReader(keySerializer, fileReader, written, CACHE_MANAGER)) {
            GlobalIndexResult result;

            result = reader.visitIsNull(ref).get();
            assertResult(result, filter(Objects::isNull));

            result = reader.visitIsNotNull(ref).get();
            assertResult(result, filter(Objects::nonNull));
        }
    }

    @TestTemplate
    public void testInPredicate() throws Exception {
        GlobalIndexIOMeta written = writeData(data);
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader =
                new BTreeIndexReader(keySerializer, fileReader, written, CACHE_MANAGER)) {
            GlobalIndexResult result;
            for (int i = 0; i < 10; i++) {
                Random random = new Random(System.currentTimeMillis());
                List<Object> literals =
                        data.stream().map(Pair::getKey).collect(Collectors.toList());
                Collections.shuffle(literals, random);
                literals = literals.subList(0, (int) (dataNum * 0.1));

                TreeSet<Object> set = new TreeSet<>(comparator);
                set.addAll(literals);

                // 1. test in
                result = reader.visitIn(ref, literals).get();
                assertResult(result, filter(set::contains));

                // 2. test not in
                result = reader.visitNotIn(ref, literals).get();
                assertResult(result, filter(obj -> !set.contains(obj)));
            }
        }
    }
}
