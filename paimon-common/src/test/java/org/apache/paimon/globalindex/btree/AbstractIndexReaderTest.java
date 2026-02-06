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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.DecimalUtils;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Common test class for BTreeIndexReader. */
public abstract class AbstractIndexReaderTest {
    protected static final CacheManager CACHE_MANAGER = new CacheManager(MemorySize.VALUE_8_MB);

    protected DataType dataType;
    protected int dataNum;
    protected List<Pair<Object, Long>> data;
    protected KeySerializer keySerializer;
    protected Comparator<Object> comparator;
    protected FileIO fileIO;
    protected GlobalIndexFileReader fileReader;
    protected GlobalIndexFileWriter fileWriter;
    protected BTreeGlobalIndexer globalIndexer;
    protected Options options;

    @TempDir protected java.nio.file.Path tempPath;

    AbstractIndexReaderTest(List<Object> args) {
        this.dataType = (DataType) args.get(0);
        this.dataNum = (Integer) args.get(1);
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
        fileIO = LocalFileIO.create();
        fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "test-btree-" + UUID.randomUUID() + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };
        fileReader =
                meta ->
                        fileIO.newInputStream(
                                new Path(new Path(tempPath.toUri()), meta.filePath()));
        options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofMebiBytes(8));
        globalIndexer = new BTreeGlobalIndexer(new DataField(1, "testField", dataType), options);
        keySerializer = KeySerializer.create(dataType);
        comparator = keySerializer.createComparator();

        // generate data and sort by key
        data = new ArrayList<>(dataNum);
        DataGenerator dataGenerator = new DataGenerator();
        for (int i = 0; i < dataNum; i++) {
            data.add(Pair.of(dataType.accept(dataGenerator), (long) i));
        }
        data.sort((p1, p2) -> comparator.compare(p1.getKey(), p2.getKey()));
    }

    @TestTemplate
    public void testRangePredicate() throws Exception {
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader = prepareDataAndCreateReader()) {
            GlobalIndexResult result;
            Random random = new Random();

            for (int i = 0; i < 5; i++) {
                int literalIdx = random.nextInt(dataNum);
                Object literal = data.get(literalIdx).getKey();

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

                // 7. test between
                int betweenOffset = random.nextInt(dataNum - literalIdx);
                Object toLiteral = data.get(literalIdx + betweenOffset).getKey();
                result = reader.visitBetween(ref, literal, toLiteral).get();
                assertResult(
                        result,
                        filter(
                                obj ->
                                        comparator.compare(obj, toLiteral) <= 0
                                                && comparator.compare(obj, literal) >= 0));
            }

            // 8. test < min
            Object literal7 = data.get(0).getKey();
            result = reader.visitLessThan(ref, literal7).get();
            Assertions.assertTrue(result.results().isEmpty());

            // 9. test > max
            Object literal8 = data.get(dataNum - 1).getKey();
            result = reader.visitGreaterThan(ref, literal8).get();
            Assertions.assertTrue(result.results().isEmpty());

            // 10. test between
            if (dataType instanceof IntType) {
                Integer min = (Integer) (data.get(0).getKey());
                Integer max = (Integer) (data.get(dataNum - 1).getKey());

                result = reader.visitBetween(ref, min - 100, min - 1).get();
                Assertions.assertTrue(result.results().isEmpty());

                result = reader.visitBetween(ref, max + 1, max + 100).get();
                Assertions.assertTrue(result.results().isEmpty());
            }
        }
    }

    @TestTemplate
    public void testIsNull() throws Exception {
        // set nulls
        // make sure that there will be some btree file only containing nulls.
        for (int i = dataNum - 1; i >= dataNum * 0.85; i--) {
            data.get(i).setLeft(null);
        }

        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader = prepareDataAndCreateReader()) {
            GlobalIndexResult result;

            result = reader.visitIsNull(ref).get();
            assertResult(result, filter(Objects::isNull));

            result = reader.visitIsNotNull(ref).get();
            assertResult(result, filter(Objects::nonNull));
        }
    }

    @TestTemplate
    public void testInPredicate() throws Exception {
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader = prepareDataAndCreateReader()) {
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

    protected abstract GlobalIndexReader prepareDataAndCreateReader() throws Exception;

    protected GlobalIndexIOMeta writeData(List<Pair<Object, Long>> data) throws IOException {
        GlobalIndexParallelWriter indexWriter = globalIndexer.createWriter(fileWriter);
        for (Pair<Object, Long> pair : data) {
            indexWriter.write(pair.getKey(), pair.getValue());
        }
        List<ResultEntry> results = indexWriter.finish();
        Assertions.assertEquals(1, results.size());

        ResultEntry resultEntry = results.get(0);
        String fileName = resultEntry.fileName();
        return new GlobalIndexIOMeta(
                new Path(new Path(tempPath.toUri()), fileName),
                fileIO.getFileSize(new Path(new Path(tempPath.toUri()), fileName)),
                resultEntry.meta());
    }

    protected List<Long> filter(Predicate<Object> filter) {
        return data.stream()
                .filter(pair -> filter.test(pair.getKey()))
                .map(Pair::getValue)
                .collect(Collectors.toList());
    }

    protected void assertResult(GlobalIndexResult indexResult, List<Long> expected) {
        Iterator<Long> iter = indexResult.results().iterator();
        List<Long> result = new ArrayList<>();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** The Generator to generate test data. */
    private static class DataGenerator extends DataTypeDefaultVisitor<Object> {
        Random random = new Random();

        @Override
        public Object visit(CharType charType) {
            return BinaryString.fromString("random char " + random.nextInt());
        }

        @Override
        public Object visit(VarCharType varCharType) {
            return BinaryString.fromString("random varchar " + random.nextInt());
        }

        @Override
        public Object visit(BooleanType booleanType) {
            return random.nextBoolean();
        }

        @Override
        public Object visit(DecimalType decimalType) {
            return DecimalUtils.castFrom(
                    random.nextDouble(), decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public Object visit(TinyIntType tinyIntType) {
            return (byte) random.nextInt();
        }

        @Override
        public Object visit(SmallIntType smallIntType) {
            return (short) random.nextInt();
        }

        @Override
        public Object visit(IntType intType) {
            return random.nextInt();
        }

        @Override
        public Object visit(BigIntType bigIntType) {
            return random.nextLong();
        }

        @Override
        public Object visit(FloatType floatType) {
            return random.nextFloat();
        }

        @Override
        public Object visit(DoubleType doubleType) {
            return random.nextDouble();
        }

        @Override
        public Object visit(DateType dateType) {
            return random.nextInt(Integer.MAX_VALUE);
        }

        @Override
        public Object visit(TimeType timeType) {
            return random.nextInt(Integer.MAX_VALUE);
        }

        @Override
        public Object visit(TimestampType timestampType) {
            return Timestamp.fromMicros(random.nextLong());
        }

        @Override
        public Object visit(LocalZonedTimestampType localZonedTimestampType) {
            return LocalZoneTimestamp.fromMicros(random.nextLong());
        }

        @Override
        protected Object defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                    "Btree index do not support " + dataType + " type.");
        }
    }
}
