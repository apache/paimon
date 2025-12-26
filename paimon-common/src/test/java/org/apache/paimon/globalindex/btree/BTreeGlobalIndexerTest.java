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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
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
import org.junit.jupiter.api.extension.ExtendWith;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Test for {@link BTreeGlobalIndexer}. */
@ExtendWith(ParameterizedTestExtension.class)
public class BTreeGlobalIndexerTest {
    private DataType dataType;
    private int dataNum;
    private List<Pair<Object, Long>> data;
    private KeySerializer keySerializer;
    private Comparator<Object> comparator;
    private FileIO fileIO;
    private GlobalIndexFileReader fileReader;
    private GlobalIndexFileWriter fileWriter;
    private BTreeGlobalIndexer globalIndexer;
    private Options options;

    @TempDir java.nio.file.Path tempPath;

    public BTreeGlobalIndexerTest(List<Object> args) {
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
                        return "test-btree-" + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };
        fileReader =
                new GlobalIndexFileReader() {
                    @Override
                    public SeekableInputStream getInputStream(String fileName) throws IOException {
                        return fileIO.newInputStream(
                                new Path(new Path(tempPath.toUri()), fileName));
                    }

                    @Override
                    public Path filePath(String fileName) {
                        return new Path(new Path(tempPath.toUri()), fileName);
                    }
                };
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
        GlobalIndexIOMeta written = writeData();
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader =
                globalIndexer.createReader(fileReader, Collections.singletonList(written))) {
            GlobalIndexResult result;
            Random random = new Random();

            for (int i = 0; i < 5; i++) {
                Object literal = data.get(random.nextInt(dataNum)).getKey();

                // 1. test <= literal
                result = reader.visitLessOrEqual(ref, literal);
                assertResult(result, filter(obj -> comparator.compare(obj, literal) <= 0));

                // 2. test < literal
                result = reader.visitLessThan(ref, literal);
                assertResult(result, filter(obj -> comparator.compare(obj, literal) < 0));

                // 3. test >= literal
                result = reader.visitGreaterOrEqual(ref, literal);
                assertResult(result, filter(obj -> comparator.compare(obj, literal) >= 0));

                // 4. test > literal
                result = reader.visitGreaterThan(ref, literal);
                assertResult(result, filter(obj -> comparator.compare(obj, literal) > 0));

                // 5. test equal
                result = reader.visitEqual(ref, literal);
                assertResult(result, filter(obj -> comparator.compare(obj, literal) == 0));

                // 6. test not equal
                result = reader.visitNotEqual(ref, literal);
                assertResult(result, filter(obj -> comparator.compare(obj, literal) != 0));
            }

            // 7. test < min
            Object literal7 = data.get(0).getKey();
            result = reader.visitLessThan(ref, literal7);
            Assertions.assertTrue(result.results().isEmpty());

            // 8. test > max
            Object literal8 = data.get(dataNum - 1).getKey();
            result = reader.visitGreaterThan(ref, literal8);
            Assertions.assertTrue(result.results().isEmpty());
        }
    }

    @TestTemplate
    public void testIsNull() throws Exception {
        // set nulls
        for (int i = dataNum - 1; i >= dataNum * 0.9; i--) {
            data.get(i).setLeft(null);
        }
        GlobalIndexIOMeta written = writeData();
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader =
                globalIndexer.createReader(fileReader, Collections.singletonList(written))) {
            GlobalIndexResult result;

            result = reader.visitIsNull(ref);
            assertResult(result, filter(Objects::isNull));

            result = reader.visitIsNotNull(ref);
            assertResult(result, filter(Objects::nonNull));
        }
    }

    @TestTemplate
    public void testInPredicate() throws Exception {
        GlobalIndexIOMeta written = writeData();
        FieldRef ref = new FieldRef(1, "testField", dataType);

        try (GlobalIndexReader reader =
                globalIndexer.createReader(fileReader, Collections.singletonList(written))) {
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
                result = reader.visitIn(ref, literals);
                assertResult(result, filter(set::contains));

                // 2. test not in
                result = reader.visitNotIn(ref, literals);
                assertResult(result, filter(obj -> !set.contains(obj)));
            }
        }
    }

    private GlobalIndexIOMeta writeData() throws IOException {
        GlobalIndexWriter indexWriter = globalIndexer.createWriter(fileWriter);
        for (Pair<Object, Long> pair : data) {
            indexWriter.write(pair.getKey(), pair.getValue());
        }
        List<GlobalIndexWriter.ResultEntry> results = indexWriter.finish();
        Assertions.assertEquals(1, results.size());

        GlobalIndexWriter.ResultEntry resultEntry = results.get(0);
        String fileName = resultEntry.fileName();
        return new GlobalIndexIOMeta(
                fileName,
                fileIO.getFileSize(new Path(new Path(tempPath.toUri()), fileName)),
                resultEntry.rowRange().to - resultEntry.rowRange().from,
                resultEntry.meta());
    }

    private List<Long> filter(Predicate<Object> filter) {
        return data.stream()
                .filter(pair -> filter.test(pair.getKey()))
                .map(Pair::getValue)
                .collect(Collectors.toList());
    }

    private void assertResult(GlobalIndexResult indexResult, List<Long> expected) {
        Iterator<Long> iter = indexResult.results().iterator();
        List<Long> result = new ArrayList<>();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        org.assertj.core.api.Assertions.assertThat(result)
                .containsExactlyInAnyOrderElementsOf(expected);
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
