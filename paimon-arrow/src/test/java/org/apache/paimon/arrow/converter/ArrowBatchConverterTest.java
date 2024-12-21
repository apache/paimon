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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.deletionvectors.ApplyDeletionFileRecordIterator;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** UT for {@link ArrowBatchConverter}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ArrowBatchConverterTest {

    private static final Random RND = ThreadLocalRandom.current();
    private @TempDir java.nio.file.Path tempDir;
    private final String testMode;
    private Catalog catalog;

    private static final boolean[] NULLABLE;
    private static final RowType PRIMITIVE_TYPE;

    static {
        int cnt = 18;
        NULLABLE = new boolean[cnt];
        for (int i = 0; i < cnt; i++) {
            NULLABLE[i] = RND.nextBoolean();
        }

        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "char", DataTypes.CHAR(10).copy(NULLABLE[0])));
        dataFields.add(new DataField(1, "varchar", DataTypes.VARCHAR(20).copy(NULLABLE[1])));
        dataFields.add(new DataField(2, "boolean", DataTypes.BOOLEAN().copy(NULLABLE[2])));
        dataFields.add(new DataField(3, "binary", DataTypes.BINARY(10).copy(NULLABLE[3])));
        dataFields.add(new DataField(4, "varbinary", DataTypes.VARBINARY(20).copy(NULLABLE[4])));
        dataFields.add(new DataField(5, "decimal1", DataTypes.DECIMAL(2, 2).copy(NULLABLE[5])));
        dataFields.add(new DataField(6, "decimal2", DataTypes.DECIMAL(38, 2).copy(NULLABLE[6])));
        dataFields.add(new DataField(7, "decimal3", DataTypes.DECIMAL(10, 1).copy(NULLABLE[7])));
        dataFields.add(new DataField(8, "tinyint", DataTypes.TINYINT().copy(NULLABLE[8])));
        dataFields.add(new DataField(9, "smallint", DataTypes.SMALLINT().copy(NULLABLE[9])));
        dataFields.add(new DataField(10, "int", DataTypes.INT().copy(NULLABLE[10])));
        dataFields.add(new DataField(11, "bigint", DataTypes.BIGINT().copy(NULLABLE[11])));
        dataFields.add(new DataField(12, "float", DataTypes.FLOAT().copy(NULLABLE[12])));
        dataFields.add(new DataField(13, "double", DataTypes.DOUBLE().copy(NULLABLE[13])));
        dataFields.add(new DataField(14, "date", DataTypes.DATE().copy(NULLABLE[14])));
        dataFields.add(new DataField(15, "timestamp3", DataTypes.TIMESTAMP(3).copy(NULLABLE[15])));
        dataFields.add(new DataField(16, "timestamp6", DataTypes.TIMESTAMP(6).copy(NULLABLE[16])));
        dataFields.add(
                new DataField(
                        17,
                        "timestampLZ9",
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9).copy(NULLABLE[17])));
        PRIMITIVE_TYPE = new RowType(dataFields);
    }

    public ArrowBatchConverterTest(String testMode) {
        this.testMode = testMode;
    }

    @SuppressWarnings("unused")
    @Parameters(name = "test-mode = {0}")
    public static List<String> testMode() {
        return Arrays.asList("vectorized_without_dv", "per_row", "vectorized_with_dv");
    }

    private void testDv(boolean testDv) {
        assumeThat(testMode.equals("vectorized_with_dv")).isEqualTo(testDv);
    }

    @BeforeEach
    public void reset() throws Exception {
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new Path(tempDir.toUri().toString())));
        catalog.createDatabase("default", false);
    }

    private RecordReader.RecordIterator<InternalRow> createPrimitiveIterator(
            List<Object[]> expected, @Nullable int[] projection) throws Exception {
        // create InternalRows
        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            Object[] randomRowValues = randomRowValues(NULLABLE);
            expected.add(randomRowValues);
            rows.add(GenericRow.of(randomRowValues));
        }

        return getRecordIterator(PRIMITIVE_TYPE, rows, projection, true);
    }

    @TestTemplate
    public void testReadEmpty() throws Exception {
        testDv(false);
        List<Object[]> expected = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> iterator =
                createPrimitiveIterator(expected, new int[0]);
        testReadEmpty(iterator, expected.size());
    }

    @TestTemplate
    public void testPrimitiveTypes() throws Exception {
        testDv(false);
        List<Object[]> expected = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> iterator = createPrimitiveIterator(expected, null);
        int numRows = expected.size();
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(PRIMITIVE_TYPE, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, PRIMITIVE_TYPE, vsr);
            arrowWriter.next(numRows);
            assertThat(vsr.getRowCount()).isEqualTo(numRows);

            // validate field names
            List<String> actualNames =
                    vsr.getSchema().getFields().stream()
                            .map(Field::getName)
                            .collect(Collectors.toList());
            assertThat(actualNames).isEqualTo(PRIMITIVE_TYPE.getFieldNames());

            // validate rows
            List<FieldVector> fieldVectors = vsr.getFieldVectors();
            for (int i = 0; i < numRows; i++) {
                String expectedString = paimonObjectsToString(expected.get(i), PRIMITIVE_TYPE);
                String actualString = arrowObjectsToString(fieldVectors, PRIMITIVE_TYPE, i);
                assertThat(actualString).isEqualTo(expectedString);
            }

            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testArrayType() throws Exception {
        testDv(false);
        // build RowType
        boolean nullable = RND.nextBoolean();
        RowType nestedArrayType =
                RowType.builder()
                        .field("string_array", DataTypes.ARRAY(DataTypes.STRING()).copy(nullable))
                        .build();

        // create InternalRows
        int numRows = RND.nextInt(5) + 5;
        List<List<String>> expectedStrings = new ArrayList<>(numRows);
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (nullable && RND.nextBoolean()) {
                expectedStrings.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> StringUtils.getRandomString(RND, 1, 10))
                            .collect(Collectors.toList());
            GenericArray array =
                    new GenericArray(
                            currentStringArray.stream().map(BinaryString::fromString).toArray());
            rows.add(GenericRow.of(array));
            expectedStrings.add(currentStringArray);
        }

        RecordReader.RecordIterator<InternalRow> iterator =
                getRecordIterator(nestedArrayType, rows, null, testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedArrayType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, nestedArrayType, vsr);
            arrowWriter.next(numRows);
            assertThat(vsr.getRowCount()).isEqualTo(numRows);

            List<FieldVector> fieldVectors = vsr.getFieldVectors();
            assertThat(fieldVectors.size()).isEqualTo(1);
            FieldVector fieldVector = fieldVectors.get(0);
            assertThat(fieldVector.getName()).isEqualTo("string_array");
            for (int i = 0; i < numRows; i++) {
                Object obj = fieldVector.getObject(i);
                List<String> expected = expectedStrings.get(i);
                if (obj == null) {
                    assertThat(expected).isNull();
                } else {
                    JsonStringArrayList<Text> actual = (JsonStringArrayList<Text>) obj;
                    assertThat(actual.size()).isEqualTo(expected.size());
                    for (int j = 0; j < actual.size(); j++) {
                        assertThat(actual.get(j).toString()).isEqualTo(expected.get(j));
                    }
                }
            }

            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testMapType() throws Exception {
        testDv(false);
        // build RowType
        boolean nullable = RND.nextBoolean();
        RowType nestedMapType =
                RowType.builder()
                        .field(
                                "map",
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()).copy(nullable))
                        .build();

        // create InternalRows
        int numRows = RND.nextInt(5) + 5;
        List<Map<Integer, String>> expectedMaps = new ArrayList<>(numRows);
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (nullable && RND.nextBoolean()) {
                expectedMaps.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int currentSize = RND.nextInt(5);
            Map<Integer, String> map1 = new HashMap<>(currentSize);
            Map<Integer, BinaryString> map2 = new HashMap<>(currentSize);
            for (int j = 0; j < currentSize; j++) {
                String value = StringUtils.getRandomString(RND, 1, 10);
                map1.put(j, value);
                map2.put(j, BinaryString.fromString(value));
            }
            rows.add(GenericRow.of(new GenericMap(map2)));
            expectedMaps.add(map1);
        }

        RecordReader.RecordIterator<InternalRow> iterator =
                getRecordIterator(nestedMapType, rows, null, testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedMapType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, nestedMapType, vsr);
            arrowWriter.next(numRows);
            assertThat(vsr.getRowCount()).isEqualTo(numRows);

            List<FieldVector> fieldVectors = vsr.getFieldVectors();
            assertThat(fieldVectors.size()).isEqualTo(1);
            FieldVector fieldVector = fieldVectors.get(0);
            assertThat(fieldVector.getName()).isEqualTo("map");
            for (int i = 0; i < numRows; i++) {
                Object obj = fieldVector.getObject(i);
                Map<Integer, String> expected = expectedMaps.get(i);
                if (obj == null) {
                    assertThat(expected).isNull();
                } else {
                    JsonStringArrayList<JsonStringHashMap<String, Object>> actual =
                            (JsonStringArrayList<JsonStringHashMap<String, Object>>) obj;
                    assertThat(actual.size()).isEqualTo(expected.size());
                    for (JsonStringHashMap<String, Object> actualMap : actual) {
                        int key = (int) actualMap.get(MapVector.KEY_NAME);
                        String value = actualMap.get(MapVector.VALUE_NAME).toString();
                        assertThat(expected.get(key)).isEqualTo(value);
                    }
                }
            }

            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testMapRowType() throws Exception {
        testDv(false);
        // build RowType
        RowType nestedMapRowType =
                new RowType(
                        Collections.singletonList(
                                DataTypes.FIELD(
                                        2,
                                        "map_row",
                                        DataTypes.MAP(
                                                DataTypes.INT(),
                                                DataTypes.ROW(DataTypes.INT(), DataTypes.INT())))));

        // create InternalRows
        InternalRow row1 = GenericRow.of((Object) null);
        InternalRow row2 = GenericRow.of(new GenericMap(new HashMap<>()));

        Map<Integer, InternalRow> map3 = new HashMap<>();
        map3.put(1, GenericRow.of(1, 1));
        map3.put(2, GenericRow.of(2, null));
        map3.put(3, null);
        InternalRow row3 = GenericRow.of(new GenericMap(map3));

        RecordReader.RecordIterator<InternalRow> iterator =
                getRecordIterator(
                        nestedMapRowType,
                        Arrays.asList(row1, row2, row3),
                        null,
                        testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedMapRowType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, nestedMapRowType, vsr);
            arrowWriter.next(3);
            assertThat(vsr.getRowCount()).isEqualTo(3);

            List<FieldVector> fieldVectors = vsr.getFieldVectors();
            assertThat(fieldVectors.size()).isEqualTo(1);
            FieldVector fieldVector = fieldVectors.get(0);
            assertThat(fieldVector.getName()).isEqualTo("map_row");

            assertThat(fieldVector.getObject(0)).isNull();
            assertThat(fieldVector.getObject(1).toString()).isEqualTo("[]");
            assertThat(fieldVector.getObject(2).toString())
                    .isEqualTo(
                            "[{\"key\":1,\"value\":{\"f0\":1,\"f1\":1}},{\"key\":2,\"value\":{\"f0\":2}},{\"key\":3}]");

            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testRowType() throws Exception {
        testDv(false);
        testRowTypeImpl(false);
    }

    @TestTemplate
    public void testRowTypeWithAllNull() throws Exception {
        testDv(false);
        testRowTypeImpl(true);
    }

    private void testRowTypeImpl(boolean allNull) throws Exception {
        testDv(false);
        // build RowType
        boolean nullable = allNull || RND.nextBoolean();
        RowType nestedRowType =
                new RowType(
                        Collections.singletonList(
                                DataTypes.FIELD(19, "row", PRIMITIVE_TYPE.copy(nullable))));

        // create InternalRows
        int numRows = RND.nextInt(5) + 5;
        List<Object[]> expected = new ArrayList<>(numRows);
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (allNull || (nullable && RND.nextBoolean())) {
                expected.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }
            Object[] randomRowValues = randomRowValues(NULLABLE);
            expected.add(randomRowValues);
            rows.add(GenericRow.of(GenericRow.of(randomRowValues)));
        }

        RecordReader.RecordIterator<InternalRow> iterator =
                getRecordIterator(nestedRowType, rows, null, testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedRowType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, nestedRowType, vsr);
            arrowWriter.next(numRows);
            assertThat(vsr.getRowCount()).isEqualTo(numRows);

            List<FieldVector> fieldVectors = vsr.getFieldVectors();
            assertThat(fieldVectors.size()).isEqualTo(1);
            StructVector structVector = (StructVector) fieldVectors.get(0);
            assertThat(structVector.getName()).isEqualTo("row");
            List<FieldVector> children = structVector.getChildrenFromFields();
            // validate field names
            List<String> actualNames =
                    children.stream().map(ValueVector::getName).collect(Collectors.toList());
            assertThat(actualNames).isEqualTo(PRIMITIVE_TYPE.getFieldNames());
            // validate nested rows
            for (int i = 0; i < numRows; i++) {
                if (structVector.isNull(i)) {
                    assertThat(expected.get(i)).isNull();
                } else {
                    String expectedString = paimonObjectsToString(expected.get(i), PRIMITIVE_TYPE);
                    String actualString = arrowObjectsToString(children, PRIMITIVE_TYPE, i);
                    assertThat(actualString).isEqualTo(expectedString);
                }
            }

            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testSliceIntType() throws Exception {
        testDv(false);
        RowType rowType = RowType.builder().field("int", DataTypes.INT()).build();
        List<InternalRow> rows = new ArrayList<>(8);

        for (int i = 0; i < 8; i++) {
            rows.add(GenericRow.of(i));
        }

        RecordReader.RecordIterator<InternalRow> iterator =
                getRecordIterator(rowType, rows, null, true);
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, rowType, vsr);

            // write 3 times
            arrowWriter.next(3);
            assertThat(vsr.getRowCount()).isEqualTo(3);
            assertThat(vsr.getFieldVectors().get(0).toString()).isEqualTo("[0, 1, 2]");

            arrowWriter.next(3);
            assertThat(vsr.getRowCount()).isEqualTo(3);
            assertThat(vsr.getFieldVectors().get(0).toString()).isEqualTo("[3, 4, 5]");

            arrowWriter.next(3);
            assertThat(vsr.getRowCount()).isEqualTo(2);
            assertThat(vsr.getFieldVectors().get(0).toString()).isEqualTo("[6, 7]");

            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testDvWithSimpleRowType() throws Exception {
        testDv(true);
        boolean nullable = RND.nextBoolean();
        RowType rowType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field("value", DataTypes.STRING().copy(nullable))
                        .build();

        int numRows = RND.nextInt(100) + 200;
        List<GenericRow> rows = new ArrayList<>();
        Map<Integer, String> strings = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            if (nullable && RND.nextBoolean()) {
                rows.add(GenericRow.of(i, null));
                strings.put(i, null);
            } else {
                String value = StringUtils.getRandomString(RND, 50, 50);
                strings.put(i, value);
                rows.add(GenericRow.of(i, BinaryString.fromString(value)));
            }
        }

        Set<Integer> deleted = getDeletedPks(numRows);
        boolean readEmpty = RND.nextBoolean();
        int[] projection = readEmpty ? new int[0] : null;
        RecordReader.RecordIterator<InternalRow> iterator =
                getApplyDeletionFileRecordIterator(
                        rowType, rows, deleted, Collections.singletonList("pk"), projection, true);
        if (readEmpty) {
            testReadEmpty(iterator, numRows - deleted.size());
        } else {
            try (RootAllocator allocator = new RootAllocator()) {
                Set<Integer> expectedPks = getExpectedPks(numRows, deleted);
                VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
                ArrowBatchConverter arrowWriter = createArrowWriter(iterator, rowType, vsr);
                int expectedRowCount = rows.size() - deleted.size();
                int averageBatchRows = RND.nextInt(expectedRowCount) + 1;
                int readRows = 0;
                Set<Integer> readPks = new HashSet<>();
                while (readRows < expectedRowCount) {
                    arrowWriter.next(averageBatchRows);
                    readRows += vsr.getRowCount();
                    // validate current result
                    IntVector pkVector = (IntVector) vsr.getVector(0);
                    FieldVector valueVector = vsr.getVector(1);
                    for (int i = 0; i < vsr.getRowCount(); i++) {
                        int pk = pkVector.get(i);
                        assertThat(expectedPks).contains(pk);
                        String value = strings.get(pk);
                        if (value == null) {
                            assertThat(valueVector.isNull(i)).isTrue();
                        } else {
                            assertThat(valueVector.getObject(i).toString()).isEqualTo(value);
                        }
                        readPks.add(pk);
                    }
                }
                assertThat(readRows).isEqualTo(expectedRowCount);
                assertThat(readPks).isEqualTo(expectedPks);
                arrowWriter.close();
            }
        }
    }

    @TestTemplate
    public void testDvWithArrayType() throws Exception {
        testDv(true);
        // build RowType
        boolean nullable = RND.nextBoolean();
        RowType nestedArrayType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field("string_array", DataTypes.ARRAY(DataTypes.STRING()).copy(nullable))
                        .build();

        int numRows = RND.nextInt(100) + 200;
        List<GenericRow> rows = new ArrayList<>();
        Map<Integer, List<String>> arrayStrings = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            if (nullable && RND.nextBoolean()) {
                rows.add(GenericRow.of(i, null));
                arrayStrings.put(i, null);
                continue;
            }
            int currentSize = RND.nextInt(5);
            List<String> currentStrings =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> StringUtils.getRandomString(RND, 50, 50))
                            .collect(Collectors.toList());
            arrayStrings.put(i, currentStrings);
            BinaryString[] binaryStrings =
                    currentStrings.stream()
                            .map(BinaryString::fromString)
                            .toArray(it -> new BinaryString[currentSize]);
            rows.add(GenericRow.of(i, new GenericArray(binaryStrings)));
        }

        Set<Integer> deleted = getDeletedPks(numRows);
        RecordReader.RecordIterator<InternalRow> iterator =
                getApplyDeletionFileRecordIterator(
                        nestedArrayType,
                        rows,
                        deleted,
                        Collections.singletonList("pk"),
                        null,
                        testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            Set<Integer> expectedPks = getExpectedPks(numRows, deleted);
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedArrayType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, nestedArrayType, vsr);
            int expectedRowCount = rows.size() - deleted.size();
            int averageBatchRows = RND.nextInt(expectedRowCount) + 1;
            int readRows = 0;
            Set<Integer> readPks = new HashSet<>();
            while ((vsr = arrowWriter.next(averageBatchRows)) != null) {
                readRows += vsr.getRowCount();
                // validate current result
                IntVector pkVector = (IntVector) vsr.getVector(0);
                ListVector listVector = (ListVector) vsr.getVector(1);
                for (int i = 0; i < vsr.getRowCount(); i++) {
                    int pk = pkVector.get(i);
                    assertThat(expectedPks).contains(pk);
                    List<String> value = arrayStrings.get(pk);
                    if (value == null) {
                        assertThat(listVector.isNull(i)).isTrue();
                    } else {
                        List<?> objects = listVector.getObject(i);
                        assertThat(objects.size()).isEqualTo(value.size());
                        for (int j = 0; j < objects.size(); j++) {
                            assertThat(objects.get(j).toString()).isEqualTo(value.get(j));
                        }
                    }
                    readPks.add(pk);
                }
            }
            assertThat(readRows).isEqualTo(expectedRowCount);
            assertThat(readPks).isEqualTo(expectedPks);
            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testDvWithMapType() throws Exception {
        testDv(true);
        // build RowType
        boolean nullable = RND.nextBoolean();
        RowType nestedMapType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field(
                                "map",
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()).copy(nullable))
                        .build();

        int numRows = RND.nextInt(100) + 200;
        List<GenericRow> rows = new ArrayList<>();
        Map<Integer, List<String>> mapValueStrings = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            if (nullable && RND.nextBoolean()) {
                rows.add(GenericRow.of(i, null));
                mapValueStrings.put(i, null);
                continue;
            }
            int currentSize = RND.nextInt(5);
            List<String> currentStrings =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> StringUtils.getRandomString(RND, 50, 50))
                            .collect(Collectors.toList());
            mapValueStrings.put(i, currentStrings);
            Map<Integer, BinaryString> map =
                    IntStream.range(0, currentSize)
                            .boxed()
                            .collect(
                                    Collectors.toMap(
                                            idx -> idx,
                                            idx ->
                                                    BinaryString.fromString(
                                                            currentStrings.get(idx))));
            rows.add(GenericRow.of(i, new GenericMap(map)));
        }
        Set<Integer> deleted = getDeletedPks(numRows);
        RecordReader.RecordIterator<InternalRow> iterator =
                getApplyDeletionFileRecordIterator(
                        nestedMapType,
                        rows,
                        deleted,
                        Collections.singletonList("pk"),
                        null,
                        testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            Set<Integer> expectedPks = getExpectedPks(numRows, deleted);
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedMapType, allocator);
            ArrowBatchConverter arrowWriter = createArrowWriter(iterator, nestedMapType, vsr);
            int expectedRowCount = rows.size() - deleted.size();
            int averageBatchRows = RND.nextInt(expectedRowCount) + 1;
            int readRows = 0;
            Set<Integer> readPks = new HashSet<>();
            while ((vsr = arrowWriter.next(averageBatchRows)) != null) {
                readRows += vsr.getRowCount();
                // validate current result
                IntVector pkVector = (IntVector) vsr.getVector(0);
                MapVector mapVector = (MapVector) vsr.getVector(1);
                for (int i = 0; i < vsr.getRowCount(); i++) {
                    int pk = pkVector.get(i);
                    assertThat(expectedPks).contains(pk);
                    List<String> value = mapValueStrings.get(pk);
                    if (value == null) {
                        assertThat(mapVector.isNull(i)).isTrue();
                    } else {
                        JsonStringArrayList<?> list =
                                (JsonStringArrayList<?>) mapVector.getObject(i);
                        assertThat(list.size()).isEqualTo(value.size());
                        for (int j = 0; j < list.size(); j++) {
                            JsonStringHashMap<?, ?> map = (JsonStringHashMap<?, ?>) list.get(j);
                            assertThat(map.size()).isEqualTo(2);
                            assertThat(map.get(MapVector.KEY_NAME)).isEqualTo(j);
                            assertThat(map.get(MapVector.VALUE_NAME).toString())
                                    .isEqualTo(value.get(j));
                        }
                    }
                    readPks.add(pk);
                }
            }
            assertThat(readRows).isEqualTo(expectedRowCount);
            assertThat(readPks).isEqualTo(expectedPks);
            arrowWriter.close();
        }
    }

    @TestTemplate
    public void testDvWithRowType() throws Exception {
        testDv(true);
        // build RowType
        boolean nullable = RND.nextBoolean();
        RowType nestedRowType =
                new RowType(
                        Arrays.asList(
                                DataTypes.FIELD(19, "pk", DataTypes.INT()),
                                DataTypes.FIELD(20, "row", PRIMITIVE_TYPE.copy(nullable))));

        int numRows = RND.nextInt(100) + 200;
        List<GenericRow> rows = new ArrayList<>();
        Map<Integer, Object[]> expectedValues = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            if (nullable && RND.nextBoolean()) {
                rows.add(GenericRow.of(i, null));
                expectedValues.put(i, null);
                continue;
            }
            Object[] randomRowValues = randomRowValues(NULLABLE);
            expectedValues.put(i, randomRowValues);
            rows.add(GenericRow.of(i, GenericRow.of(randomRowValues)));
        }

        Set<Integer> deleted = getDeletedPks(numRows);
        RecordReader.RecordIterator<InternalRow> iterator =
                getApplyDeletionFileRecordIterator(
                        nestedRowType,
                        rows,
                        deleted,
                        Collections.singletonList("pk"),
                        null,
                        testMode.equals("per_row"));
        try (RootAllocator allocator = new RootAllocator()) {
            Set<Integer> expectedPks = getExpectedPks(numRows, deleted);
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(nestedRowType, allocator);
            ArrowBatchConverter arrowBatchConverter =
                    createArrowWriter(iterator, nestedRowType, vsr);
            int expectedRowCount = rows.size() - deleted.size();
            int averageBatchRows = RND.nextInt(expectedRowCount) + 1;
            int readRows = 0;
            Set<Integer> readPks = new HashSet<>();
            while ((vsr = arrowBatchConverter.next(averageBatchRows)) != null) {
                readRows += vsr.getRowCount();
                // validate current result
                IntVector pkVector = (IntVector) vsr.getVector(0);
                StructVector structVector = (StructVector) vsr.getVector(1);
                for (int i = 0; i < vsr.getRowCount(); i++) {
                    int pk = pkVector.get(i);
                    assertThat(expectedPks).contains(pk);
                    Object[] expected = expectedValues.get(pk);
                    if (expected == null) {
                        assertThat(structVector.isNull(i)).isTrue();
                    } else {
                        String expectedString = paimonObjectsToString(expected, PRIMITIVE_TYPE);
                        String actualString =
                                arrowObjectsToString(
                                        structVector.getChildrenFromFields(), PRIMITIVE_TYPE, i);
                        assertThat(actualString).isEqualTo(expectedString);
                    }
                    readPks.add(pk);
                }
            }
            assertThat(readRows).isEqualTo(expectedRowCount);
            assertThat(readPks).isEqualTo(expectedPks);
            arrowBatchConverter.close();
        }
    }

    private Set<Integer> getDeletedPks(int numRows) {
        int numDeleted = RND.nextInt(50) + 1;
        Set<Integer> deleted = new HashSet<>();
        while (deleted.size() < numDeleted) {
            deleted.add(RND.nextInt(numRows));
        }
        return deleted;
    }

    private Set<Integer> getExpectedPks(int numRows, Set<Integer> deletedPks) {
        return IntStream.range(0, numRows)
                .filter(i -> !deletedPks.contains(i))
                .boxed()
                .collect(Collectors.toSet());
    }

    private void testReadEmpty(
            RecordReader.RecordIterator<InternalRow> iterator, int expectedRowCount) {
        try (RootAllocator allocator = new RootAllocator()) {
            RowType rowType = RowType.of();
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
            ArrowBatchConverter arrowBatchConverter = createArrowWriter(iterator, rowType, vsr);
            arrowBatchConverter.next(expectedRowCount);
            assertThat(vsr.getRowCount()).isEqualTo(expectedRowCount);
            assertThat(vsr.getSchema().getFields()).isEmpty();
            assertThat(vsr.getFieldVectors()).isEmpty();
            arrowBatchConverter.close();
        }
    }

    private RecordReader.RecordIterator<InternalRow> getRecordIterator(
            RowType rowType,
            List<InternalRow> rows,
            @Nullable int[] projection,
            boolean canTestParquet)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.FILE_FORMAT.key(),
                canTestParquet && RND.nextBoolean() ? "parquet" : "orc");
        FileStoreTable table = createFileStoreTable(rowType, Collections.emptyList(), options);

        StreamTableWrite write = table.newStreamWriteBuilder().newWrite();
        write.withIOManager(new IOManagerImpl(tempDir.toString()));
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();
        for (InternalRow row : rows) {
            write.write(row);
        }
        commit.commit(0, write.prepareCommit(false, 0));

        return table.newRead()
                .withProjection(projection)
                .createReader(table.newReadBuilder().newScan().plan())
                .readBatch();
    }

    private RecordReader.RecordIterator<InternalRow> getApplyDeletionFileRecordIterator(
            RowType rowType,
            List<GenericRow> rows,
            Set<Integer> deletedPks,
            List<String> primaryKeys,
            @Nullable int[] projection,
            boolean canTestParquet)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(
                CoreOptions.FILE_FORMAT.key(),
                canTestParquet && RND.nextBoolean() ? "parquet" : "orc");
        FileStoreTable table = createFileStoreTable(rowType, primaryKeys, options);

        StreamTableWrite write = table.newStreamWriteBuilder().newWrite();
        write.withIOManager(new IOManagerImpl(tempDir.toString()));
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();

        for (InternalRow row : rows) {
            write.write(row);
        }
        commit.commit(0, write.prepareCommit(true, 0));

        for (int delete : deletedPks) {
            GenericRow row = rows.get(delete);
            Object[] fields =
                    IntStream.range(0, row.getFieldCount()).mapToObj(row::getField).toArray();
            write.write(GenericRow.ofKind(RowKind.DELETE, fields));
        }
        commit.commit(1, write.prepareCommit(true, 1));

        RecordReader.RecordIterator<InternalRow> iterator =
                table.newRead()
                        .withProjection(projection)
                        .createReader(table.newReadBuilder().newScan().plan())
                        .readBatch();
        assertThat(VectorSchemaRootConverter.isVectorizedWithDv(iterator)).isTrue();
        return iterator;
    }

    private FileStoreTable createFileStoreTable(
            RowType rowType, List<String> primaryKeys, Map<String, String> options)
            throws Exception {
        Schema schema =
                new Schema(rowType.getFields(), Collections.emptyList(), primaryKeys, options, "");
        Identifier identifier = Identifier.create("default", UUID.randomUUID().toString());
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private ArrowBatchConverter createArrowWriter(
            RecordReader.RecordIterator<InternalRow> iterator,
            RowType rowType,
            VectorSchemaRoot vsr) {
        ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, rowType);
        if (testMode.equals("vectorized_without_dv")) {
            ArrowVectorizedBatchConverter batchWriter =
                    new ArrowVectorizedBatchConverter(vsr, fieldWriters);
            batchWriter.reset((VectorizedRecordIterator) iterator);
            return batchWriter;
        } else if (testMode.equals("vectorized_with_dv")) {
            ArrowVectorizedBatchConverter batchWriter =
                    new ArrowVectorizedBatchConverter(vsr, fieldWriters);
            batchWriter.reset((ApplyDeletionFileRecordIterator) iterator);
            return batchWriter;
        } else {
            ArrowPerRowBatchConverter rowWriter = new ArrowPerRowBatchConverter(vsr, fieldWriters);
            rowWriter.reset(iterator);
            return rowWriter;
        }
    }

    private Object[] randomRowValues(boolean[] nullable) {
        Object[] values = new Object[18];
        // The orc char reader will trim the string. See TreeReaderFactory.CharTreeReader
        values[0] = BinaryString.fromString(StringUtils.getRandomString(RND, 9, 9) + "A");
        values[1] = BinaryString.fromString(StringUtils.getRandomString(RND, 1, 19) + "A");
        values[2] = RND.nextBoolean();
        values[3] = randomBytes(10, 10);
        values[4] = randomBytes(1, 20);
        values[5] = Decimal.fromBigDecimal(new BigDecimal("0.22"), 2, 2);
        values[6] = Decimal.fromBigDecimal(new BigDecimal("12312455.22"), 38, 2);
        values[7] = Decimal.fromBigDecimal(new BigDecimal("12455.1"), 10, 1);
        values[8] = (byte) RND.nextInt(Byte.MAX_VALUE);
        values[9] = (short) RND.nextInt(Short.MAX_VALUE);
        values[10] = RND.nextInt();
        values[11] = RND.nextLong();
        values[12] = RND.nextFloat();
        values[13] = RND.nextDouble();
        values[14] = RND.nextInt();
        values[15] = Timestamp.fromEpochMillis(RND.nextInt(1000));
        values[16] = Timestamp.fromEpochMillis(RND.nextInt(1000), RND.nextInt(1000) * 1000);
        values[17] = Timestamp.fromEpochMillis(RND.nextInt(1000), RND.nextInt(1000_000));

        for (int i = 0; i < 18; i++) {
            if (nullable[i] && RND.nextBoolean()) {
                values[i] = null;
            }
        }

        return values;
    }

    private byte[] randomBytes(int minLength, int maxLength) {
        int len = RND.nextInt(maxLength - minLength + 1) + minLength;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) RND.nextInt(10);
        }
        return bytes;
    }

    private String paimonObjectsToString(Object[] values, RowType rowType) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 18; i++) {
            if (i > 0) {
                sb.append(" | ");
            }

            Object value = values[i];

            if (value == null) {
                sb.append("null");
                continue;
            }

            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case BINARY:
                case VARBINARY:
                    // transfer bytes to string
                    sb.append(new String((byte[]) value));
                    continue;
                case TIME_WITHOUT_TIME_ZONE:
                    // transfer integer to LocalDateTime string
                    sb.append(
                            DateTimeUtils.toLocalDateTime(
                                    (int) value, DateTimeUtils.UTC_ZONE.toZoneId()));
                    continue;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    // transfer Timestamp to epoch value
                    Timestamp timestamp = (Timestamp) value;
                    LocalZonedTimestampType type = (LocalZonedTimestampType) rowType.getTypeAt(i);
                    sb.append(ArrowUtils.timestampToEpoch(timestamp, type.getPrecision(), null));
                    continue;
                default:
                    sb.append(value);
            }
        }

        return sb.toString();
    }

    private String arrowObjectsToString(List<FieldVector> fieldVectors, RowType rowType, int row) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 18; i++) {
            if (i > 0) {
                sb.append(" | ");
            }

            FieldVector currentVector = fieldVectors.get(i);
            Object value = currentVector.getObject(row);

            if (value == null) {
                sb.append("null");
                continue;
            }

            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case BINARY:
                case VARBINARY:
                    // transfer bytes to string
                    sb.append(new String((byte[]) value));
                    continue;
                default:
                    sb.append(value);
            }
        }

        return sb.toString();
    }
}
