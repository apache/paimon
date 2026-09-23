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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.NestedProjectedRow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests filtering and query-authorization projection expansion in {@link AbstractDataTableRead}.
 */
class AbstractDataTableReadTest {

    @Test
    void testExecuteFilterWithUnprojectedFields() throws IOException {
        RowType type = RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING());
        TestingDataTableRead read =
                new TestingDataTableRead(
                        schema(type),
                        GenericRow.of(1, 1, BinaryString.fromString("first")),
                        GenericRow.of(1, 2, BinaryString.fromString("second")),
                        GenericRow.of(2, 2, BinaryString.fromString("third")),
                        GenericRow.of(2, null, BinaryString.fromString("null")),
                        GenericRow.of(2, 3, BinaryString.fromString("last")));
        PredicateBuilder builder = new PredicateBuilder(type);
        RowType outputType = type.project(new int[] {2, 0});
        read.withReadType(outputType);
        read.executeFilter();

        read.withFilter(builder.equal(1, 2));
        assertThat(readRows(read, outputType)).containsExactly("second:1", "third:2");
        // Reuse the same read across splits, then change its filter.
        assertThat(readRows(read, outputType)).containsExactly("second:1", "third:2");
        read.withFilter(PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 2)));
        assertThat(readRows(read, outputType)).containsExactly("second:1");
        read.withFilter(PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 2)));
        assertThat(readRows(read, outputType)).containsExactly("first:1", "second:1", "third:2");
        read.withFilter(builder.isNull(1));
        assertThat(readRows(read, outputType)).containsExactly("null:2");

        read.withReadType(RowType.of());
        read.withFilter(builder.equal(1, 2));
        List<Integer> arities = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(mock(Split.class))) {
            reader.forEachRemaining(row -> arities.add(row.getFieldCount()));
        }
        assertThat(arities).containsExactly(0, 0);
    }

    @Test
    void testExecuteFilterOnUnprojectedMaskedField() throws IOException {
        RowType type = RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        TestingDataTableRead read =
                new TestingDataTableRead(
                        schema(type),
                        GenericRow.of(
                                1,
                                BinaryString.fromString("wrong"),
                                BinaryString.fromString("match")),
                        GenericRow.of(
                                2,
                                BinaryString.fromString("MATCH"),
                                BinaryString.fromString("no")));
        read.withReadType(type.project(new int[] {0}));
        read.withFilter(new PredicateBuilder(type).equal(1, BinaryString.fromString("MATCH")));
        read.executeFilter();
        TableQueryAuthResult auth =
                new TableQueryAuthResult(
                        null,
                        Collections.singletonMap(
                                type.getFieldNames().get(1),
                                JsonSerdeUtil.toFlatJson(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(
                                                                2,
                                                                type.getFieldNames().get(2),
                                                                DataTypes.STRING()))))));
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createDataReader(mock(Split.class), auth)) {
            reader.forEachRemaining(
                    row -> {
                        assertThat(row.getFieldCount()).isEqualTo(1);
                        result.add(row.getInt(0));
                    });
        }
        assertThat(result).containsExactly(1);
    }

    @Test
    void testMaskedQueryFilterIsEvaluatedOnceAfterAuthorization() throws IOException {
        RowType type = RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        TestingDataTableRead read =
                new TestingDataTableRead(
                        schema(type),
                        GenericRow.of(
                                0,
                                BinaryString.fromString("wrong"),
                                BinaryString.fromString("match")),
                        GenericRow.of(
                                1,
                                BinaryString.fromString("wrong"),
                                BinaryString.fromString("match")),
                        GenericRow.of(
                                2,
                                BinaryString.fromString("MATCH"),
                                BinaryString.fromString("no")));
        read.withReadType(type.project(new int[] {0}));
        AtomicInteger evaluations = new AtomicInteger();
        PredicateBuilder builder = new PredicateBuilder(type);
        read.withFilter(
                builder.equal(
                        new CountingFieldTransform(
                                new FieldRef(1, type.getFieldNames().get(1), DataTypes.STRING()),
                                evaluations),
                        BinaryString.fromString("MATCH")));
        read.executeFilter();
        TableQueryAuthResult auth =
                new TableQueryAuthResult(
                        Collections.singletonList(
                                JsonSerdeUtil.toFlatJson(builder.greaterThan(0, 0))),
                        Collections.singletonMap(
                                type.getFieldNames().get(1),
                                JsonSerdeUtil.toFlatJson(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(
                                                                2,
                                                                type.getFieldNames().get(2),
                                                                DataTypes.STRING()))))));
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createDataReader(mock(Split.class), auth)) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        assertThat(result).containsExactly(1);
        // Only the two authorized rows reach the query expression, once each.
        assertThat(evaluations.get()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMaskedDisjunctionPreservesUnmaskedOperand(boolean executeFilter) throws IOException {
        RowType type =
                RowType.of(
                        DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        TestingDataTableRead read =
                new TestingDataTableRead(
                        schema(type),
                        GenericRow.of(
                                1,
                                0,
                                BinaryString.fromString("raw"),
                                BinaryString.fromString("match")),
                        GenericRow.of(
                                11,
                                0,
                                BinaryString.fromString("raw"),
                                BinaryString.fromString("match")),
                        GenericRow.of(
                                12,
                                1,
                                BinaryString.fromString("raw"),
                                BinaryString.fromString("no")),
                        GenericRow.of(
                                13,
                                0,
                                BinaryString.fromString("MATCH"),
                                BinaryString.fromString("no")));
        read.withReadType(type.project(new int[] {0}));
        PredicateBuilder builder = new PredicateBuilder(type);
        read.withFilter(
                PredicateBuilder.and(
                        builder.greaterThan(0, 10),
                        PredicateBuilder.or(
                                builder.equal(2, BinaryString.fromString("MATCH")),
                                builder.equal(1, 1))));
        if (executeFilter) {
            read.executeFilter();
        }
        TableQueryAuthResult auth =
                new TableQueryAuthResult(
                        null,
                        Collections.singletonMap(
                                type.getFieldNames().get(2),
                                JsonSerdeUtil.toFlatJson(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(
                                                                3,
                                                                type.getFieldNames().get(3),
                                                                DataTypes.STRING()))))));
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createDataReader(mock(Split.class), auth)) {
            reader.forEachRemaining(
                    row -> {
                        assertThat(row.getFieldCount()).isEqualTo(1);
                        result.add(row.getInt(0));
                    });
        }
        // The unmasked standalone conjunct is left to the engine unless full execution is enabled.
        assertThat(result)
                .containsExactlyElementsOf(
                        executeFilter ? Arrays.asList(11, 12) : Arrays.asList(1, 11, 12));
    }

    @Test
    void testReadersKeepTheirOwnAuthorization() throws IOException {
        RowType type = RowType.of(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        TestingDataTableRead read =
                new TestingDataTableRead(
                        schema(type),
                        GenericRow.of(
                                BinaryString.fromString("raw"),
                                BinaryString.fromString("alpha"),
                                BinaryString.fromString("beta")));
        read.withReadType(type.project(new int[] {0}));
        read.withFilter(new PredicateBuilder(type).equal(0, BinaryString.fromString("ALPHA")))
                .executeFilter();
        TableQueryAuthResult firstAuth =
                new TableQueryAuthResult(
                        null,
                        Collections.singletonMap(
                                type.getFieldNames().get(0),
                                JsonSerdeUtil.toFlatJson(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(
                                                                1,
                                                                type.getFieldNames().get(1),
                                                                DataTypes.STRING()))))));
        TableQueryAuthResult secondAuth =
                new TableQueryAuthResult(
                        null,
                        Collections.singletonMap(
                                type.getFieldNames().get(0),
                                JsonSerdeUtil.toFlatJson(
                                        new UpperTransform(
                                                Collections.singletonList(
                                                        new FieldRef(
                                                                2,
                                                                type.getFieldNames().get(2),
                                                                DataTypes.STRING()))))));
        try (RecordReader<InternalRow> first = read.createDataReader(mock(Split.class), firstAuth);
                RecordReader<InternalRow> second =
                        read.createDataReader(mock(Split.class), secondAuth)) {
            List<String> firstRows = new ArrayList<>();
            first.forEachRemaining(row -> firstRows.add(row.getString(0).toString()));
            List<String> secondRows = new ArrayList<>();
            second.forEachRemaining(row -> secondRows.add(row.getString(0).toString()));
            assertThat(firstRows).containsExactly("ALPHA");
            assertThat(secondRows).isEmpty();
        }
        read.withFilter((Predicate) null);
        List<String> rawRows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(mock(Split.class))) {
            reader.forEachRemaining(
                    row -> {
                        assertThat(row.getFieldCount()).isEqualTo(1);
                        rawRows.add(row.getString(0).toString());
                    });
        }
        assertThat(rawRows).containsExactly("raw");
    }

    private static class CountingFieldTransform extends FieldTransform {

        private static final long serialVersionUID = 1L;
        private final AtomicInteger evaluations;

        private CountingFieldTransform(FieldRef field, AtomicInteger evaluations) {
            super(field);
            this.evaluations = evaluations;
        }

        @Override
        public Object transform(InternalRow row) {
            evaluations.incrementAndGet();
            return super.transform(row);
        }

        @Override
        public Transform copyWithNewInputs(List<Object> inputs) {
            return new CountingFieldTransform((FieldRef) inputs.get(0), evaluations);
        }
    }

    private static TableSchema schema(RowType type) {
        return new TableSchema(
                0,
                type.getFields(),
                type.getFieldCount() - 1,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    @Test
    void testExecuteFilterPreservesNestedOutputProjection() throws IOException {
        RowType nested =
                new RowType(
                        Arrays.asList(
                                new DataField(1, "a", DataTypes.INT()),
                                new DataField(2, "b", DataTypes.STRING())));
        RowType type = new RowType(Collections.singletonList(new DataField(0, "profile", nested)));
        RowType outputType =
                new RowType(
                        Collections.singletonList(
                                type.getFields().get(0).newType(nested.project("b"))));
        TestingDataTableRead read =
                new TestingDataTableRead(
                        schema(type),
                        GenericRow.of(GenericRow.of(10, BinaryString.fromString("kept"))),
                        GenericRow.of((Object) null));
        read.withReadType(outputType)
                .withFilter(new PredicateBuilder(type).isNotNull(0))
                .executeFilter();
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(mock(Split.class))) {
            reader.forEachRemaining(
                    row -> {
                        InternalRow profile = row.getRow(0, 1);
                        assertThat(profile.getFieldCount()).isEqualTo(1);
                        result.add(profile.getString(0).toString());
                    });
        }
        assertThat(result).containsExactly("kept");
    }

    private static List<String> readRows(TestingDataTableRead read, RowType outputType)
            throws IOException {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(mock(Split.class))) {
            reader.forEachRemaining(
                    row -> {
                        assertThat(row.getFieldCount()).isEqualTo(outputType.getFieldCount());
                        result.add(row.getString(0) + ":" + row.getInt(1));
                    });
        }
        return result;
    }

    @Test
    void testNoProjectionResetWithoutExplicitReadType() throws IOException {
        TableSchema schema =
                new TableSchema(
                        1,
                        Collections.singletonList(new DataField(0, "value", DataTypes.STRING())),
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null);
        TestingDataTableRead read = new TestingDataTableRead(schema);

        read.createReader(mock(Split.class));

        assertThat(read.appliedReadType()).isNull();
    }

    @Test
    void testMaskDependenciesPreserveNestedProjectionAndSkipUnselectedMasks() throws IOException {
        RowType fullProfile =
                new RowType(
                        Arrays.asList(
                                new DataField(1, "a", DataTypes.INT()),
                                new DataField(2, "b", DataTypes.STRING())));
        DataField profile = new DataField(0, "profile", fullProfile);
        DataField protectedField = new DataField(3, "protected", DataTypes.STRING());
        DataField seed = new DataField(4, "seed", DataTypes.STRING());
        DataField unused = new DataField(5, "unused", DataTypes.STRING());
        DataField unusedSeed = new DataField(6, "unused_seed", DataTypes.STRING());
        TableSchema schema =
                new TableSchema(
                        1,
                        Arrays.asList(profile, protectedField, seed, unused, unusedSeed),
                        6,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null);

        RowType prunedProfile =
                new RowType(Collections.singletonList(new DataField(2, "b", DataTypes.STRING())));
        RowType requestedType =
                new RowType(Arrays.asList(profile.newType(prunedProfile), protectedField));
        TestingDataTableRead read = new TestingDataTableRead(schema);
        read.withReadType(requestedType);

        Map<String, String> masks = new LinkedHashMap<>();
        masks.put(
                "protected",
                JsonSerdeUtil.toFlatJson(
                        new UpperTransform(
                                Collections.singletonList(
                                        new FieldRef(4, "seed", DataTypes.STRING())))));
        masks.put(
                "unused",
                JsonSerdeUtil.toFlatJson(
                        new UpperTransform(
                                Collections.singletonList(
                                        new FieldRef(6, "unused_seed", DataTypes.STRING())))));

        read.createAuthedReader(new TableQueryAuthResult(null, masks));

        assertThat(read.appliedReadType().getFieldNames())
                .containsExactly("profile", "protected", "seed");
        assertThat(read.appliedReadType().getTypeAt(0)).isEqualTo(prunedProfile);

        read.createAuthedReader(new TableQueryAuthResult(null, Collections.emptyMap()));

        assertThat(read.appliedReadType()).isEqualTo(requestedType);
    }

    private static class TestingDataTableRead extends AbstractDataTableRead {

        private RowType appliedReadType;
        private final List<InternalRow> rows;

        private TestingDataTableRead(TableSchema schema, InternalRow... rows) {
            super(schema);
            this.rows = Arrays.asList(rows);
        }

        @Override
        public void applyReadType(RowType readType) {
            appliedReadType = readType;
        }

        @Override
        public RecordReader<InternalRow> reader(Split split) {
            RowType type = appliedReadType == null ? schema().logicalRowType() : appliedReadType;
            NestedProjectedRow projection =
                    NestedProjectedRow.create(schema().logicalRowType(), type);
            InternalRowSerializer serializer = new InternalRowSerializer(type);
            List<InternalRow> projected = new ArrayList<>();
            for (InternalRow row : rows) {
                projected.add(
                        serializer.copy(projection == null ? row : projection.replaceRow(row)));
            }
            return new IteratorRecordReader<>(projected.iterator());
        }

        @Override
        protected InnerTableRead innerWithFilter(Predicate predicate) {
            return this;
        }

        private void createAuthedReader(TableQueryAuthResult authResult) throws IOException {
            createDataReader(mock(Split.class), authResult);
        }

        private RowType appliedReadType() {
            return appliedReadType;
        }
    }
}
