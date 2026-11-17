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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/** Scalar BTree candidates must satisfy the complete filter before vector ranking. */
public class BTreeScalarFilterCompletenessTest extends TableTestBase {
    private static final String[] NAMES = {"abacus", "alpha", "zebra"};
    private static final double[] PRICES = {10.0d, 20.0d, 30.0d};
    private static final float[][] VECTORS = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};

    @ParameterizedTest
    @CsvSource({"full,true", "full,false", "detail,true", "detail,false"})
    public void sameFieldUnsupportedStringMustNotRankNonMatchingRow(String mode, boolean refine)
            throws Exception {
        Fixture fixture = prepare(mode, refine);
        PredicateBuilder b = new PredicateBuilder(fixture.table.rowType());
        // Both leaves use name, but only the first is supported. Field coverage cannot
        // distinguish this from the fully supported conjunction tested below.
        Predicate filter =
                PredicateBuilder.and(
                        b.startsWith(1, BinaryString.fromString("a")),
                        b.startsWith(1, BinaryString.fromString("ab")).negate().get());
        Observation actual = observe(fixture, filter);
        assertAll(
                () -> assertThat(actual.scalar).containsExactly(1L),
                () -> assertThat(actual.exact).as("partial predicate must not be exact").isFalse(),
                () ->
                        assertThat(actual.vector)
                                .as("filter must be checked before top-k")
                                .isEqualTo(
                                        refine
                                                ? Collections.singletonList(1L)
                                                : Collections.emptyList()));
    }

    @ParameterizedTest
    @CsvSource({"full,true", "full,false", "detail,true", "detail,false"})
    public void sameFieldUnsupportedIsNaNMustReturnNoRows(String mode, boolean refine)
            throws Exception {
        Fixture fixture = prepare(mode, refine);
        PredicateBuilder b = new PredicateBuilder(fixture.table.rowType());
        Predicate filter = PredicateBuilder.and(b.greaterThan(2, 0.0d), b.isNaN(2));
        Observation actual = observe(fixture, filter);
        assertAll(
                () -> assertThat(actual.scalar).isEmpty(),
                () -> assertThat(actual.exact).as("partial predicate must not be exact").isFalse(),
                () -> assertThat(actual.vector).as("finite prices cannot satisfy isNaN").isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"full,true", "full,false", "detail,true", "detail,false"})
    public void fullySupportedEqualityControl(String mode, boolean refine) throws Exception {
        Fixture fixture = prepare(mode, refine);
        Predicate filter =
                new PredicateBuilder(fixture.table.rowType())
                        .equal(1, BinaryString.fromString("alpha"));
        Observation actual = observe(fixture, filter);
        assertAll(
                () -> assertThat(actual.scalar).containsExactly(1L),
                () -> assertThat(actual.exact).isTrue(),
                () -> assertThat(actual.vector).containsExactly(1L));
    }

    @ParameterizedTest
    @CsvSource({"full,true", "full,false", "detail,true", "detail,false"})
    public void differentFieldUnsupportedPredicateControl(String mode, boolean refine)
            throws Exception {
        Fixture fixture = prepare(mode, refine);
        PredicateBuilder b = new PredicateBuilder(fixture.table.rowType());
        Predicate filter =
                PredicateBuilder.and(
                        b.greaterThan(2, 0.0d),
                        b.startsWith(1, BinaryString.fromString("ab")).negate().get());
        Observation actual = observe(fixture, filter);
        assertAll(
                () -> assertThat(actual.scalar).containsExactly(1L, 2L),
                () -> assertThat(actual.exact).isFalse(),
                () ->
                        assertThat(actual.vector)
                                .isEqualTo(
                                        refine
                                                ? Collections.singletonList(1L)
                                                : Collections.emptyList()));
    }

    @ParameterizedTest
    @CsvSource({"full,true", "full,false", "detail,true", "detail,false"})
    public void fullySupportedSameFieldConjunctionStaysExact(String mode, boolean refine)
            throws Exception {
        Fixture fixture = prepare(mode, refine);
        PredicateBuilder b = new PredicateBuilder(fixture.table.rowType());
        Predicate filter =
                PredicateBuilder.and(
                        b.startsWith(1, BinaryString.fromString("a")),
                        b.notEqual(1, BinaryString.fromString("abacus")));
        Observation actual = observe(fixture, filter);
        assertAll(
                () -> assertThat(actual.scalar).containsExactly(1L),
                () -> assertThat(actual.exact).isTrue(),
                () -> assertThat(actual.vector).containsExactly(1L));
    }

    private Fixture prepare(String mode, boolean refine) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .column("vec", new ArrayType(DataTypes.FLOAT()))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), mode)
                        .option(
                                CoreOptions.GLOBAL_INDEX_FILTER_REFINE_FROM_DATA.key(),
                                Boolean.toString(refine))
                        .option("test.vector.dimension", "2")
                        .option("test.vector.metric", "l2")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        InternalRow[] rows = new InternalRow[NAMES.length];
        for (int i = 0; i < rows.length; i++) {
            rows[i] =
                    GenericRow.of(
                            i,
                            BinaryString.fromString(NAMES[i]),
                            PRICES[i],
                            new GenericArray(VECTORS[i]));
        }
        write(table, rows);

        List<IndexFileMeta> scalarFiles = new ArrayList<>();
        scalarFiles.addAll(
                buildIndex(
                        table,
                        "btree",
                        "name",
                        Arrays.stream(NAMES).map(BinaryString::fromString).toArray()));
        scalarFiles.addAll(
                buildIndex(
                        table, "btree", "price", new Object[] {PRICES[0], PRICES[1], PRICES[2]}));
        List<IndexFileMeta> allFiles = new ArrayList<>(scalarFiles);
        allFiles.addAll(
                buildIndex(table, TestVectorGlobalIndexerFactory.IDENTIFIER, "vec", VECTORS));
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.indexIncrement(allFiles),
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
        return new Fixture(table, scalarFiles);
    }

    private static List<IndexFileMeta> buildIndex(
            FileStoreTable table, String type, String column, Object[] sortedValues)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField field = table.rowType().getField(column);
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, type, Collections.singletonList(field), options);
        for (int i = 0; i < sortedValues.length; i++) {
            writer.write(sortedValues[i], i);
        }
        return GlobalIndexBuilderUtils.toIndexFileMetas(
                table.fileIO(),
                table.store().pathFactory().globalIndexFileFactory(),
                table.coreOptions(),
                new Range(0, sortedValues.length - 1),
                field.id(),
                type,
                writer.finish());
    }

    private static Observation observe(Fixture fixture, Predicate filter) throws Exception {
        FileStoreTable table = fixture.table;
        GlobalIndexEvaluator.Evaluation evaluation;
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForScalarFilters(
                                table, null, null, fixture.scalarFiles)
                        .get()) {
            evaluation = scanner.scanWithCoverage(filter).get();
        }
        boolean exact = FilteredRowIdReader.isExact(table.rowType(), filter, evaluation);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filter);
        List<Long> scalar = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> scalar.add((long) row.getInt(0)));
        }
        List<Long> vector = new ArrayList<>();
        table.newVectorSearchBuilder()
                .withVector(new float[] {1.0f, 0.0f})
                .withVectorColumn("vec")
                .withLimit(1)
                .withFilter(filter)
                .executeLocal()
                .results()
                .forEach(vector::add);
        return new Observation(exact, scalar, vector);
    }

    private static final class Fixture {
        final FileStoreTable table;
        final List<IndexFileMeta> scalarFiles;

        Fixture(FileStoreTable table, List<IndexFileMeta> scalarFiles) {
            this.table = table;
            this.scalarFiles = scalarFiles;
        }
    }

    private static final class Observation {
        final boolean exact;
        final List<Long> scalar;
        final List<Long> vector;

        Observation(boolean exact, List<Long> scalar, List<Long> vector) {
            this.exact = exact;
            this.scalar = scalar;
            this.vector = vector;
        }
    }
}
