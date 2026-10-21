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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Table reads of mosaic files whose FLOAT/DOUBLE stats are collected. Native bounds can hide NaN or
 * {@code -0.0}, so neither manifest pruning nor row-group pruning may drop a matching row.
 */
class MosaicFloatingPointBoundsTest {

    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @ParameterizedTest
    @ValueSource(strings = {"float", "double"})
    void testLeadingNaNDoesNotHideFiniteRows(String typeName) throws Exception {
        boolean isFloat = "float".equals(typeName);
        Table table = createTable(isFloat);
        write(table, value(isFloat, Double.NaN), value(isFloat, -5.0), value(isFloat, 3.0));

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        assertMatches(table, builder.lessThan(1, value(isFloat, 0.0)), 2);
        assertMatches(table, builder.equal(1, value(isFloat, -5.0)), 2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"float", "double"})
    void testSignedZeroIsNotHidden(String typeName) throws Exception {
        boolean isFloat = "float".equals(typeName);
        Table positiveFirst = createTable(isFloat, "positive_first");
        write(positiveFirst, value(isFloat, 0.0), value(isFloat, -0.0));
        Table negativeFirst = createTable(isFloat, "negative_first");
        write(negativeFirst, value(isFloat, -0.0), value(isFloat, 0.0));

        PredicateBuilder builder = new PredicateBuilder(positiveFirst.rowType());
        assertMatches(positiveFirst, builder.equal(1, value(isFloat, -0.0)), 2);
        assertMatches(negativeFirst, builder.equal(1, value(isFloat, 0.0)), 2);
    }

    /**
     * Checks the manifest level (the filtered scan still plans the file), the row-group level (the
     * filtered reader still returns the row group of an unfiltered plan) and the final result.
     */
    private void assertMatches(Table table, Predicate filter, int expectedId) throws Exception {
        ReadBuilder filtered = table.newReadBuilder().withFilter(filter);
        assertThat(filtered.newScan().plan().splits()).as("manifest pruning").isNotEmpty();

        List<Split> allSplits = table.newReadBuilder().newScan().plan().splits();
        assertThat(readIds(filtered, allSplits, false))
                .as("row-group pruning")
                .contains(expectedId);

        assertThat(readIds(filtered, filtered.newScan().plan().splits(), true))
                .as("filtered result")
                .containsExactly(expectedId);
    }

    private static List<Integer> readIds(
            ReadBuilder builder, List<Split> splits, boolean executeFilter) throws Exception {
        List<Integer> ids = new ArrayList<>();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader =
                    executeFilter
                            ? builder.newRead().executeFilter().createReader(split)
                            : builder.newRead().createReader(split)) {
                reader.forEachRemaining(row -> ids.add(row.getInt(0)));
            }
        }
        return ids;
    }

    private Table createTable(boolean isFloat) throws Exception {
        return createTable(isFloat, "t");
    }

    private Table createTable(boolean isFloat, String name) throws Exception {
        DataType type = isFloat ? DataTypes.FLOAT() : DataTypes.DOUBLE();
        Options options = new Options();
        options.set("warehouse", tempDir.toUri().toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", true);
            Identifier identifier = Identifier.create("db", name);
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("v", type)
                            .option("file.format", "mosaic")
                            .option(MosaicFileFormat.STATS_COLUMNS.key(), "v")
                            .option("bucket", "-1")
                            .build(),
                    false);
            return catalog.getTable(identifier);
        }
    }

    /** Writes the values into a single file, with ids starting at 1 in write order. */
    private static void write(Table table, Object... values) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < values.length; i++) {
                write.write(GenericRow.of(i + 1, values[i]));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static Object value(boolean isFloat, double value) {
        return isFloat ? (Object) (float) value : (Object) value;
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
