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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests reading a table with an IsNaN query authorization filter. */
class IsNaNQueryAuthTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest(name = "{0}")
    @MethodSource("floatingPointValues")
    void testIsNaNRowFilter(DataType valueType, Object[] values) throws Exception {
        Path path = new Path(tempDir.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        FileSystemSchemaManager schemaManager = new FileSystemSchemaManager(fileIO, path);
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "value", valueType));
        Map<String, String> options = new HashMap<>();
        options.put("query-auth.enabled", "true");
        options.put("file.format", "parquet");
        schemaManager.createTable(
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        ""));
        TableSchema schema = schemaManager.latest().get();
        FileStoreTable table = FileStoreTableFactory.create(fileIO, path, schema);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < values.length; i++) {
                write.write(GenericRow.of(i, values[i]));
            }
            commit.commit(write.prepareCommit());
        }
        assertThat(readKeys(table)).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6);

        Predicate filter = new PredicateBuilder(rowType).isNaN(1);
        TableQueryAuthResult authResult =
                new TableQueryAuthResult(
                        Collections.singletonList(JsonSerdeUtil.toFlatJson(filter)), null);
        Catalog catalog = Mockito.mock(Catalog.class);
        Mockito.when(catalog.authTableQuery(Mockito.any(), Mockito.any())).thenReturn(authResult);
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        Identifier.create("default", "t"),
                        null,
                        () -> catalog,
                        null,
                        null,
                        null,
                        false,
                        false);

        assertThat(readKeys(FileStoreTableFactory.create(fileIO, path, schema, environment)))
                .containsExactly(0);
    }

    private static Stream<Arguments> floatingPointValues() {
        return Stream.of(
                Arguments.of(
                        DataTypes.FLOAT(),
                        new Object[] {
                            Float.NaN,
                            1.0f,
                            0.0f,
                            -0.0f,
                            Float.POSITIVE_INFINITY,
                            Float.NEGATIVE_INFINITY,
                            null
                        }),
                Arguments.of(
                        DataTypes.DOUBLE(),
                        new Object[] {
                            Double.NaN,
                            1.0d,
                            0.0d,
                            -0.0d,
                            Double.POSITIVE_INFINITY,
                            Double.NEGATIVE_INFINITY,
                            null
                        }));
    }

    private List<Integer> readKeys(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder().withProjection(new int[] {0});
        List<Integer> keys = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> keys.add(row.getInt(0)));
        }
        return keys;
    }
}
