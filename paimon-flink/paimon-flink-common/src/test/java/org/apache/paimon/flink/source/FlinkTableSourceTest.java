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

package org.apache.paimon.flink.source;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Test for {@link FlinkTableSource}. */
public class FlinkTableSourceTest extends TableTestBase {

    @Test
    public void testApplyFilterNonPartitionTable() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        Schema schema = Schema.newBuilder().column("col1", DataTypes.INT()).build();
        TableSchema tableSchema = new SchemaManager(fileIO, tablePath).createTable(schema);
        Table table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
        FlinkTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null, null);

        // col1 = 1
        List<ResolvedExpression> filters = ImmutableList.of(col1Equal1());
        Assertions.assertThat(tableSource.pushFilters(filters)).isEqualTo(filters);
    }

    @Test
    public void testApplyPartitionTable() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        Schema schema =
                Schema.newBuilder()
                        .column("col1", DataTypes.INT())
                        .column("col2", DataTypes.INT())
                        .column("p1", DataTypes.INT())
                        .column("p2", DataTypes.STRING())
                        .partitionKeys("p1", "p2")
                        .build();
        TableSchema tableSchema = new SchemaManager(fileIO, tablePath).createTable(schema);
        Table table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
        FlinkTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null, null);

        // col1 = 1 && p1 = 1 => [p1 = 1]
        List<ResolvedExpression> filters = ImmutableList.of(col1Equal1(), p1Equal1());
        Assertions.assertThat(tableSource.pushFilters(filters))
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // col1 = 1 && p2 like '%a' => None
        filters = ImmutableList.of(col1Equal1(), p2Like("%a"));
        Assertions.assertThat(tableSource.pushFilters(filters)).isEqualTo(filters);

        // col1 = 1 && p2 like 'a%' => [p2 like 'a%']
        filters = ImmutableList.of(col1Equal1(), p2Like("a%"));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // rand(42) > 0.1 => None
        filters = ImmutableList.of(rand());
        Assertions.assertThat(tableSource.pushFilters(filters)).isEqualTo(filters);

        // upper(p1) = "A" => [upper(p1) = "A"]
        filters = ImmutableList.of(upperP2EqualA());
        Assertions.assertThat(tableSource.pushFilters(filters)).isEqualTo(filters);

        // col1 = 1 && (p2 like 'a%' or p1 = 1) => [p2 like 'a%' or p1 = 1]
        filters = ImmutableList.of(col1Equal1(), or(p2Like("a%"), p1Equal1()));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // col1 = 1 && (p2 like '%a' or p1 = 1) => None
        filters = ImmutableList.of(col1Equal1(), or(p2Like("%a"), p1Equal1()));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .containsExactlyInAnyOrder(filters.toArray(new ResolvedExpression[0]));

        // col1 = 1 && (p2 like 'a%' && p1 = 1) => [p2 like 'a%' && p1 = 1]
        filters = ImmutableList.of(col1Equal1(), and(p2Like("a%"), p1Equal1()));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // col1 = 1 && (p2 like '%a' && p1 = 1) => None
        filters = ImmutableList.of(col1Equal1(), and(p2Like("%a"), p1Equal1()));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .containsExactlyInAnyOrder(filters.toArray(new ResolvedExpression[0]));

        // p2 like 'a%' && (col1 = 1 or p1 = 1) => [col1 = 1 or p1 = 1]
        filters = ImmutableList.of(p2Like("a%"), or(col1Equal1(), p1Equal1()));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .isEqualTo(ImmutableList.of(filters.get(1)));

        // p2 like 'a%' && (col1 = 1 && p1 = 1) => [col1 = 1 && p1 = 1]
        filters = ImmutableList.of(p2Like("a%"), and(col1Equal1(), p1Equal1()));
        Assertions.assertThat(tableSource.pushFilters(filters))
                .isEqualTo(ImmutableList.of(filters.get(1)));
    }

    private ResolvedExpression col1Equal1() {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.EQUALS,
                ImmutableList.of(
                        new FieldReferenceExpression(
                                "col1", org.apache.flink.table.api.DataTypes.INT(), 0, 0),
                        new ValueLiteralExpression(
                                1, org.apache.flink.table.api.DataTypes.INT().notNull())),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ResolvedExpression p1Equal1() {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.EQUALS,
                ImmutableList.of(
                        new FieldReferenceExpression(
                                "p1", org.apache.flink.table.api.DataTypes.INT(), 0, 2),
                        new ValueLiteralExpression(
                                1, org.apache.flink.table.api.DataTypes.INT().notNull())),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ResolvedExpression p2Like(String literal) {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.LIKE,
                ImmutableList.of(
                        new FieldReferenceExpression(
                                "p2", org.apache.flink.table.api.DataTypes.STRING(), 0, 3),
                        new ValueLiteralExpression(
                                literal, org.apache.flink.table.api.DataTypes.STRING().notNull())),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    // where rand(42) > 0.1
    private ResolvedExpression rand() {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.GREATER_THAN,
                ImmutableList.of(
                        CallExpression.anonymous(
                                BuiltInFunctionDefinitions.RAND,
                                ImmutableList.of(
                                        new ValueLiteralExpression(
                                                42,
                                                org.apache.flink.table.api.DataTypes.INT()
                                                        .notNull())),
                                org.apache.flink.table.api.DataTypes.DOUBLE().notNull()),
                        new ValueLiteralExpression(
                                0.1, org.apache.flink.table.api.DataTypes.DOUBLE().notNull())),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ResolvedExpression upperP2EqualA() {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.EQUALS,
                ImmutableList.of(
                        CallExpression.anonymous(
                                BuiltInFunctionDefinitions.UPPER,
                                ImmutableList.of(
                                        new FieldReferenceExpression(
                                                "p2",
                                                org.apache.flink.table.api.DataTypes.STRING(),
                                                0,
                                                3)),
                                org.apache.flink.table.api.DataTypes.STRING().notNull()),
                        new ValueLiteralExpression(
                                "A", org.apache.flink.table.api.DataTypes.STRING().notNull())),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ResolvedExpression or(ResolvedExpression e1, ResolvedExpression e2) {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.OR,
                ImmutableList.of(e1, e2),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ResolvedExpression and(ResolvedExpression e1, ResolvedExpression e2) {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.AND,
                ImmutableList.of(e1, e2),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }
}
