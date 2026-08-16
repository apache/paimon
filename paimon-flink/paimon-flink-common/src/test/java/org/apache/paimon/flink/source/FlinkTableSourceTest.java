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
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Or;
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

import java.util.ArrayList;
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
        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null);

        // col1 = 1
        List<ResolvedExpression> filters = ImmutableList.of(col1Equal1());
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(filters);
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
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null);

        // col1 = 1 && p1 = 1 => [p1 = 1]
        List<ResolvedExpression> filters = ImmutableList.of(col1Equal1(), p1Equal1());
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // col1 = 1 && p2 like '%a' => None
        filters = ImmutableList.of(col1Equal1(), p2Like("%a"));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(filters);

        // col1 = 1 && p2 like 'a%' => [p2 like 'a%']
        filters = ImmutableList.of(col1Equal1(), p2Like("a%"));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // rand(42) > 0.1 => None
        filters = ImmutableList.of(rand());
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(filters);

        // upper(p1) = "A" => [upper(p1) = "A"]
        filters = ImmutableList.of(upperP2EqualA());
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(filters);

        // col1 = 1 && (p2 like 'a%' or p1 = 1) => [p2 like 'a%' or p1 = 1]
        filters = ImmutableList.of(col1Equal1(), or(p2Like("a%"), p1Equal1()));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // col1 = 1 && (p2 like '%a' or p1 = 1) => None
        filters = ImmutableList.of(col1Equal1(), or(p2Like("%a"), p1Equal1()));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .containsExactlyInAnyOrder(filters.toArray(new ResolvedExpression[0]));

        // col1 = 1 && (p2 like 'a%' && p1 = 1) => [p2 like 'a%' && p1 = 1]
        filters = ImmutableList.of(col1Equal1(), and(p2Like("a%"), p1Equal1()));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(ImmutableList.of(filters.get(0)));

        // col1 = 1 && (p2 like '%a' && p1 = 1) => None
        filters = ImmutableList.of(col1Equal1(), and(p2Like("%a"), p1Equal1()));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .containsExactlyInAnyOrder(filters.toArray(new ResolvedExpression[0]));

        // p2 like 'a%' && (col1 = 1 or p1 = 1) => [col1 = 1 or p1 = 1]
        filters = ImmutableList.of(p2Like("a%"), or(col1Equal1(), p1Equal1()));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(ImmutableList.of(filters.get(1)));

        // p2 like 'a%' && (col1 = 1 && p1 = 1) => [col1 = 1 && p1 = 1]
        filters = ImmutableList.of(p2Like("a%"), and(col1Equal1(), p1Equal1()));
        Assertions.assertThat(tableSource.applyFilters(filters).getRemainingFilters())
                .isEqualTo(ImmutableList.of(filters.get(1)));
    }

    // ==================== Nested OR Tree Tests ====================
    //
    // These tests construct OR trees in various shapes — mimicking what Flink's
    // SQL Planner may produce when expanding IN(v1,...,vN) — and pass them directly
    // to applyFilters, bypassing Flink's ExpressionResolver.
    //
    // They verify that PredicateConverter flattens any nesting shape into a flat list
    // of predicates, which PredicateBuilder.or() combines into a binary tree.

    @Test
    public void testApplyFiltersLargeNestedOr() throws Exception {
        Table table = createStringTable();

        // 10000 values: the nested OR tree is flattened and combined into a
        // binary tree (depth ~14), preventing StackOverflowError.
        int size = 10000;
        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null);
        ResolvedExpression orTree = buildNestedOrTree(size);

        tableSource.applyFilters(ImmutableList.of(orTree));

        Assertions.assertThat(tableSource.predicate).isNotNull();
        Assertions.assertThat(tableSource.predicate).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) tableSource.predicate;
        Assertions.assertThat(compound.function()).isEqualTo(Or.INSTANCE);
        Assertions.assertThat(compound.children()).hasSize(2);
    }

    @Test
    public void testApplyFiltersRightFoldOrTree() throws Exception {
        Table table = createStringTable();
        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null);

        // Right-fold tree: OR(OR(OR(=(f,0), =(f,1)), =(f,2)), ...) with 25 values
        ResolvedExpression orTree = buildRightFoldOrTree(25);

        tableSource.applyFilters(ImmutableList.of(orTree));

        // Regardless of tree shape → flattened → binary tree
        Assertions.assertThat(tableSource.predicate).isNotNull();
        Assertions.assertThat(tableSource.predicate).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) tableSource.predicate;
        Assertions.assertThat(compound.function()).isEqualTo(Or.INSTANCE);
        Assertions.assertThat(compound.children()).hasSize(2);
    }

    @Test
    public void testApplyFiltersBalancedOrTree() throws Exception {
        Table table = createStringTable();
        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null);

        // Balanced tree: OR(OR(=(f,0), =(f,1)), OR(=(f,2), =(f,3)), ...) with 25 values
        ResolvedExpression orTree = buildBalancedOrTree(25);

        tableSource.applyFilters(ImmutableList.of(orTree));

        Assertions.assertThat(tableSource.predicate).isNotNull();
        Assertions.assertThat(tableSource.predicate).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) tableSource.predicate;
        Assertions.assertThat(compound.function()).isEqualTo(Or.INSTANCE);
        Assertions.assertThat(compound.children()).hasSize(2);
    }

    @Test
    public void testApplyFiltersFlatOrWithMultipleChildren() throws Exception {
        Table table = createStringTable();
        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null);

        // Flat OR with >2 children in a single CallExpression
        ResolvedExpression orExpr = buildFlatOrExpression(25);

        tableSource.applyFilters(ImmutableList.of(orExpr));

        Assertions.assertThat(tableSource.predicate).isNotNull();
        Assertions.assertThat(tableSource.predicate).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) tableSource.predicate;
        Assertions.assertThat(compound.function()).isEqualTo(Or.INSTANCE);
        Assertions.assertThat(compound.children()).hasSize(2);
    }

    private Table createStringTable() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        Schema schema = Schema.newBuilder().column("contract_address", DataTypes.STRING()).build();
        TableSchema tableSchema = new SchemaManager(fileIO, tablePath).createTable(schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    /**
     * Build a nested binary OR tree mimicking Flink's IN-to-OR expansion: OR(=(f, v1), OR(=(f, v2),
     * OR(..., OR(=(f, vN-1), =(f, vN)))))
     *
     * <p>Built iteratively (inside-out) to avoid StackOverflow during construction.
     */
    private ResolvedExpression buildNestedOrTree(int count) {
        FieldReferenceExpression field =
                new FieldReferenceExpression(
                        "contract_address", org.apache.flink.table.api.DataTypes.STRING(), 0, 0);

        // Start with innermost: =(contract_address, addr_{count-1})
        ResolvedExpression result = equalExpr(field, count - 1);

        // Wrap outward: OR(=(field, addr_i), result) for i = count-2 down to 0
        for (int i = count - 2; i >= 0; i--) {
            result = or(equalExpr(field, i), result);
        }
        return result;
    }

    /**
     * Build a right-fold binary OR tree: OR(OR(OR(=(f, v0), =(f, v1)), =(f, v2)), =(f, v3), ...).
     */
    private ResolvedExpression buildRightFoldOrTree(int count) {
        FieldReferenceExpression field =
                new FieldReferenceExpression(
                        "contract_address", org.apache.flink.table.api.DataTypes.STRING(), 0, 0);
        ResolvedExpression result = equalExpr(field, 0);
        for (int i = 1; i < count; i++) {
            result = or(result, equalExpr(field, i));
        }
        return result;
    }

    /** Build a balanced binary OR tree: OR(OR(=(f, v0), =(f, v1)), OR(=(f, v2), =(f, v3)), ...). */
    private ResolvedExpression buildBalancedOrTree(int count) {
        FieldReferenceExpression field =
                new FieldReferenceExpression(
                        "contract_address", org.apache.flink.table.api.DataTypes.STRING(), 0, 0);
        List<ResolvedExpression> leaves = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            leaves.add(equalExpr(field, i));
        }
        while (leaves.size() > 1) {
            List<ResolvedExpression> next = new ArrayList<>();
            for (int i = 0; i < leaves.size(); i += 2) {
                if (i + 1 < leaves.size()) {
                    next.add(or(leaves.get(i), leaves.get(i + 1)));
                } else {
                    next.add(leaves.get(i));
                }
            }
            leaves = next;
        }
        return leaves.get(0);
    }

    /** Build a flat OR CallExpression with more than 2 children. */
    private ResolvedExpression buildFlatOrExpression(int count) {
        FieldReferenceExpression field =
                new FieldReferenceExpression(
                        "contract_address", org.apache.flink.table.api.DataTypes.STRING(), 0, 0);
        List<ResolvedExpression> children = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            children.add(equalExpr(field, i));
        }
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.OR,
                children,
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ResolvedExpression equalExpr(FieldReferenceExpression field, int i) {
        return CallExpression.anonymous(
                BuiltInFunctionDefinitions.EQUALS,
                ImmutableList.of(field, addressLiteral(i)),
                org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private ValueLiteralExpression addressLiteral(int i) {
        return new ValueLiteralExpression(
                String.format("0x%040x", i),
                org.apache.flink.table.api.DataTypes.STRING().notNull());
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
