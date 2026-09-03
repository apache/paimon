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

package org.apache.paimon.flink.action;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for sub-field-level data evolution via {@link DataEvolutionMergeIntoAction}: updating a
 * single sub-field of a nested struct column should write an incremental file containing only that
 * sub-field (a dotted write column like {@code nest.a}) aligned by row id, while the rest of the
 * struct is read back from the original file.
 */
public class NestedSubfieldMergeIntoActionITCase extends ActionITCaseBase {

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        init(warehouse);
    }

    private void prepareNestedTarget(boolean nestedFieldEnabled) throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "nest ROW<a INT, b STRING>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                if (nestedFieldEnabled) {
                                    put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                                }
                            }
                        }));
        insertInto(
                "T",
                "(1, CAST(ROW(10, 'x') AS ROW<a INT, b STRING>))",
                "(2, CAST(ROW(20, 'y') AS ROW<a INT, b STRING>))");
    }

    private void prepareSubFieldSource() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "newa INT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("S", "(1, 100)");
    }

    @Test
    public void testUpdateSingleSubFieldWritesOnlyThatLeaf() throws Exception {
        prepareNestedTarget(true);
        prepareSubFieldSource();

        builder(warehouse, database, "T")
                .withMergeCondition("T.id=S.id")
                .withMatchedUpdateSet("T.nest.a=S.newa")
                .withSourceTable("S")
                .withSinkParallelism(2)
                .build()
                .run();

        // correctness: nest.a updated for id=1, nest.b preserved, other row untouched
        testBatchRead(
                "SELECT id, nest.a, nest.b FROM T",
                Arrays.asList(changelogRow("+I", 1, 100, "x"), changelogRow("+I", 2, 20, "y")));

        // feature engaged: an incremental file written by the merge only contains nest.a
        assertThat(deltaWriteCols("T")).contains(Collections.singletonList("nest.a"));
    }

    @Test
    public void testUpdateSubFieldDisabledThrows() throws Exception {
        // data-evolution.nested-field.enabled left at its default (false)
        prepareNestedTarget(false);
        prepareSubFieldSource();

        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "T")
                                        .withMergeCondition("T.id=S.id")
                                        .withMatchedUpdateSet("T.nest.a=S.newa")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key());
    }

    @Test
    public void testDisabledOptionTreatsDotAsPartOfTopLevelColumnName() throws Exception {
        testDottedTopLevelColumn(false);
    }

    @Test
    public void testEnabledOptionTreatsDotAsPartOfTopLevelColumnName() throws Exception {
        testDottedTopLevelColumn(true);
    }

    private void testDottedTopLevelColumn(boolean nestedFieldEnabled) throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "`literal.dot` INT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                if (nestedFieldEnabled) {
                                    put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                                }
                            }
                        }));
        insertInto("T", "(1, 10)");

        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "new_value INT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap()));
        insertInto("S", "(1, 100)");

        builder(warehouse, database, "T")
                .withMergeCondition("T.id=S.id")
                .withMatchedUpdateSet("T.literal.dot=S.new_value")
                .withSourceTable("S")
                .withSinkParallelism(1)
                .build()
                .run();

        testBatchRead(
                "SELECT id, `literal.dot` FROM T",
                Collections.singletonList(changelogRow("+I", 1, 100)));
        assertThat(deltaWriteCols("T")).contains(Collections.singletonList("literal.dot"));
    }

    @Test
    public void testUpdateWholeStructStillWorks() throws Exception {
        prepareNestedTarget(true);
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "nest ROW<a INT, b STRING>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("S", "(1, CAST(ROW(100, 'z') AS ROW<a INT, b STRING>))");

        builder(warehouse, database, "T")
                .withMergeCondition("T.id=S.id")
                .withMatchedUpdateSet("T.nest=S.nest")
                .withSourceTable("S")
                .withSinkParallelism(2)
                .build()
                .run();

        testBatchRead(
                "SELECT id, nest.a, nest.b FROM T",
                Arrays.asList(changelogRow("+I", 1, 100, "z"), changelogRow("+I", 2, 20, "y")));
    }

    @Test
    public void testWholeStructAssignmentWithNarrowerSourceThrows() throws Exception {
        // target nest is ROW<a, b>; a whole-column assignment from a narrower source ROW<a> must be
        // rejected (it would otherwise be written as an incomplete whole-struct file)
        prepareNestedTarget(true);
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "nest ROW<a INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("S", "(1, CAST(ROW(100) AS ROW<a INT>))");

        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "T")
                                        .withMergeCondition("T.id=S.id")
                                        .withMatchedUpdateSet("T.nest=S.nest")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining("incompatible");
    }

    @Test
    public void testWholeStructAssignmentWithReorderedSourceThrows() throws Exception {
        // target nest is ROW<a, b>; a whole-column assignment from a same-named-but-reordered
        // source ROW<b, a> must be rejected. The write is positional (the source struct is
        // written through as-is), so accepting a reordered source would silently swap the a/b
        // values instead of failing loudly (regression for #8334 review comment on
        // isFullyCompatibleStruct).
        prepareNestedTarget(true);
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "nest ROW<b STRING, a INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("S", "(1, CAST(ROW('z', 100) AS ROW<b STRING, a INT>))");

        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "T")
                                        .withMergeCondition("T.id=S.id")
                                        .withMatchedUpdateSet("T.nest=S.nest")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining("incompatible");
    }

    @Test
    public void testDuplicateSetTargetThrows() throws Exception {
        prepareNestedTarget(true);
        prepareSubFieldSource();

        // T.nest.a is targeted twice; parseCommaSeparatedKeyValues would otherwise silently keep
        // only the last assignment (regression for #8334 review comment on buildExplicitProject).
        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "T")
                                        .withMergeCondition("T.id=S.id")
                                        .withMatchedUpdateSet("T.nest.a=S.newa,T.nest.a=S.newa")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining("Duplicate");
    }

    @Test
    public void testDuplicateSetTargetWithMixedQualifierThrows() throws Exception {
        prepareNestedTarget(true);
        prepareSubFieldSource();

        // the same target written once qualified (T.nest.a) and once unqualified (nest.a) must
        // still be caught as a duplicate.
        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "T")
                                        .withMergeCondition("T.id=S.id")
                                        .withMatchedUpdateSet("T.nest.a=S.newa,nest.a=S.newa")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining("Duplicate");
    }

    @Test
    public void testAmbiguousTargetPathIsRejected() throws Exception {
        // the target table is named "payload" and the schema holds BOTH a struct column "payload"
        // and a top-level column "a", so the unqualified nested target "payload.a" is ambiguous:
        // it can be read as the nested payload.a, or as the qualified form of the top-level "a".
        sEnv.executeSql(
                buildDdl(
                        "payload",
                        Arrays.asList("id INT", "a INT", "payload ROW<a INT, b STRING>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                            }
                        }));
        insertInto("payload", "(1, 1, CAST(ROW(10, 'x') AS ROW<a INT, b STRING>))");
        prepareSubFieldSource();

        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "payload")
                                        .withMergeCondition("payload.id=S.id")
                                        .withMatchedUpdateSet("payload.a=S.newa")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining("mbiguous");

        // and neither interpretation must have been written. Note the alias: Flink SQL itself
        // reads a bare "payload.a" here as the table-qualified top-level column, which is exactly
        // the ambiguity the action now refuses to guess at.
        testBatchRead(
                "SELECT p.id, p.a, p.payload.a, p.payload.b FROM payload AS p",
                Collections.singletonList(changelogRow("+I", 1, 1, 10, "x")));
    }

    @Test
    public void testAmbiguousTargetPathAcceptsTheExplicitlyQualifiedForm() throws Exception {
        // the way out of the ambiguity above, as recommended by that error message: qualify the
        // nested target with the table name so the path is unmistakable.
        sEnv.executeSql(
                buildDdl(
                        "payload",
                        Arrays.asList("id INT", "a INT", "payload ROW<a INT, b STRING>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                            }
                        }));
        insertInto("payload", "(1, 1, CAST(ROW(10, 'x') AS ROW<a INT, b STRING>))");
        prepareSubFieldSource();

        builder(warehouse, database, "payload")
                .withMergeCondition("payload.id=S.id")
                .withMatchedUpdateSet("payload.payload.a=S.newa")
                .withSourceTable("S")
                .withSinkParallelism(2)
                .build()
                .run();

        // the NESTED payload.a moved; the top-level "a" is untouched
        testBatchRead(
                "SELECT p.id, p.a, p.payload.a, p.payload.b FROM payload AS p",
                Collections.singletonList(changelogRow("+I", 1, 1, 100, "x")));
        assertThat(deltaWriteCols("payload")).contains(Collections.singletonList("payload.a"));
    }

    @Test
    public void testUnqualifiedNestedTargetResolvesWhenOnlyOneReadingIsValid() throws Exception {
        // same table name / struct column name clash, but there is no top-level "a", so stripping
        // the qualifier yields nothing resolvable and "payload.a" can only be the nested sub-field.
        sEnv.executeSql(
                buildDdl(
                        "payload",
                        Arrays.asList("id INT", "payload ROW<a INT, b STRING>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                            }
                        }));
        insertInto("payload", "(1, CAST(ROW(10, 'x') AS ROW<a INT, b STRING>))");
        prepareSubFieldSource();

        builder(warehouse, database, "payload")
                .withMergeCondition("payload.id=S.id")
                .withMatchedUpdateSet("payload.a=S.newa")
                .withSourceTable("S")
                .withSinkParallelism(2)
                .build()
                .run();

        testBatchRead(
                "SELECT p.id, p.payload.a, p.payload.b FROM payload AS p",
                Collections.singletonList(changelogRow("+I", 1, 100, "x")));
        assertThat(deltaWriteCols("payload")).contains(Collections.singletonList("payload.a"));
    }

    @Test
    public void testUpdateMultipleSubFieldsWritesOnlyThoseLeaves() throws Exception {
        // a 3-field struct so that updating two sub-fields is a strict subset (stays
        // sub-field-level
        // rather than collapsing to a whole-column write)
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "nest ROW<a INT, b STRING, c INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                            }
                        }));
        insertInto(
                "T",
                "(1, CAST(ROW(10, 'x', 30) AS ROW<a INT, b STRING, c INT>))",
                "(2, CAST(ROW(20, 'y', 40) AS ROW<a INT, b STRING, c INT>))");

        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "newa INT", "newc INT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("S", "(1, 100, 300)");

        builder(warehouse, database, "T")
                .withMergeCondition("T.id=S.id")
                .withMatchedUpdateSet("T.nest.a=S.newa,T.nest.c=S.newc")
                .withSourceTable("S")
                .withSinkParallelism(2)
                .build()
                .run();

        // correctness: a and c updated for id=1, b preserved, other row untouched
        testBatchRead(
                "SELECT id, nest.a, nest.b, nest.c FROM T",
                Arrays.asList(
                        changelogRow("+I", 1, 100, "x", 300), changelogRow("+I", 2, 20, "y", 40)));

        // feature engaged: the incremental file contains exactly the two touched leaves, in schema
        // order (a before c)
        assertThat(deltaWriteCols("T")).contains(Arrays.asList("nest.a", "nest.c"));
    }

    @Test
    public void testUpdateDeeplyNestedSubFieldThrows() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "nest ROW<a INT, sub ROW<x INT, y INT>>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
                            }
                        }));
        insertInto("T", "(1, CAST(ROW(10, ROW(1, 2)) AS ROW<a INT, sub ROW<x INT, y INT>>))");

        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "newx INT"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("S", "(1, 100)");

        // deeper-than-one-level sub-field updates are rejected (the reader composes only one level)
        assertThatThrownBy(
                        () ->
                                builder(warehouse, database, "T")
                                        .withMergeCondition("T.id=S.id")
                                        .withMatchedUpdateSet("T.nest.sub.x=S.newx")
                                        .withSourceTable("S")
                                        .withSinkParallelism(2)
                                        .build()
                                        .run())
                .hasMessageContaining("one level");
    }

    /** The write columns of every data file in the latest snapshot of {@code tableName}. */
    private List<List<String>> deltaWriteCols(String tableName) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName);
        List<List<String>> result = new ArrayList<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFileMeta file = entry.file();
            result.add(file.writeCols());
        }
        return result;
    }

    private DataEvolutionMergeIntoActionBuilder builder(
            String warehouse, String database, String table) {
        return new DataEvolutionMergeIntoActionBuilder(warehouse, database, table);
    }

    private static class DataEvolutionMergeIntoActionBuilder {
        private final List<String> args;

        DataEvolutionMergeIntoActionBuilder(String warehouse, String database, String table) {
            this.args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "data_evolution_merge_into",
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    table));
        }

        DataEvolutionMergeIntoActionBuilder withSourceTable(String sourceTable) {
            args.add("--source_table");
            args.add(sourceTable);
            return this;
        }

        DataEvolutionMergeIntoActionBuilder withMergeCondition(String mergeCondition) {
            args.add("--on");
            args.add(mergeCondition);
            return this;
        }

        DataEvolutionMergeIntoActionBuilder withMatchedUpdateSet(String matchedUpdateSet) {
            args.add("--matched_update_set");
            args.add(matchedUpdateSet);
            return this;
        }

        DataEvolutionMergeIntoActionBuilder withSinkParallelism(int sinkParallelism) {
            args.add("--sink_parallelism");
            args.add(String.valueOf(sinkParallelism));
            return this;
        }

        DataEvolutionMergeIntoAction build() {
            return (DataEvolutionMergeIntoAction)
                    ActionFactory.createAction(args.toArray(new String[0]))
                            .orElseThrow(RuntimeException::new);
        }
    }
}
