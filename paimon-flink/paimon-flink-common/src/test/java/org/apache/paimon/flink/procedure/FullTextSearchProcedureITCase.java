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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexFile;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link FullTextSearchProcedure}. */
public class FullTextSearchProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testDataEvolutionSourceMetaForGenericFullTextIndex() throws Exception {
        sql(
                "CREATE TABLE T_DE (id INT, content STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        sql("INSERT INTO T_DE VALUES (1, 'apache paimon'), (2, 'lake format')");

        FileStoreTable table = paimonTable("T_DE");
        long scanSnapshotId = table.snapshotManager().latestSnapshot().id();
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_DE', "
                        + "index_column => 'content', "
                        + "index_type => 'full-text')");

        List<IndexManifestEntry> entries = table.store().newIndexFileHandler().scan("full-text");
        assertThat(entries).isNotEmpty();
        assertThat(entries)
                .allSatisfy(
                        entry ->
                                assertThat(
                                                DataEvolutionIndexSourceMeta.fromIndexFile(
                                                                entry.indexFile())
                                                        .scanSnapshotId())
                                        .isEqualTo(scanSnapshotId));
    }

    @Test
    public void testPrimaryKeyFullTextSearchWithScoreProjection() throws Exception {
        createPrimaryKeyFullTextTable("T");
        sql("INSERT INTO T VALUES (0, 'lake format')");
        sql(
                "INSERT INTO T VALUES "
                        + "(1, 'paimon full text search'), "
                        + "(2, 'apache paimon storage')");

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.T')");

        assertThat(
                        paimonTable("T").store().newIndexFileHandler().scanEntries().stream()
                                .filter(
                                        entry ->
                                                PkFullTextIndexFile.INDEX_TYPE.equals(
                                                        entry.indexFile().indexType())))
                .isNotEmpty();

        List<String> rows =
                sql(
                                "CALL sys.full_text_search("
                                        + "`table` => 'default.T', "
                                        + "`column` => 'content', "
                                        + "query => '{\"match\":{\"column\":\"content\",\"terms\":\"paimon storage\"}}', "
                                        + "top_k => 10, "
                                        + "projection => 'id,__paimon_search_score')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0)).contains("\"id\":\"2\"");
        assertThat(rows.get(1)).contains("\"id\":\"1\"");
        assertThat(rows).allMatch(row -> row.contains("\"__paimon_search_score\":\""));
    }

    @Test
    public void testValidatesQueryLimitAndProjection() {
        createPrimaryKeyFullTextTable("VALIDATION_T");

        assertThatThrownBy(
                        () ->
                                search(
                                        "VALIDATION_T",
                                        "{\"match\":{\"column\":\"content\",\"terms\":\"paimon\"}}",
                                        0,
                                        "id"))
                .hasStackTraceContaining("top_k must be positive");
        assertThatThrownBy(() -> search("VALIDATION_T", "   ", 1, "id"))
                .hasStackTraceContaining("query must not be blank");
        assertThatThrownBy(
                        () ->
                                search(
                                        "VALIDATION_T",
                                        "{\"match\":{\"column\":\"content\",\"terms\":\"paimon\"}}",
                                        1,
                                        "missing"))
                .hasStackTraceContaining("Unknown projection column");
        assertThatThrownBy(
                        () ->
                                search(
                                        "VALIDATION_T",
                                        "{\"match\":{\"column\":\"content\",\"terms\":\"paimon\"}}",
                                        10_001,
                                        "id"))
                .hasStackTraceContaining("top_k must not exceed 10000");
    }

    @Test
    public void testRejectsQueryAuthorizationOverride() {
        createPrimaryKeyFullTextTable("AUTH_T");

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.full_text_search("
                                                + "`table` => 'default.AUTH_T', "
                                                + "`column` => 'content', "
                                                + "query => '{\"match\":{\"column\":\"content\",\"terms\":\"paimon\"}}', "
                                                + "top_k => 1, "
                                                + "projection => 'id', "
                                                + "options => 'query-auth.enabled=false')"))
                .hasStackTraceContaining("Option 'query-auth.enabled' is not allowed");
    }

    private List<Row> search(String table, String query, int topK, String projection) {
        return sql(
                "CALL sys.full_text_search("
                        + "`table` => 'default.%s', "
                        + "`column` => 'content', "
                        + "query => '%s', "
                        + "top_k => %d, "
                        + "projection => '%s')",
                table, query, topK, projection);
    }

    private void createPrimaryKeyFullTextTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "id INT, "
                        + "content STRING, "
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "'bucket' = '1', "
                        + "'file.format' = 'json', "
                        + "'file.compression' = 'none', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'pk-full-text.index.columns' = 'content'"
                        + ")",
                tableName);
    }
}
