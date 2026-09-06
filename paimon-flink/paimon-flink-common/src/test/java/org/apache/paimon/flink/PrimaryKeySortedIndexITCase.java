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

package org.apache.paimon.flink;

import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end Flink SQL tests for source-backed primary-key BTree and Bitmap indexes. */
public class PrimaryKeySortedIndexITCase extends CatalogITCaseBase {

    @Test
    public void testMixedSortedIndexesWithDeletionVectors() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + "id INT PRIMARY KEY NOT ENFORCED, "
                        + "score INT, category STRING, note STRING"
                        + ") WITH ("
                        + "'bucket' = '1', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'pk-btree.index.columns' = 'score', "
                        + "'pk-bitmap.index.columns' = 'category', "
                        + "'fields.score.pk-btree.index.options' = "
                        + "'{\"block-size\":\"4 kb\"}', "
                        + "'fields.category.pk-bitmap.index.options' = "
                        + "'{\"dictionary-block-size\":\"8 kb\"}'"
                        + ")");
        sql("INSERT INTO T VALUES (1, 10, 'red', 'keep'), (2, 20, 'blue', 'drop')");
        sql("INSERT INTO T VALUES (3, 30, 'red', 'keep'), (4, 40, 'green', 'keep')");
        sql("CALL sys.compact(`table` => 'default.T')");

        FileStoreTable table = paimonTable("T");
        List<IndexFileMeta> sourceIndexes =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(
                                file ->
                                        file.globalIndexMeta() != null
                                                && file.globalIndexMeta().sourceMeta() != null)
                        .collect(Collectors.toList());
        assertThat(sourceIndexes).extracting(IndexFileMeta::indexType).contains("btree", "bitmap");

        sql("UPDATE T SET score = 25, category = 'red', note = 'keep' WHERE id = 2");
        sql("DELETE FROM T WHERE id = 3");
        sql("INSERT INTO T VALUES (5, 35, 'yellow', 'keep')");

        String indexedQuery = "SELECT * FROM T WHERE score >= 20 AND score < 40";
        assertThat(tEnv.explainSql(indexedQuery, ExplainFormat.TEXT))
                .contains("TableSourceScan", "filter=[", ">=(score, 20)", "<(score, 40)");
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        List<Split> splits =
                table.newReadBuilder()
                        .withFilter(
                                PredicateBuilder.and(
                                        predicateBuilder.greaterOrEqual(1, 20),
                                        predicateBuilder.lessThan(1, 40)))
                        .newScan()
                        .plan()
                        .splits();
        assertThat(splits).anyMatch(IndexedSplit.class::isInstance);
        assertThat(splits).anyMatch(DataSplit.class::isInstance);

        assertThat(sql("SELECT * FROM T WHERE score = 10"))
                .containsExactly(Row.of(1, 10, "red", "keep"));
        assertThat(sql(indexedQuery))
                .containsExactlyInAnyOrder(
                        Row.of(2, 25, "red", "keep"), Row.of(5, 35, "yellow", "keep"));
        assertThat(sql("SELECT * FROM T WHERE score >= 10 AND category = 'red'"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 10, "red", "keep"), Row.of(2, 25, "red", "keep"));
        assertThat(sql("SELECT * FROM T WHERE score = 40 OR category = 'red'"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 10, "red", "keep"),
                        Row.of(2, 25, "red", "keep"),
                        Row.of(4, 40, "green", "keep"));
        assertThat(sql("SELECT * FROM T WHERE note = 'keep'"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 10, "red", "keep"),
                        Row.of(2, 25, "red", "keep"),
                        Row.of(4, 40, "green", "keep"),
                        Row.of(5, 35, "yellow", "keep"));
    }
}
