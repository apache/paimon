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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for btree global index. */
public class BTreeGlobalIndexITCase extends CatalogITCaseBase {

    @Test
    public void testBTreeIndex() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        String values =
                IntStream.range(0, 1_000)
                        .mapToObj(i -> String.format("(%s, %s)", i, "'name_" + i + "'"))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T VALUES " + values);
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', index_column => 'id', index_type => 'btree')");

        // assert has btree index
        FileStoreTable table = paimonTable("T");
        List<IndexFileMeta> btreeEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> "btree".equals(f.indexType()))
                        .collect(Collectors.toList());

        long totalRowCount = btreeEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(btreeEntries).hasSize(1);
        assertThat(totalRowCount).isEqualTo(1000L);

        // assert select with filter
        assertThat(sql("SELECT * FROM T WHERE id = 100")).containsOnly(Row.of(100, "name_100"));
    }
}
