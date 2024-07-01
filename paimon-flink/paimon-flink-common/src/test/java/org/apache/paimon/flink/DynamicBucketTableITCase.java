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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for batch file store. */
public class DynamicBucketTableITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "pt INT, "
                        + "pk INT, "
                        + "v INT, "
                        + "PRIMARY KEY (pt, pk) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'bucket'='-1', "
                        + " 'dynamic-bucket.target-row-num'='3' "
                        + ")");
    }

    @Test
    public void testWriteReadDelete() {
        sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 1, 1),
                        Row.of(1, 2, 2),
                        Row.of(1, 3, 3),
                        Row.of(1, 4, 4),
                        Row.of(1, 5, 5));
        sql("INSERT INTO T VALUES (1, 3, 33), (1, 1, 11)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 1, 11),
                        Row.of(1, 2, 2),
                        Row.of(1, 3, 33),
                        Row.of(1, 4, 4),
                        Row.of(1, 5, 5));
        sql("DELETE FROM T WHERE pt = 1 AND pk = 3");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 1, 11), Row.of(1, 2, 2), Row.of(1, 4, 4), Row.of(1, 5, 5));

        assertThat(sql("SELECT DISTINCT bucket FROM T$files"))
                .containsExactlyInAnyOrder(Row.of(0), Row.of(1));
    }

    @Test
    public void testWriteWithAssignerParallelism() {
        sql(
                "INSERT INTO T /*+ OPTIONS('dynamic-bucket.initial-buckets'='3') */ "
                        + "VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)");
        // initial-buckets is 3, but parallelism is 2, will use 2
        assertThat(sql("SELECT DISTINCT bucket FROM T$files"))
                .containsExactlyInAnyOrder(Row.of(0), Row.of(1));
    }

    @Test
    public void testWriteWithAssignerParallelism1() {
        sql(
                "INSERT INTO T /*+ OPTIONS('dynamic-bucket.initial-buckets'='1') */ "
                        + "VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)");
        assertThat(sql("SELECT DISTINCT bucket FROM T$files"))
                .containsExactlyInAnyOrder(Row.of(0), Row.of(1));
    }

    @Test
    public void testOverwrite() throws Exception {
        sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 1, 1),
                        Row.of(1, 2, 2),
                        Row.of(1, 3, 3),
                        Row.of(1, 4, 4),
                        Row.of(1, 5, 5));

        // overwrite the whole table, we should update the index file by this sql
        sql("INSERT OVERWRITE T SELECT * FROM T LIMIT 4");

        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(path)));

        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("default", "T"));
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        List<BinaryRow> partitions = table.newScan().listPartitions();
        List<IndexManifestEntry> entries = new ArrayList<>();
        partitions.forEach(p -> entries.addAll(indexFileHandler.scanEntries(HASH_INDEX, p)));

        Long records =
                entries.stream().map(entry -> entry.indexFile().rowCount()).reduce(Long::sum).get();
        Assertions.assertThat(records).isEqualTo(4);

        catalog.close();
    }
}
