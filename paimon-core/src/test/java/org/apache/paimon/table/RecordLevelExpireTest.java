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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RecordLevelExpireTest extends PrimaryKeyTableTestBase {

    @Override
    @BeforeEach
    public void beforeEachBase() throws Exception {
        CatalogContext context =
                CatalogContext.create(
                        new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString()));
        Catalog catalog = CatalogFactory.createCatalog(context);
        Identifier identifier = new Identifier("default", "T");
        catalog.createDatabase(identifier.getDatabaseName(), true);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .options(tableOptions().toMap())
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
        commitUser = UUID.randomUUID().toString();
    }

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.RECORD_LEVEL_EXPIRE_TIME, Duration.ofSeconds(1));
        options.set(CoreOptions.RECORD_LEVEL_TIME_FIELD, "col1");
        return options;
    }

    @Test
    public void test() throws Exception {
        writeCommit(GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 2));

        // can be queried
        assertThat(query())
                .containsExactlyInAnyOrder(GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 2));

        int currentSecs = (int) (System.currentTimeMillis() / 1000);
        writeCommit(GenericRow.of(1, 3, currentSecs));
        writeCommit(GenericRow.of(1, 4, currentSecs + 60 * 60));
        Thread.sleep(2000);

        // no compaction, can be queried
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 1),
                        GenericRow.of(1, 2, 2),
                        GenericRow.of(1, 3, currentSecs),
                        GenericRow.of(1, 4, currentSecs + 60 * 60));

        // compact, expired
        compact(1);
        assertThat(query()).containsExactlyInAnyOrder(GenericRow.of(1, 4, currentSecs + 60 * 60));
        assertThat(query(new int[] {2}))
                .containsExactlyInAnyOrder(GenericRow.of(currentSecs + 60 * 60));

        writeCommit(GenericRow.of(1, 5, null));
        compact(1);
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 4, currentSecs + 60 * 60), GenericRow.of(1, 5, null));

        writeCommit(GenericRow.of(1, 5, currentSecs + 60 * 60));
        // compact, merged
        compact(1);
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 4, currentSecs + 60 * 60),
                        GenericRow.of(1, 5, currentSecs + 60 * 60));
    }

    @Test
    public void testIsExpireFile() throws Exception {
        CoreOptions coreOptions = table.coreOptions();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        table = table.copy(dynamicOptions);

        // common case
        int currentSecs = (int) (System.currentTimeMillis() / 1000);
        writeCommit(
                GenericRow.of(1, 1, currentSecs + 60 * 60),
                GenericRow.of(1, 2, currentSecs + 30 * 60),
                GenericRow.of(1, 3, currentSecs - 60 * 60),
                GenericRow.of(1, 4, currentSecs - 30 * 60));

        writeCommit(
                GenericRow.of(1, 5, currentSecs + 60 * 60),
                GenericRow.of(1, 6, currentSecs + 30 * 60),
                GenericRow.of(1, 7, currentSecs + 20 * 60),
                GenericRow.of(1, 8, currentSecs + 10 * 60));

        RecordLevelExpire recordLevelExpire =
                RecordLevelExpire.create(coreOptions, table.schema(), table.schemaManager());
        List<DataSplit> splits1 = table.newSnapshotReader().read().dataSplits();
        assertThat(splits1.size()).isEqualTo(1);
        List<DataFileMeta> files1 = splits1.get(0).dataFiles();
        assertThat(files1.size()).isEqualTo(2);
        assertThat(recordLevelExpire.isExpireFile(files1.get(0))).isTrue();
        assertThat(recordLevelExpire.isExpireFile(files1.get(1))).isFalse();

        // schema evolution
        table.schemaManager()
                .commitChanges(
                        Collections.singletonList(
                                SchemaChange.addColumn(
                                        "col0",
                                        DataTypes.INT(),
                                        null,
                                        SchemaChange.Move.after("col0", "pk"))));
        refreshTable();

        recordLevelExpire =
                RecordLevelExpire.create(coreOptions, table.schema(), table.schemaManager());
        List<DataSplit> splits2 = table.newSnapshotReader().read().dataSplits();
        List<DataFileMeta> files2 = splits2.get(0).dataFiles();
        assertThat(recordLevelExpire.isExpireFile(files2.get(0))).isTrue();
        assertThat(recordLevelExpire.isExpireFile(files2.get(1))).isFalse();

        // metadata.stats-dense-store = true
        dynamicOptions.put(CoreOptions.METADATA_STATS_DENSE_STORE.key(), "true");
        dynamicOptions.put(CoreOptions.METADATA_STATS_MODE.key(), "none");
        dynamicOptions.put("fields.col1.stats-mode", "full");
        table = table.copy(dynamicOptions);

        writeCommit(
                GenericRow.of(1, 9, 9, currentSecs + 60 * 60),
                GenericRow.of(1, 10, 10, currentSecs + 30 * 60));
        writeCommit(
                GenericRow.of(1, 11, 11, currentSecs + 60 * 60),
                GenericRow.of(1, 12, 12, currentSecs - 30 * 60));

        recordLevelExpire =
                RecordLevelExpire.create(coreOptions, table.schema(), table.schemaManager());
        List<DataSplit> splits3 = table.newSnapshotReader().read().dataSplits();
        List<DataFileMeta> files3 = splits3.get(0).dataFiles();
        assertThat(recordLevelExpire.isExpireFile(files3.get(0))).isTrue();
        assertThat(recordLevelExpire.isExpireFile(files3.get(1))).isFalse();
        assertThat(recordLevelExpire.isExpireFile(files3.get(2))).isFalse();
        assertThat(recordLevelExpire.isExpireFile(files3.get(3))).isTrue();

        // schema evolution again, change the valueCols
        table.schemaManager()
                .commitChanges(
                        Collections.singletonList(
                                SchemaChange.addColumn(
                                        "col2",
                                        DataTypes.INT(),
                                        null,
                                        SchemaChange.Move.after("col2", "pk"))));
        refreshTable();

        // new files has no stats for record-level.time-field
        dynamicOptions.put("fields.col1.stats-mode", "none");
        dynamicOptions.put("fields.col2.stats-mode", "full");
        table = table.copy(dynamicOptions);

        writeCommit(
                GenericRow.of(1, 13, 13, 13, currentSecs + 60 * 60),
                GenericRow.of(1, 14, 14, 14, currentSecs + 30 * 60));
        writeCommit(
                GenericRow.of(1, 15, 15, 15, currentSecs + 60 * 60),
                GenericRow.of(1, 16, 16, 16, currentSecs - 30 * 60));

        recordLevelExpire =
                RecordLevelExpire.create(coreOptions, table.schema(), table.schemaManager());
        List<DataSplit> splits4 = table.newSnapshotReader().read().dataSplits();
        List<DataFileMeta> files4 = splits4.get(0).dataFiles();
        // old files with record-level.time-field stats
        assertThat(recordLevelExpire.isExpireFile(files4.get(0))).isTrue();
        assertThat(recordLevelExpire.isExpireFile(files4.get(1))).isFalse();
        assertThat(recordLevelExpire.isExpireFile(files4.get(2))).isFalse();
        assertThat(recordLevelExpire.isExpireFile(files4.get(3))).isTrue();
        // new files without record-level.time-field stats, cannot expire records
        assertThat(recordLevelExpire.isExpireFile(files4.get(4))).isFalse();
        assertThat(recordLevelExpire.isExpireFile(files4.get(5))).isFalse();
    }

    @Test
    public void testTotallyExpire() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(CoreOptions.TARGET_FILE_SIZE.key(), "1500 B");
        table = table.copy(map);

        int currentSecs = (int) (System.currentTimeMillis() / 1000);
        // if seconds is too short, this test might file
        int seconds = 5;

        // large file A. It has no delete records and expired records, will be upgraded to maxLevel
        // without rewriting when full compaction
        writeCommit(
                GenericRow.of(1, 1, currentSecs + 60 * 60),
                GenericRow.of(1, 2, currentSecs + seconds));
        compact(1);
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, currentSecs + 60 * 60),
                        GenericRow.of(1, 2, currentSecs + seconds));

        // large file B. It has no delete records but has expired records
        writeCommit(
                GenericRow.of(1, 3, currentSecs + 60 * 60),
                GenericRow.of(1, 4, currentSecs - 60 * 60));
        // no full compaction, expired records can be queried
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, currentSecs + 60 * 60),
                        GenericRow.of(1, 2, currentSecs + seconds),
                        GenericRow.of(1, 3, currentSecs + 60 * 60),
                        GenericRow.of(1, 4, currentSecs - 60 * 60));
        compact(1);
        List<DataSplit> splits1 = table.newSnapshotReader().read().dataSplits();
        assertThat(splits1.size()).isEqualTo(1);
        assertThat(splits1.get(0).dataFiles().size()).isEqualTo(2);
        // full compaction, expired records will be removed
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, currentSecs + 60 * 60),
                        GenericRow.of(1, 2, currentSecs + seconds),
                        GenericRow.of(1, 3, currentSecs + 60 * 60));

        // ensure (1, 2, currentSecs + seconds) out of date
        Thread.sleep(seconds * 1000 + 2000);
        compact(1);
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, currentSecs + 60 * 60),
                        GenericRow.of(1, 3, currentSecs + 60 * 60));
    }

    private void refreshTable() throws Catalog.TableNotExistException {
        CatalogContext context =
                CatalogContext.create(
                        new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString()));
        Catalog catalog = CatalogFactory.createCatalog(context);
        Identifier identifier = new Identifier("default", "T");

        table = (FileStoreTable) catalog.getTable(identifier);
    }
}
