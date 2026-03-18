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
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Test for changelog expire. */
public class ChangelogExpireTest extends IndexFileExpireTableTest {

    @BeforeEach
    public void beforeEachBase() throws Exception {
        CatalogContext context =
                CatalogContext.create(
                        new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString()));
        context.options().set(CACHE_ENABLED.key(), "false");
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
                        .option("changelog-producer", "input")
                        .option("changelog.num-retained.max", "40")
                        .option("snapshot.num-retained.max", "39")
                        .options(tableOptions().toMap())
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testChangelogExpire() throws Exception {
        ExpireConfig expireConfig =
                ExpireConfig.builder().changelogRetainMax(40).snapshotRetainMax(39).build();
        prepareExpireTable();
        ExpireChangelogImpl expire =
                (ExpireChangelogImpl) table.newExpireChangelog().config(expireConfig);

        ExpireSnapshotsImpl expireSnapshots =
                (ExpireSnapshotsImpl) table.newExpireSnapshots().config(expireConfig);
        expireSnapshots.expireUntil(1, 7);
        assertThatCode(() -> expire.expireUntil(1, 6)).doesNotThrowAnyException();
    }

    @Test
    public void testChangelogExpireWithUncleanedManifestLists() throws Exception {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        dynamicOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        dynamicOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "3");
        table = table.copy(dynamicOptions);

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();

        write(write, createRow(1, 0, 1, 10));
        commit.commit(1, write.prepareCommit(true, 1));

        // Save snapshot-1's manifest list files before they get cleaned
        SnapshotManager snapshotManager = table.snapshotManager();
        FileStorePathFactory pathFactory = table.store().pathFactory();
        FileIO fileIO = table.fileIO();
        Snapshot snapshot1 = snapshotManager.snapshot(1);
        Path manifestDir = pathFactory.manifestPath();
        Path baseSaved = new Path(manifestDir, snapshot1.baseManifestList() + ".saved");
        Path deltaSaved = new Path(manifestDir, snapshot1.deltaManifestList() + ".saved");
        fileIO.copyFile(
                pathFactory.toManifestListPath(snapshot1.baseManifestList()), baseSaved, false);
        fileIO.copyFile(
                pathFactory.toManifestListPath(snapshot1.deltaManifestList()), deltaSaved, false);

        write(write, createRow(1, 0, 2, 20));
        commit.commit(2, write.prepareCommit(true, 2));
        write(write, createRow(1, 0, 3, 30));
        commit.commit(3, write.prepareCommit(true, 3));

        // Copy back changelog-1's manifest list files, simulating SnapshotDeletion failure
        fileIO.copyFile(
                baseSaved, pathFactory.toManifestListPath(snapshot1.baseManifestList()), true);
        fileIO.copyFile(
                deltaSaved, pathFactory.toManifestListPath(snapshot1.deltaManifestList()), true);

        // expire changelog-1
        write(write, createRow(1, 0, 4, 40));
        commit.commit(4, write.prepareCommit(true, 4));

        write.close();
        commit.close();
        Snapshot snapshot4 = snapshotManager.snapshot(4);
        ManifestList manifestList = table.store().manifestListFactory().create();
        for (ManifestFileMeta manifest : manifestList.readDataManifests(snapshot4)) {
            assertThat(fileIO.exists(pathFactory.toManifestFilePath(manifest.fileName()))).isTrue();
        }
    }
}
