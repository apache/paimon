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

package org.apache.paimon.stats;

import org.apache.paimon.AbstractFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.CoreOptions.METADATA_STATS_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for table stats mode. */
public class StatsTableTest extends TableTestBase {

    @Test
    public void testPartitionStats() throws Exception {
        Identifier identifier = identifier("T");
        Options options = new Options();
        options.set(METADATA_STATS_MODE, "NONE");
        options.set(CoreOptions.BUCKET, 1);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .options(options.toMap())
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        write(
                table,
                GenericRow.of(1, 1, 1),
                GenericRow.of(1, 2, 1),
                GenericRow.of(1, 3, 1),
                GenericRow.of(2, 1, 1));

        FileStoreTable storeTable = (FileStoreTable) table;
        AbstractFileStore<?> store = (AbstractFileStore<?>) storeTable.store();
        String manifestListFile = storeTable.snapshotManager().latestSnapshot().deltaManifestList();

        ManifestList manifestList = store.manifestListFactory().create();
        ManifestFileMeta manifest = manifestList.read(manifestListFile).get(0);

        // should have partition stats
        BinaryTableStats partitionStats = manifest.partitionStats();
        assertThat(partitionStats.minValues().getInt(0)).isEqualTo(1);
        assertThat(partitionStats.maxValues().getInt(0)).isEqualTo(2);

        // should not have record stats because of NONE mode
        ManifestFile manifestFile = store.manifestFileFactory().create();
        DataFileMeta file =
                manifestFile.read(manifest.fileName(), manifest.fileSize()).get(0).file();
        BinaryTableStats recordStats = file.valueStats();
        assertThat(recordStats.minValues().isNullAt(0)).isTrue();
        assertThat(recordStats.minValues().isNullAt(1)).isTrue();
        assertThat(recordStats.minValues().isNullAt(2)).isTrue();
        assertThat(recordStats.maxValues().isNullAt(0)).isTrue();
        assertThat(recordStats.maxValues().isNullAt(1)).isTrue();
        assertThat(recordStats.maxValues().isNullAt(2)).isTrue();
    }
}
