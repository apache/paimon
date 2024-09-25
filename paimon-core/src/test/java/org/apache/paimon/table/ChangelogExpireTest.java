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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;

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
        Assertions.assertThatCode(() -> expire.expireUntil(1, 6)).doesNotThrowAnyException();
    }
}
