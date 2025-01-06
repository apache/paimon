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
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(query())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 4, currentSecs + 60 * 60), GenericRow.of(1, 5, null));

        // null time field for record-level expire is not supported yet.
        assertThatThrownBy(() -> compact(1))
                .hasMessageContaining("Time field for record-level expire should not be null.");
    }
}
