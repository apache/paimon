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

package org.apache.paimon.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for table access context. */
public class CatalogAccessContextTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testTableAccessContextPassedToQueryAuth() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempDir.toUri().toString());
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        FileIO fileIO = new ResolvingFileIO();
        fileIO.configure(catalogContext);

        TrackingCatalog catalog =
                new TrackingCatalog(fileIO, new Path(tempDir.toUri().toString()), catalogContext);
        Identifier tableIdentifier = Identifier.create("default", "T");
        Identifier viewIdentifier = Identifier.create("default", "ViewT");
        TableAccessContext accessContext = TableAccessContext.blobViewFallback(viewIdentifier);

        catalog.createDatabase("default", true);
        catalog.createTable(
                tableIdentifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.QUERY_AUTH_ENABLED.key(), "true")
                        .build(),
                false);

        Table table = catalog.getTable(tableIdentifier, accessContext);
        table.newReadBuilder().newScan().plan();

        assertThat(((FileStoreTable) table).catalogEnvironment().tableAccessContext())
                .isSameAs(accessContext);
        assertThat(catalog.latestAuthContext.get()).isSameAs(accessContext);
        assertThat(catalog.latestAuthIdentifier.get()).isEqualTo(tableIdentifier);
    }

    private static class TrackingCatalog extends FileSystemCatalog {

        private final AtomicReference<TableAccessContext> latestAuthContext =
                new AtomicReference<>();
        private final AtomicReference<Identifier> latestAuthIdentifier = new AtomicReference<>();

        private TrackingCatalog(FileIO fileIO, Path warehouse, CatalogContext context) {
            super(fileIO, warehouse, context);
        }

        @Override
        public CatalogLoader catalogLoader() {
            return () -> this;
        }

        @Override
        public TableQueryAuthResult authTableQuery(
                Identifier identifier,
                @Nullable List<String> select,
                TableAccessContext accessContext) {
            latestAuthIdentifier.set(identifier);
            latestAuthContext.set(accessContext);
            return null;
        }
    }
}
