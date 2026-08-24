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

package org.apache.paimon.spark.write;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import scala.Option;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PaimonBatchWrite}. */
public class PaimonBatchWriteTest {

    @Test
    public void testResolveBlobDescriptorSourceBeforeCreatingTaskWriters() throws Exception {
        Identifier sourceIdentifier = Identifier.fromString("db.source");
        Table sourceTable = mock(Table.class);
        when(sourceTable.fileIO()).thenReturn(LocalFileIO.create());

        Catalog catalog = mock(Catalog.class);
        when(catalog.getTable(sourceIdentifier)).thenReturn(sourceTable);
        CatalogLoader catalogLoader = mock(CatalogLoader.class);
        when(catalogLoader.load()).thenReturn(catalog);
        CatalogEnvironment catalogEnvironment = mock(CatalogEnvironment.class);
        when(catalogEnvironment.catalogLoader()).thenReturn(catalogLoader);

        FileStoreTable targetTable = mock(FileStoreTable.class);
        when(targetTable.catalogEnvironment()).thenReturn(catalogEnvironment);
        when(targetTable.coreOptions())
                .thenReturn(
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        "blob-descriptor.source-table", "db.source")));
        when(targetTable.newBatchWriteBuilder()).thenReturn(mock(BatchWriteBuilder.class));

        StructType schema = new StructType();
        PaimonBatchWrite batchWrite =
                new PaimonBatchWrite(
                        targetTable,
                        schema,
                        schema,
                        Option.empty(),
                        Option.empty(),
                        Option.empty());

        batchWrite.createBatchWriterFactory(null);

        verify(catalogLoader).load();
        verify(catalog).getTable(sourceIdentifier);
    }
}
