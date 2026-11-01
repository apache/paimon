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
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PaimonBatchWrite}. */
public class PaimonBatchWriteTest {

    @Test
    public void testCommitFailureStillPropagatesAndClosesCommit() throws Exception {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(Collections.emptyMap()));
        BatchWriteBuilder builder = mock(BatchWriteBuilder.class);
        when(table.newBatchWriteBuilder()).thenReturn(builder);
        BatchTableCommit commit = mock(BatchTableCommit.class);
        when(builder.newCommit()).thenReturn(commit);
        IllegalStateException failure = new IllegalStateException("commit failed");
        doThrow(failure).when(commit).commit(anyList());

        StructType schema = new StructType();
        PaimonBatchWrite batchWrite =
                new PaimonBatchWrite(
                        table, schema, schema, Option.empty(), Option.empty(), Option.empty()) {
                    @Override
                    public void postDriverMetrics(CustomTaskMetric[] metrics) {
                        throw new AssertionError("Metrics must not be posted after commit failure");
                    }
                };

        WriterCommitMessage[] messages = new WriterCommitMessage[0];
        assertSame(
                failure,
                assertThrows(IllegalStateException.class, () -> batchWrite.commit(messages)));
        verify(commit).close();
        batchWrite.abort(messages);
        verify(commit, never()).abort(anyList());
    }

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
