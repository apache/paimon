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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoreOptions#DELETION_VECTORS_MERGE_ON_READ}. */
public class DeletionVectorsMergeOnReadTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private final Identifier identifier = Identifier.create("my_db", "T");

    @BeforeEach
    public void before() throws Exception {
        Options options = new Options();
        options.set("warehouse", new Path(tempDir.toString() + "/warehouse").toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("my_db", true);
    }

    private FileStoreTable createTable(boolean mergeOnRead) throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("k", DataTypes.INT());
        schemaBuilder.column("v", DataTypes.INT());
        schemaBuilder.primaryKey("k");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("deletion-vectors.enabled", "true");
        schemaBuilder.option("write-only", "true");
        if (mergeOnRead) {
            schemaBuilder.option("deletion-vectors.merge-on-read", "true");
        }
        catalog.createTable(identifier, schemaBuilder.build(), true);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private void writeCommit(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        for (GenericRow row : rows) {
            write.write(row);
        }
        writeBuilder.newCommit().commit(write.prepareCommit());
        write.close();
    }

    private List<GenericRow> query(FileStoreTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        List<GenericRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(plan)
                .forEachRemaining(row -> result.add(GenericRow.of(row.getInt(0), row.getInt(1))));
        return result;
    }

    @Test
    public void testDefaultSkipsLevel0() throws Exception {
        FileStoreTable table = createTable(false);

        writeCommit(table, GenericRow.of(1, 10), GenericRow.of(2, 20));
        writeCommit(table, GenericRow.of(1, 11), GenericRow.of(3, 30));

        // write-only mode, no compaction, all files at level 0
        // default DV mode skips level 0 — only the first commit's compacted data is visible
        // since no compaction has run, no data should be visible from level > 0
        List<GenericRow> result = query(table);
        assertThat(result).isEmpty();
    }

    @Test
    public void testMergeOnReadReadsLevel0() throws Exception {
        FileStoreTable table = createTable(true);

        writeCommit(table, GenericRow.of(1, 10), GenericRow.of(2, 20));
        writeCommit(table, GenericRow.of(1, 11), GenericRow.of(3, 30));

        // merge-on-read enabled, level 0 data is visible via MOR
        List<GenericRow> result = query(table);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 11), GenericRow.of(2, 20), GenericRow.of(3, 30));
    }

    @Test
    public void testMergeOnReadWithQueryHint() throws Exception {
        FileStoreTable table = createTable(false);

        writeCommit(table, GenericRow.of(1, 10), GenericRow.of(2, 20));
        writeCommit(table, GenericRow.of(1, 11), GenericRow.of(3, 30));

        // default: no data visible (L0 skipped)
        assertThat(query(table)).isEmpty();

        // override with dynamic option to enable merge-on-read
        table =
                table.copy(
                        java.util.Collections.singletonMap(
                                "deletion-vectors.merge-on-read", "true"));
        List<GenericRow> result = query(table);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 11), GenericRow.of(2, 20), GenericRow.of(3, 30));
    }
}
