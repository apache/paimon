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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileIndexProcessor}. */
public class FileIndexProcessorTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testProcessReadsTheSchemasOfTheTableBranch() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path warehouse = new Path(tempDir.toString());
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.FILE_FORMAT.key(), "parquet");
        options.put(CoreOptions.FILE_INDEX + ".bloom-filter.columns", "v");
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

        Identifier identifier = Identifier.create("mydb", "t");
        FileStoreTable branchTable;
        try (FileSystemCatalog catalog = new FileSystemCatalog(fileIO, warehouse)) {
            catalog.createDatabase("mydb", false);
            catalog.createTable(
                    identifier,
                    new Schema(
                            rowType.getFields(),
                            Collections.emptyList(),
                            Collections.singletonList("k"),
                            options,
                            ""),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            table.branchManager().createBranch("b1");

            branchTable = table.switchToBranch("b1");
            branchTable
                    .schemaManager()
                    .commitChanges(SchemaChange.addColumn("branch_only", DataTypes.INT()));
            branchTable = table.switchToBranch("b1");
        }

        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = branchTable.newWrite(commitUser);
                TableCommitImpl commit = branchTable.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 10, 100));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        List<ManifestEntry> entries = branchTable.store().newScan().plan().files();
        assertThat(entries).isNotEmpty();
        ManifestEntry entry = entries.get(0);
        assertThat(entry.file().schemaId()).isEqualTo(1L);

        FileIndexProcessor processor = new FileIndexProcessor(branchTable);
        DataFileMeta processed = processor.process(entry.partition(), entry.bucket(), entry);
        assertThat(processed.extraFiles()).isNotEmpty();
    }
}
