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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse));
    }

    @Test
    @Override
    public void testListDatabasesWhenNoDatabases() {
        // List databases returns an empty list when there are no databases
        List<String> databases = catalog.listDatabases();
        assertThat(databases).isEmpty();
    }

    @Test
    public void testListPartitions() throws Exception {
        catalog.createDatabase("test_db", false);
        // Create table creates a new table when it does not exist
        Identifier identifier = Identifier.create("test_db", "new_table");
        catalog.createTable(identifier, PARTITION_SCHEMA, false);
        Table table = catalog.getTable(identifier);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        BatchTableWrite write = builder.newWrite();

        for (int i = 0; i < 1000; i++) {
            InternalRow row =
                    GenericRow.of(
                            1,
                            BinaryString.fromString(String.valueOf(i)),
                            BinaryString.fromString(String.valueOf(i)),
                            BinaryString.fromString(String.valueOf(i)));
            write.write(row);
        }
        List<CommitMessage> result = write.prepareCommit();
        builder.newCommit().commit(result);

        AtomicInteger ai = new AtomicInteger(0);
        catalog.listPartitions(identifier).stream()
                .map(Partition::partitionSpec)
                .forEach(
                        spec -> {
                            String.valueOf(ai.get()).equals(spec.get("pk1"));
                            String.valueOf(ai.getAndIncrement()).equals(spec.get("pk2"));
                        });
    }
}
