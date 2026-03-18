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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LookupTable} with variant type. */
public class LookupVariantTableTest extends TableTestBase {

    @TempDir java.nio.file.Path tempDir;
    private IOManager ioManager;

    @BeforeEach
    public void before() throws IOException {
        this.ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testRemoteFile() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.LOOKUP_REMOTE_FILE_ENABLED, true);
        options.set(CoreOptions.MERGE_ENGINE, PARTIAL_UPDATE);
        Identifier identifier = new Identifier("default", "t");
        Schema schema =
                new Schema(
                        RowType.of(new IntType(), new IntType(), new VariantType()).getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        options.toMap(),
                        null);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        // first write
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1, null, GenericVariant.fromJson("{\"age\":27,\"city\":\"Beijing\"}")));
            commit.commit(write.prepareCommit());
        }

        // second write to trigger ser and deser
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 1, null));
            commit.commit(write.prepareCommit());
        }

        // read to assert
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            result.add(
                                    row.getInt(0)
                                            + "-"
                                            + row.getInt(1)
                                            + "-"
                                            + row.getVariant(2).toJson());
                        });
        assertThat(result).containsOnly("1-1-{\"age\":27,\"city\":\"Beijing\"}");
    }
}
