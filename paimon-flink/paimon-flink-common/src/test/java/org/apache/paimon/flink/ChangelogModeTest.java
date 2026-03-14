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

package org.apache.paimon.flink;

import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.ChangelogProducer.INPUT;
import static org.apache.paimon.CoreOptions.ChangelogProducer.LOOKUP;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for changelog mode with flink source and sink. */
public class ChangelogModeTest {

    @TempDir java.nio.file.Path temp;

    private final ObjectIdentifier identifier = ObjectIdentifier.of("c", "d", "t");

    private Path path;

    @BeforeEach
    public void beforeEach() {
        path = new Path(temp.toUri().toString());
    }

    private void test(Options options, ChangelogMode expectSource, ChangelogMode expectSink)
            throws Exception {
        new SchemaManager(LocalFileIO.create(), path)
                .createTable(
                        new Schema(
                                RowType.of(new IntType(), new IntType()).getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("f0"),
                                options.toMap(),
                                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);

        DataTableSource source = new DataTableSource(identifier, table, true, null);
        assertThat(source.getChangelogMode()).isEqualTo(expectSource);

        FlinkTableSink sink = new FlinkTableSink(identifier, table, null);
        assertThat(sink.getChangelogMode(ChangelogMode.all())).isEqualTo(expectSink);
    }

    @Test
    public void testDefault() throws Exception {
        test(
                new Options(),
                ChangelogMode.upsert(),
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build());
    }

    @Test
    public void testInputChangelogProducer() throws Exception {
        Options options = new Options();
        options.set(CHANGELOG_PRODUCER, INPUT);
        test(options, ChangelogMode.all(), ChangelogMode.all());
    }

    @Test
    public void testLookupChangelogProducer() throws Exception {
        Options options = new Options();
        options.set(CHANGELOG_PRODUCER, LOOKUP);

        // Compatible to Flink 2.0 (keyOnlyDeletes)
        ChangelogMode.Builder upsertBuilder = ChangelogMode.newBuilder();
        for (RowKind kind : ChangelogMode.all().getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                upsertBuilder.addContainedKind(kind);
            }
        }
        test(options, ChangelogMode.all(), upsertBuilder.build());
    }
}
