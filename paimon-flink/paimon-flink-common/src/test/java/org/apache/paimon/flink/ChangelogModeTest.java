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

import org.apache.paimon.CoreOptions;
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

        DataTableSource source = new DataTableSource(identifier, table, true, null, null);
        assertThat(source.getChangelogMode()).isEqualTo(expectSource);

        FlinkTableSink sink = new FlinkTableSink(identifier, table, null, null);
        assertThat(sink.getChangelogMode(ChangelogMode.all())).isEqualTo(expectSink);
    }

    @Test
    public void testDefault() throws Exception {
        test(new Options(), ChangelogMode.upsert(), ChangelogMode.upsert());
    }

    @Test
    public void testInputChangelogProducer() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        test(options, ChangelogMode.all(), ChangelogMode.all());
    }

    @Test
    public void testChangelogModeAll() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.LOG_CHANGELOG_MODE, CoreOptions.LogChangelogMode.ALL);
        test(options, ChangelogMode.all(), ChangelogMode.all());
    }

    @Test
    public void testFirstRowWithSeq() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.FIRST_ROW);
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        options.set(CoreOptions.SEQUENCE_FIELD, "f0");
        test(
                options,
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.INSERT)
                        .build(),
                ChangelogMode.upsert());
    }

    @Test
    public void testFirstRowWithoutSeq() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.FIRST_ROW);
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        test(
                options,
                ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build(),
                ChangelogMode.upsert());
    }
}
