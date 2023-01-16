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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.connector.source.TableStoreSource;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.kafka.KafkaLogStoreFactory;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

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

    private void test(Configuration options, ChangelogMode expectSource, ChangelogMode expectSink)
            throws Exception {
        test(options, expectSource, expectSink, null);
    }

    private void test(
            Configuration options,
            ChangelogMode expectSource,
            ChangelogMode expectSink,
            @Nullable LogStoreTableFactory logStoreTableFactory)
            throws Exception {
        new SchemaManager(path)
                .commitNewVersion(
                        new UpdateSchema(
                                RowType.of(new IntType(), new IntType()),
                                Collections.emptyList(),
                                Collections.singletonList("f0"),
                                options.toMap(),
                                ""));
        FileStoreTable table = FileStoreTableFactory.create(path);

        TableStoreSource source =
                new TableStoreSource(identifier, table, true, null, logStoreTableFactory);
        assertThat(source.getChangelogMode()).isEqualTo(expectSource);

        TableStoreSink sink = new TableStoreSink(identifier, table, null, null);
        assertThat(sink.getChangelogMode(ChangelogMode.all())).isEqualTo(expectSink);
    }

    @Test
    public void testDefault() throws Exception {
        test(new Configuration(), ChangelogMode.upsert(), ChangelogMode.upsert());
    }

    @Test
    public void testInputChangelogProducer() throws Exception {
        Configuration options = new Configuration();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        test(options, ChangelogMode.all(), ChangelogMode.all());
    }

    @Test
    public void testChangelogModeAll() throws Exception {
        Configuration options = new Configuration();
        options.set(CoreOptions.LOG_CHANGELOG_MODE, CoreOptions.LogChangelogMode.ALL);
        test(options, ChangelogMode.all(), ChangelogMode.all());
    }

    @Test
    public void testInputChangelogProducerWithLog() throws Exception {
        Configuration options = new Configuration();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        test(options, ChangelogMode.upsert(), ChangelogMode.all(), new KafkaLogStoreFactory());
    }
}
