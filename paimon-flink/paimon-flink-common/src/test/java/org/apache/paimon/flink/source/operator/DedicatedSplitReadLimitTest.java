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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link ReadOperator} limit on the dedicated split read path. */
public class DedicatedSplitReadLimitTest {

    private static final int LIMIT = 10;

    @TempDir Path tempDir;

    private Table table;

    @BeforeEach
    public void before()
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableNotExistException, Catalog.DatabaseAlreadyExistException {
        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .primaryKey("a")
                        .option("bucket", "1")
                        .build();
        Identifier identifier = Identifier.create("default", "t");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        this.table = catalog.getTable(identifier);
    }

    @Test
    public void testReadOperatorStopsAfterLimit() throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        for (int i = 0; i < 100; i++) {
            write.write(GenericRow.of(i, i, i));
        }
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();

        ReadBatchCountingRead countingRead =
                new ReadBatchCountingRead(table.newReadBuilder().newRead());
        ReadOperator readOperator = new ReadOperator(() -> countingRead, null, (long) LIMIT);

        OneInputStreamOperatorTestHarness<Split, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(readOperator);
        harness.setup(
                InternalSerializers.create(
                        RowType.of(new IntType(), new IntType(), new IntType())));
        harness.open();
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            harness.processElement(new StreamRecord<>(split));
        }

        assertThat(harness.getOutput()).hasSize(LIMIT);
        assertThat(countingRead.readBatchInvocations()).isEqualTo(1);
    }

    private static class ReadBatchCountingRead implements TableRead {

        private final TableRead delegate;
        private final AtomicInteger readBatchInvocations = new AtomicInteger();

        private ReadBatchCountingRead(TableRead delegate) {
            this.delegate = delegate;
        }

        int readBatchInvocations() {
            return readBatchInvocations.get();
        }

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            delegate.withMetricRegistry(registry);
            return this;
        }

        @Override
        public TableRead executeFilter() {
            delegate.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            delegate.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            RecordReader<InternalRow> reader = delegate.createReader(split);
            return new RecordReader<InternalRow>() {
                @Override
                public RecordIterator<InternalRow> readBatch() throws IOException {
                    readBatchInvocations.incrementAndGet();
                    return reader.readBatch();
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };
        }
    }
}
