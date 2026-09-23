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

package org.apache.paimon.flink.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link QueryExecutorOperator}. */
public class QueryExecutorOperatorTest extends TableTestBase {

    private static final int CONNECT_TIMEOUT_MS = 10_000;

    @Test
    public void testQueryServerIsShutDownOnClose() throws Exception {
        Identifier identifier = identifier("query_executor_table");
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        OneInputStreamOperatorTestHarness<InternalRow, InternalRow> harness =
                new OneInputStreamOperatorTestHarness<>(new QueryExecutorOperator(table));
        harness.setup();
        harness.initializeEmptyState();
        harness.open();

        InetSocketAddress address = serverAddress(harness);
        assertThatCode(() -> connect(address)).doesNotThrowAnyException();

        harness.close();

        assertThatThrownBy(() -> connect(address)).isInstanceOf(ConnectException.class);
    }

    /**
     * Reads the address the operator has published downstream. The output row is built by {@link
     * QueryExecutorOperator#outputType()}: parallelism, subtask index, host, port.
     */
    @SuppressWarnings("unchecked")
    private static InetSocketAddress serverAddress(
            OneInputStreamOperatorTestHarness<InternalRow, InternalRow> harness) {
        List<InternalRow> rows = new ArrayList<>();
        for (Object record : harness.getOutput()) {
            rows.add(((StreamRecord<InternalRow>) record).getValue());
        }
        assertThat(rows).hasSize(1);
        InternalRow row = rows.get(0);
        return new InetSocketAddress(row.getString(2).toString(), row.getInt(3));
    }

    private static void connect(InetSocketAddress address) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(address, CONNECT_TIMEOUT_MS);
        }
    }
}
