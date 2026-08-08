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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests durable unavailable-generation acknowledgements from the address register. */
class GlobalIndexQueryAddressRegisterTest extends TableTestBase {

    @Test
    void testNotReadyDescriptorAcknowledgesEventTargetFence() throws Exception {
        FileStoreTable table = createTable();
        QuerySpec spec =
                GlobalIndexQueryServiceUtils.querySpec(
                        table, "url", Collections.singletonList("descriptor"));
        GlobalIndexQueryAddressRegister register = new GlobalIndexQueryAddressRegister(table, spec);
        WriterInitContext context = mock(WriterInitContext.class);
        when(context.getAttemptNumber()).thenReturn(0);

        try (SinkWriter<InternalRow> writer = register.createWriter(context)) {
            writer.write(notReadyRow(0, "BTree tail is not covered"), null);

            GlobalIndexQueryServiceDescriptor partial =
                    table.store()
                            .newServiceManager()
                            .globalIndexService(spec.serviceId())
                            .orElseThrow(() -> new AssertionError("Missing descriptor"));
            assertThat(partial.ready()).isFalse();
            assertThat(partial.servedGeneration()).isEqualTo(17L);
            assertThat(partial.servedSnapshotId())
                    .isEqualTo(GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID);

            // A slower generic refresh acknowledgement must not overwrite a validation reason
            // already reported by another executor.
            writer.write(notReadyRow(1, "Refreshing"), null);

            GlobalIndexQueryServiceDescriptor descriptor =
                    table.store()
                            .newServiceManager()
                            .globalIndexService(spec.serviceId())
                            .orElseThrow(() -> new AssertionError("Missing descriptor"));
            assertThat(descriptor.ready()).isFalse();
            assertThat(descriptor.servedGeneration()).isEqualTo(17L);
            assertThat(descriptor.servedSnapshotId()).isEqualTo(23L);
            assertThat(descriptor.reason()).contains("not covered");
        }
    }

    private InternalRow notReadyRow(int executorId, String reason) {
        return GenericRow.of(
                17L,
                false,
                2,
                executorId,
                BinaryString.fromString("127.0.0.1"),
                12345 + executorId,
                BinaryString.fromString(reason),
                17L,
                23L,
                null,
                BinaryString.fromString("epoch-" + executorId));
    }

    private FileStoreTable createTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("url", DataTypes.STRING())
                        .column("descriptor", DataTypes.BYTES())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 h")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return (FileStoreTable) catalog.getTable(identifier());
    }
}
