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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.UriReader;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link ReadOperator} reading BLOB values on the dedicated split path. */
public class ReadOperatorBlobTest {

    private static final RowType READ_TYPE = RowType.of(DataTypes.BLOB());

    @Test
    public void testReadBlobAsDescriptor() throws Exception {
        BlobDescriptor descriptor = new BlobDescriptor("file:///blob", 7, 11);
        byte[] result = readBlob(Blob.fromDescriptor(UriReader.fromHttp(), descriptor), true);

        assertThat(result).isEqualTo(descriptor.serialize());
    }

    @Test
    public void testReadBlobAsData() throws Exception {
        byte[] data = new byte[] {1, 2, 3};

        assertThat(readBlob(Blob.fromData(data), false)).isEqualTo(data);
    }

    private byte[] readBlob(Blob blob, boolean blobAsDescriptor) throws Exception {
        ReadOperator operator =
                new ReadOperator(
                        () -> new TestingTableRead(GenericRow.of(blob)),
                        null,
                        null,
                        READ_TYPE,
                        blobAsDescriptor);
        OneInputStreamOperatorTestHarness<Split, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(InternalSerializers.create(toLogicalType(READ_TYPE)));
        harness.open();
        try {
            DataSplit split =
                    DataSplit.builder()
                            .withPartition(BinaryRow.EMPTY_ROW)
                            .withBucket(0)
                            .withBucketPath("bucket-0")
                            .withDataFiles(Collections.emptyList())
                            .build();
            harness.processElement(new StreamRecord<>(split));

            assertThat(harness.getOutput()).hasSize(1);
            @SuppressWarnings("unchecked")
            StreamRecord<RowData> result =
                    (StreamRecord<RowData>) harness.getOutput().iterator().next();
            return result.getValue().getBinary(0);
        } finally {
            harness.close();
        }
    }

    private static class TestingTableRead implements TableRead {

        private final InternalRow row;

        private TestingTableRead(InternalRow row) {
            this.row = row;
        }

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            return this;
        }

        @Override
        public TableRead executeFilter() {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            return new IteratorRecordReader<>(Collections.singleton(row).iterator());
        }
    }
}
