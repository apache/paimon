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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link DataSplit} v9 serialization with optional file split boundary. */
public class DataSplitBoundarySerializationTest {

    @Test
    public void roundTripWithBoundary() throws Exception {
        DataSplit original = newSplit(new FileSplitBoundary(1024, 2048, 100));
        byte[] bytes = serialize(original);
        DataSplit deserialized = DataSplit.deserialize(new DataInputDeserializer(bytes));
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.boundary()).isNotNull();
        assertThat(deserialized.boundary().offset()).isEqualTo(1024);
        assertThat(deserialized.boundary().length()).isEqualTo(2048);
        assertThat(deserialized.boundary().rowCount()).isEqualTo(100);
    }

    @Test
    public void roundTripWithoutBoundary() throws Exception {
        DataSplit original = newSplit(null);
        byte[] bytes = serialize(original);
        DataSplit deserialized = DataSplit.deserialize(new DataInputDeserializer(bytes));
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.boundary()).isNull();
    }

    @Test
    public void v9ReaderHandlesV8BytesGracefully() throws Exception {
        DataSplit original = newSplit(null);
        byte[] v9Bytes = serialize(original);
        byte[] v8Bytes = downgradeVersion(v9Bytes, 9, 8);

        DataSplit deserialized = DataSplit.deserialize(new DataInputDeserializer(v8Bytes));
        assertThat(deserialized.boundary()).isNull();
        assertThat(deserialized.dataFiles()).hasSize(1);
        assertThat(deserialized.bucket()).isEqualTo(0);
    }

    private static DataSplit newSplit(FileSplitBoundary boundary) {
        DataSplit.Builder b =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("/tmp/bucket-0")
                        .withDataFiles(Collections.singletonList(newDataFile()))
                        .isStreaming(false)
                        .rawConvertible(true);
        if (boundary != null) {
            b.withBoundary(boundary);
        }
        return b.build();
    }

    private static DataFileMeta newDataFile() {
        return DataFileMeta.create(
                "data.parquet",
                1024,
                100L,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                0,
                null,
                null,
                FileSource.APPEND,
                null,
                0L,
                null);
    }

    private static byte[] serialize(DataSplit split) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(new DataOutputStream(baos)));
        return baos.toByteArray();
    }

    private static byte[] downgradeVersion(byte[] in, int fromVersion, int toVersion) {
        byte[] out = in.clone();
        // VERSION is written as a single int after the 8-byte MAGIC.
        // Layout: MAGIC(8) | VERSION(4) | ...
        // We rewrite VERSION and truncate the trailing boundary block
        // (1 byte boolean=false for no-boundary; for boundary path the test does not call this).
        int versionOffset = 8;
        int v = ((out[versionOffset] & 0xff) << 24)
                | ((out[versionOffset + 1] & 0xff) << 16)
                | ((out[versionOffset + 2] & 0xff) << 8)
                | (out[versionOffset + 3] & 0xff);
        if (v != fromVersion) {
            throw new IllegalStateException("Expected version " + fromVersion + " got " + v);
        }
        out[versionOffset] = (byte) ((toVersion >>> 24) & 0xff);
        out[versionOffset + 1] = (byte) ((toVersion >>> 16) & 0xff);
        out[versionOffset + 2] = (byte) ((toVersion >>> 8) & 0xff);
        out[versionOffset + 3] = (byte) (toVersion & 0xff);
        // Truncate the trailing 1-byte boundary-absent boolean (the test only uses no-boundary case).
        byte[] truncated = new byte[out.length - 1];
        System.arraycopy(out, 0, truncated, 0, truncated.length);
        return truncated;
    }
}
