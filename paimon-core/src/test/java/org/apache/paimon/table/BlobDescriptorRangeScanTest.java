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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end descriptor equality for range-split batch reads. */
public class BlobDescriptorRangeScanTest extends DataEvolutionTestBase {
    @Test
    public void testDescriptorRangesReadExactlyOnce() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("image", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder writer = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writer.newWrite();
                BatchTableCommit commit = writer.newCommit()) {
            for (int i = 0; i < 20; i++) {
                write.write(
                        GenericRow.of(i, i % 4 == 0 ? null : new BlobData(new byte[] {(byte) i})));
            }
            commit.commit(write.prepareCommit());
        }
        FileStoreTable descriptors =
                table.copy(Collections.singletonMap(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true"));
        List<String> expected = read(descriptors);
        FileStoreTable ranged =
                descriptors.copy(
                        Collections.singletonMap(
                                CoreOptions.BLOB_DESCRIPTOR_SCAN_MAX_FILES.key(), "3"));
        TableScan.Plan plan = ranged.newReadBuilder().newScan().plan();
        assertThat(plan.splits()).hasSizeGreaterThan(1).allMatch(s -> s instanceof IndexedSplit);
        assertThat(read(ranged)).containsExactlyElementsOf(expected);
        assertThat(expected).hasSize(20);
        PredicateBuilder predicate = new PredicateBuilder(ranged.rowType());
        ReadBuilder filtered = ranged.newReadBuilder().withFilter(predicate.greaterOrEqual(0, 10));
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                filtered.newRead().createReader(filtered.newScan().plan())) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        Collections.sort(ids);
        // Reader predicates are best-effort pruning; compare with the unsplit scan.
        ReadBuilder baseline =
                descriptors.newReadBuilder().withFilter(predicate.greaterOrEqual(0, 10));
        List<Integer> baselineIds = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                baseline.newRead().createReader(baseline.newScan().plan())) {
            reader.forEachRemaining(row -> baselineIds.add(row.getInt(0)));
        }
        Collections.sort(baselineIds);
        assertThat(ids).containsExactlyElementsOf(baselineIds);
        assertThat(ranged.newReadBuilder().withLimit(5).newScan().plan().splits())
                .noneMatch(s -> s instanceof IndexedSplit);
        // The option is inert for ordinary payload reads.
        FileStoreTable payloads =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.BLOB_DESCRIPTOR_SCAN_MAX_FILES.key(), "3"));
        assertThat(payloads.newReadBuilder().newScan().plan().splits()).hasSize(1);
    }

    private List<String> read(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            result.add(
                                    row.getInt(0)
                                            + ":"
                                            + (row.isNullAt(1)
                                                    ? "null"
                                                    : row.getBlob(1).toDescriptor().toString())));
        }
        Collections.sort(result);
        return result;
    }
}
