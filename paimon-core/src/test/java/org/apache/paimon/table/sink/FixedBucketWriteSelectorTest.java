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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FixedBucketWriteSelector}. */
public class FixedBucketWriteSelectorTest {

    @Test
    public void testLegacyConstructorUsesTableBucketCount() {
        TableSchema schema = schema(100);
        GenericRow row = GenericRow.of(1, 456, 7);

        int actual = new FixedBucketWriteSelector(schema).select(row, 128);
        int expected = ChannelComputer.select(BinaryRow.singleColumn(1), 47, 128);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testPerPartitionBucketCount() {
        TableSchema schema = schema(100);
        BinaryRow partition = BinaryRow.singleColumn(1);
        Map<BinaryRow, Integer> partitionBuckets = new HashMap<>();
        partitionBuckets.put(partition, 4);
        PartitionBucketMapping mapping = new PartitionBucketMapping(100, partitionBuckets);
        GenericRow row = GenericRow.of(1, 456, 7);

        int actual = new FixedBucketWriteSelector(schema, mapping).select(row, 128);
        int expected = ChannelComputer.select(partition, 3, 128);

        assertThat(actual).isEqualTo(expected);
    }

    private TableSchema schema(int numBuckets) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType())));
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET_KEY.key(), "b");
        options.put(BUCKET.key(), String.valueOf(numBuckets));
        return new TableSchema(
                0,
                TableSchema.newFields(rowType),
                RowType.currentHighestFieldId(TableSchema.newFields(rowType)),
                Arrays.asList("a"),
                Arrays.asList("a", "b"),
                options,
                "");
    }
}
