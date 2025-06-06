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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.BUCKET_STRATEGY;
import static org.apache.paimon.table.sink.KeyAndBucketExtractor.bucketKeyHashCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FixedBucketRowKeyExtractor}. */
public class FixedBucketRowKeyExtractorTest {

    @Test
    public void testInvalidBucket() {
        assertThatThrownBy(() -> extractor("n", "b"))
                .hasMessageContaining("Field names [a, b, c] should contains all bucket keys [n].");

        assertThatThrownBy(() -> extractor("a", "b"))
                .hasMessageContaining("Primary keys [b] should contains all bucket keys [a].");

        assertThatThrownBy(() -> extractor("a", "a", "a,b", null))
                .hasMessageContaining("Bucket keys [a] should not in partition keys [a].");
    }

    @Test
    public void testBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThat(bucket(extractor("a", "a,b"), row)).isEqualTo(96);
        assertThat(bucket(extractor("", "a"), row)).isEqualTo(96);
        assertThat(bucket(extractor("", "a,b"), row)).isEqualTo(27);
        assertThat(bucket(extractor("a,b", "a,b"), row)).isEqualTo(27);
        assertThat(bucket(extractor("a,b,c", ""), row)).isEqualTo(40);
        assertThat(bucket(extractor("", "a,b,c"), row)).isEqualTo(40);
    }

    @Test
    public void testIllegalBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThatThrownBy(() -> bucket(extractor("", "", "a", -1), row));
    }

    @Test
    public void testUnCompactDecimalAndTimestampNullValueBucketNumber() {
        GenericRow row = GenericRow.of(null, null, null, 1);
        int bucketNum = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);

        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "d", new DecimalType(38, 18)),
                                new DataField(1, "ltz", new LocalZonedTimestampType()),
                                new DataField(2, "ntz", new TimestampType()),
                                new DataField(3, "k", new IntType())));

        String[] bucketColsToTest = {"d", "ltz", "ntz"};
        for (String bucketCol : bucketColsToTest) {
            FixedBucketRowKeyExtractor extractor = extractor(rowType, "", bucketCol, "", bucketNum);
            BinaryRow binaryRow =
                    new InternalRowSerializer(rowType.project(bucketCol)).toBinaryRow(row);
            assertThat(bucket(extractor, row))
                    .isEqualTo(
                            KeyAndBucketExtractor.bucket(bucketKeyHashCode(binaryRow), bucketNum));
        }
    }

    @Test
    public void testWithBucketStrategyBased() {
        GenericRow row = GenericRow.of(5, 6, 7);
        String bucketStrategy = "truncate[1]";
        assertThat(bucket(extractor("a", "", bucketStrategy), row)).isEqualTo(5);
        assertThat(bucket(extractor("b", "", bucketStrategy), row)).isEqualTo(6);
        assertThat(bucket(extractor("c", "", bucketStrategy), row)).isEqualTo(7);
    }

    private int bucket(FixedBucketRowKeyExtractor extractor, InternalRow row) {
        extractor.setRecord(row);
        return extractor.bucket();
    }

    private FixedBucketRowKeyExtractor extractor(String bk, String pk) {
        return extractor("", bk, pk, null);
    }

    private FixedBucketRowKeyExtractor extractor(
            String bk, String pk, @Nullable String bucketStrategy) {
        return extractor("", bk, pk, bucketStrategy);
    }

    private FixedBucketRowKeyExtractor extractor(
            String partK, String bk, String pk, @Nullable String bucketStrategy) {
        return extractor(partK, bk, pk, 100, bucketStrategy);
    }

    private FixedBucketRowKeyExtractor extractor(
            String partK, String bk, String pk, int numBucket) {
        return extractor(partK, bk, pk, numBucket, null);
    }

    private FixedBucketRowKeyExtractor extractor(
            String partK, String bk, String pk, int numBucket, @Nullable String bucketStrategy) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType())));
        return extractor(rowType, partK, bk, pk, numBucket, bucketStrategy);
    }

    private FixedBucketRowKeyExtractor extractor(
            RowType rowType, String partK, String bk, String pk, int numBucket) {
        return extractor(rowType, partK, bk, pk, numBucket, null);
    }

    private FixedBucketRowKeyExtractor extractor(
            RowType rowType,
            String partK,
            String bk,
            String pk,
            int numBucket,
            @Nullable String bucketStrategy) {
        List<DataField> fields = TableSchema.newFields(rowType);
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET_KEY.key(), bk);
        options.put(BUCKET.key(), String.valueOf(numBucket));
        if (bucketStrategy != null) {
            options.put(BUCKET_STRATEGY.key(), bucketStrategy);
        }
        TableSchema schema =
                new TableSchema(
                        0,
                        fields,
                        RowType.currentHighestFieldId(fields),
                        "".equals(partK)
                                ? Collections.emptyList()
                                : Arrays.asList(partK.split(",")),
                        "".equals(pk) ? Collections.emptyList() : Arrays.asList(pk.split(",")),
                        options,
                        "");
        return new FixedBucketRowKeyExtractor(schema);
    }
}
