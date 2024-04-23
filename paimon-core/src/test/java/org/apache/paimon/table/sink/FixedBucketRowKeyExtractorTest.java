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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
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

        assertThatThrownBy(() -> extractor("a", "a", "a,b"))
                .hasMessageContaining("Bucket keys [a] should not in partition keys [a].");
    }

    @Test
    public void testBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThat(bucket(extractor("a", "a,b"), row)).isEqualTo(96);
        assertThat(bucket(extractor("", "a"), row)).isEqualTo(96);
        assertThat(bucket(extractor("", "a,b"), row)).isEqualTo(27);
        assertThat(bucket(extractor("a,b", "a,b"), row)).isEqualTo(27);
        assertThat(bucket(extractor("", ""), row)).isEqualTo(40);
        assertThat(bucket(extractor("a,b,c", ""), row)).isEqualTo(40);
        assertThat(bucket(extractor("", "a,b,c"), row)).isEqualTo(40);
    }

    @Test
    public void testIllegalBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThatThrownBy(() -> bucket(extractor("", "", "a", -1), row))
                .hasMessageContaining("Num bucket is illegal");
    }

    private int bucket(FixedBucketRowKeyExtractor extractor, InternalRow row) {
        extractor.setRecord(row);
        return extractor.bucket();
    }

    private FixedBucketRowKeyExtractor extractor(String bk, String pk) {
        return extractor("", bk, pk);
    }

    private FixedBucketRowKeyExtractor extractor(String partK, String bk, String pk) {
        return extractor(partK, bk, pk, 100);
    }

    private FixedBucketRowKeyExtractor extractor(
            String partK, String bk, String pk, int numBucket) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType())));
        List<DataField> fields = TableSchema.newFields(rowType);
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET_KEY.key(), bk);
        options.put(BUCKET.key(), String.valueOf(numBucket));
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
