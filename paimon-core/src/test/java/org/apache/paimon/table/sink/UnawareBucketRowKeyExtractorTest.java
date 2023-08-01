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
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UnawareBucketRowKeyExtractor}. */
public class UnawareBucketRowKeyExtractorTest {

    @Test
    public void testBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThat(bucket(extractor("a"), row)).isEqualTo(0);
    }

    private int bucket(UnawareBucketRowKeyExtractor extractor, InternalRow row) {
        extractor.setRecord(row);
        return extractor.bucket();
    }

    private UnawareBucketRowKeyExtractor extractor(String partK) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType())));
        List<DataField> fields = TableSchema.newFields(rowType);
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), "-1");
        TableSchema schema =
                new TableSchema(
                        0,
                        fields,
                        RowType.currentHighestFieldId(fields),
                        "".equals(partK)
                                ? Collections.emptyList()
                                : Arrays.asList(partK.split(",")),
                        Collections.emptyList(),
                        options,
                        "");
        return new UnawareBucketRowKeyExtractor(schema);
    }
}
