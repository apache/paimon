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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link QueryAuthSplit}. */
public class QueryAuthSplitTest {

    @Test
    public void testUnwrapDataSplit() {
        DataSplit dataSplit = dataSplit();

        assertThat(QueryAuthSplit.unwrapDataSplit(dataSplit)).isSameAs(dataSplit);
        assertThat(QueryAuthSplit.unwrapDataSplit(new QueryAuthSplit(dataSplit, authResult())))
                .isSameAs(dataSplit);

        Split fallbackDataSplit = FallbackReadFileStoreTable.toFallbackSplit(dataSplit, false);
        assertThat(QueryAuthSplit.unwrapDataSplit(fallbackDataSplit)).isSameAs(fallbackDataSplit);

        Split fallbackQueryAuthSplit =
                new FallbackReadFileStoreTable.FallbackSplitImpl(
                        new QueryAuthSplit(dataSplit, authResult()), false);
        assertThat(QueryAuthSplit.unwrapDataSplit(fallbackQueryAuthSplit)).isSameAs(dataSplit);
    }

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        QueryAuthSplit split = new QueryAuthSplit(dataSplit(), authResult());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));

        QueryAuthSplit deserialized =
                QueryAuthSplit.deserialize(new DataInputDeserializer(out.toByteArray()));

        assertQueryAuthSplitEquals(split, deserialized);
    }

    @Test
    public void testJavaSerializeAndDeserialize() throws Exception {
        QueryAuthSplit split = new QueryAuthSplit(dataSplit(), authResult());

        byte[] bytes = InstantiationUtil.serializeObject(split);
        QueryAuthSplit deserialized =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        assertQueryAuthSplitEquals(split, deserialized);
    }

    private static DataSplit dataSplit() {
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        DataFileTestDataGenerator.Data data = gen.next();
        DataFileMeta file = gen.next().meta;
        return DataSplit.builder()
                .withSnapshot(1L)
                .withPartition(data.partition)
                .withBucket(data.bucket)
                .withBucketPath("bucket-path")
                .withDataFiles(Collections.singletonList(file))
                .build();
    }

    private static TableQueryAuthResult authResult() {
        Map<String, String> columnMasking = new HashMap<>();
        columnMasking.put("name", "mask-json");
        return new TableQueryAuthResult(Collections.singletonList(largeFilter()), columnMasking);
    }

    private static String largeFilter() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 70_000; i++) {
            builder.append('x');
        }
        return builder.toString();
    }

    private static void assertQueryAuthSplitEquals(QueryAuthSplit expected, QueryAuthSplit actual) {
        assertThat(actual.split()).isEqualTo(expected.split());
        assertThat(actual.authResult()).isNotNull();
        assertThat(actual.authResult().filter()).isEqualTo(expected.authResult().filter());
        assertThat(actual.authResult().columnMasking())
                .isEqualTo(expected.authResult().columnMasking());
        assertThat(actual.mergedRowCount()).isEmpty();
    }
}
