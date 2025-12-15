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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions.BucketFunctionType;

import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Hash type ITCase. */
public class BucketFunctionTypeITCase extends CatalogITCaseBase {

    @ParameterizedTest
    @EnumSource(BucketFunctionType.class)
    public void testInsertAndSelect(BucketFunctionType bucketFunctionType) {
        sql(
                "CREATE TABLE T (a INT, b STRING, c INT) with "
                        + "('bucket-function.type' = '%s', 'bucket-key' = 'a', 'bucket' = '4', "
                        + "'metadata.stats-mode' = 'none')",
                bucketFunctionType);
        // disable filter by manifest and let bucket filter work
        for (int i = 0; i < 10; i++) {
            sql("INSERT INTO T (a, b, c) VALUES (%s, '%s', %s)", i, i, i);
        }

        for (int i = 0; i < 10; i++) {
            List<Row> rows = sql("SELECT * FROM T where a = %s", i);
            assertThat(rows.size()).isEqualTo(1);
            assertThat(rows.get(0)).isEqualTo(Row.of(i, String.valueOf(i), i));
        }
    }
}
