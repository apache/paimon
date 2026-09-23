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

package org.apache.paimon.spark;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.spark.SparkTypeUtils.fromPaimonRowType;
import static org.apache.paimon.spark.SparkTypeUtils.toPaimonType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for legacy timestamp mapping. */
public class SparkLegacyTimestampMappingTest {

    private static final String LEGACY_TIMESTAMP_MAPPING_KEY =
            "spark.paimon.legacy-timestamp-mapping.enabled";

    @Test
    public void testLegacyTimestampMapping() {
        RowType paimonType = RowType.builder().field("ts", DataTypes.TIMESTAMP()).build();

        assertThat(fromPaimonRowType(paimonType).apply("ts").dataType())
                .isEqualTo(org.apache.spark.sql.types.DataTypes.TimestampNTZType);

        withSQLConf(
                LEGACY_TIMESTAMP_MAPPING_KEY,
                "true",
                () -> {
                    StructType sparkType = fromPaimonRowType(paimonType);
                    assertThat(sparkType.apply("ts").dataType())
                            .isEqualTo(org.apache.spark.sql.types.DataTypes.TimestampType);
                    assertThat(toPaimonType(sparkType)).isEqualTo(paimonType);
                });
    }

    private static void withSQLConf(String key, String value, Runnable test) {
        SQLConf conf = SQLConf.get();
        boolean contains = conf.contains(key);
        String originalValue = contains ? conf.getConfString(key) : null;
        conf.setConfString(key, value);
        try {
            test.run();
        } finally {
            if (contains) {
                conf.setConfString(key, originalValue);
            } else {
                conf.unsetConf(key);
            }
        }
    }
}
