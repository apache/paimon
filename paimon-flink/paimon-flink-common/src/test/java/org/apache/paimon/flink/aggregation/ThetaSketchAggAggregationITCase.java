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

package org.apache.paimon.flink.aggregation;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.commons.codec.binary.Hex;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.paimon.utils.ThetaSketch.sketchOf;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link org.apache.paimon.mergetree.compact.aggregate.FieldThetaSketchAgg}. */
public class ThetaSketchAggAggregationITCase extends CatalogITCaseBase {

    @Test
    public void testThetaSketchAgg() {
        sql(
                "CREATE TABLE test_collect("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 VARBINARY"
                        + ") WITH ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.f0.aggregate-function' = 'theta_sketch'"
                        + ")");

        String str1 = Hex.encodeHexString(sketchOf(1)).toUpperCase();
        String str2 = Hex.encodeHexString(sketchOf(2)).toUpperCase();
        String str3 = Hex.encodeHexString(sketchOf(3)).toUpperCase();

        sql(
                String.format(
                        "INSERT INTO test_collect VALUES (1, CAST (NULL AS VARBINARY)),(2, CAST(x'%s' AS VARBINARY)), (3, CAST(x'%s' AS VARBINARY))",
                        str1, str2));

        List<Row> result = queryAndSort("SELECT * FROM test_collect");
        checkOneRecord(result.get(0), 1, null);
        checkOneRecord(result.get(1), 2, sketchOf(1));
        checkOneRecord(result.get(2), 3, sketchOf(2));

        sql(
                String.format(
                        "INSERT INTO test_collect VALUES (1, CAST (x'%s' AS VARBINARY)),(2, CAST(x'%s' AS VARBINARY)), (2, CAST(x'%s' AS VARBINARY)), (3, CAST(x'%s' AS VARBINARY))",
                        str1, str2, str2, str3));

        result = queryAndSort("SELECT * FROM test_collect");
        checkOneRecord(result.get(0), 1, sketchOf(1));
        checkOneRecord(result.get(1), 2, sketchOf(1, 2));
        checkOneRecord(result.get(2), 3, sketchOf(2, 3));
    }

    private void checkOneRecord(Row row, int id, byte[] expected) {
        assertThat(row.getField(0)).isEqualTo(id);
        assertThat(row.getField(1)).isEqualTo(expected);
    }
}
