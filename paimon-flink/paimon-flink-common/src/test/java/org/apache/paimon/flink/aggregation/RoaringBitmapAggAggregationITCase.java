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
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.paimon.utils.RoaringBitmap64;

import org.apache.commons.codec.binary.Hex;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for {@link org.apache.paimon.mergetree.compact.aggregate.FieldRoaringBitmap32Agg} & {@link
 * org.apache.paimon.mergetree.compact.aggregate.FieldRoaringBitmap64Agg}.
 */
public class RoaringBitmapAggAggregationITCase extends CatalogITCaseBase {

    @Test
    public void testRoaring32BitmapAgg() throws IOException {
        sql(
                "CREATE TABLE test_rbm64("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 VARBINARY"
                        + ") WITH ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.f0.aggregate-function' = 'rbm32'"
                        + ")");

        byte[] v1Bytes = RoaringBitmap32.bitmapOf(1).serialize();
        byte[] v2Bytes = RoaringBitmap32.bitmapOf(2).serialize();
        byte[] v3Bytes = RoaringBitmap32.bitmapOf(3).serialize();
        byte[] v4Bytes = RoaringBitmap32.bitmapOf(1, 2).serialize();
        byte[] v5Bytes = RoaringBitmap32.bitmapOf(2, 3).serialize();
        String v1 = Hex.encodeHexString(v1Bytes).toUpperCase();
        String v2 = Hex.encodeHexString(v2Bytes).toUpperCase();
        String v3 = Hex.encodeHexString(v3Bytes).toUpperCase();

        sql(
                "INSERT INTO test_rbm64 VALUES "
                        + "(1, CAST (NULL AS VARBINARY)), "
                        + "(2, CAST (x'"
                        + v1
                        + "' AS VARBINARY)), "
                        + "(3, CAST (x'"
                        + v2
                        + "' AS VARBINARY))");

        List<Row> result = queryAndSort("SELECT * FROM test_rbm64");
        checkOneRecord(result.get(0), 1, null);
        checkOneRecord(result.get(1), 2, v1Bytes);
        checkOneRecord(result.get(2), 3, v2Bytes);

        sql(
                "INSERT INTO test_rbm64 VALUES "
                        + "(1, CAST (x'"
                        + v1
                        + "' AS VARBINARY)), "
                        + "(2, CAST (x'"
                        + v2
                        + "' AS VARBINARY)), "
                        + "(2, CAST (x'"
                        + v2
                        + "' AS VARBINARY)), "
                        + "(3, CAST (x'"
                        + v3
                        + "' AS VARBINARY))");

        result = queryAndSort("SELECT * FROM test_rbm64");
        checkOneRecord(result.get(0), 1, v1Bytes);
        checkOneRecord(result.get(1), 2, v4Bytes);
        checkOneRecord(result.get(2), 3, v5Bytes);
    }

    @Test
    public void testRoaring64BitmapAgg() throws IOException {
        sql(
                "CREATE TABLE test_rbm64("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 VARBINARY"
                        + ") WITH ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.f0.aggregate-function' = 'rbm64'"
                        + ")");

        byte[] v1Bytes = RoaringBitmap64.bitmapOf(1L).serialize();
        byte[] v2Bytes = RoaringBitmap64.bitmapOf(2L).serialize();
        byte[] v3Bytes = RoaringBitmap64.bitmapOf(3L).serialize();
        byte[] v4Bytes = RoaringBitmap64.bitmapOf(1L, 2L).serialize();
        byte[] v5Bytes = RoaringBitmap64.bitmapOf(2L, 3L).serialize();
        String v1 = Hex.encodeHexString(v1Bytes).toUpperCase();
        String v2 = Hex.encodeHexString(v2Bytes).toUpperCase();
        String v3 = Hex.encodeHexString(v3Bytes).toUpperCase();

        sql(
                "INSERT INTO test_rbm64 VALUES "
                        + "(1, CAST (NULL AS VARBINARY)), "
                        + "(2, CAST (x'"
                        + v1
                        + "' AS VARBINARY)), "
                        + "(3, CAST (x'"
                        + v2
                        + "' AS VARBINARY))");

        List<Row> result = queryAndSort("SELECT * FROM test_rbm64");
        checkOneRecord(result.get(0), 1, null);
        checkOneRecord(result.get(1), 2, v1Bytes);
        checkOneRecord(result.get(2), 3, v2Bytes);

        sql(
                "INSERT INTO test_rbm64 VALUES "
                        + "(1, CAST (x'"
                        + v1
                        + "' AS VARBINARY)), "
                        + "(2, CAST (x'"
                        + v2
                        + "' AS VARBINARY)), "
                        + "(2, CAST (x'"
                        + v2
                        + "' AS VARBINARY)), "
                        + "(3, CAST (x'"
                        + v3
                        + "' AS VARBINARY))");

        result = queryAndSort("SELECT * FROM test_rbm64");
        checkOneRecord(result.get(0), 1, v1Bytes);
        checkOneRecord(result.get(1), 2, v4Bytes);
        checkOneRecord(result.get(2), 3, v5Bytes);
    }

    private void checkOneRecord(Row row, int id, byte[] expected) {
        assertThat(row.getField(0)).isEqualTo(id);
        assertThat(row.getField(1)).isEqualTo(expected);
    }
}
