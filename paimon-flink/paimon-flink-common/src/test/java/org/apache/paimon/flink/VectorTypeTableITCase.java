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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.ArrayUtils;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/** Test write and read table with vector type. */
public class VectorTypeTableITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();

    private final String testTblName = "vector_table";

    private final float[] testVector = randomVector();

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(getCreateTableDdl());
    }

    @Test
    public void testBasic() throws Exception {
        batchSql("SELECT * FROM %s", testTblName);
        batchSql("INSERT INTO %s VALUES %s", testTblName, makeValueStr(1, false));
        batchSql("INSERT INTO %s VALUES %s", testTblName, makeValueStr(2, false));

        { // Check by Flink SQL.
            List<Row> rows = batchSql("SELECT * FROM %s ORDER BY id ASC", testTblName);
            Assertions.assertEquals(2, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals(1, (int) row.getFieldAs("id"));
            Assertions.assertEquals("paimon", row.getFieldAs("data"));
            Assertions.assertArrayEquals(ArrayUtils.toObject(testVector), row.getFieldAs("embed"));

            row = rows.get(1);
            Assertions.assertEquals(2, (int) row.getFieldAs("id"));
            Assertions.assertEquals("paimon", row.getFieldAs("data"));
            Assertions.assertArrayEquals(ArrayUtils.toObject(testVector), row.getFieldAs("embed"));
        }

        { // Check by Paimon API.
            List<InternalRow> rows = innerReadData();
            Assertions.assertEquals(2, rows.size());
            rows.sort(Comparator.comparingInt(r -> r.getInt(0)));
            Assertions.assertEquals(2, rows.size());

            InternalRow row = rows.get(0);
            Assertions.assertEquals(1, row.getInt(0));
            Assertions.assertEquals("paimon", row.getString(1).toString());
            Assertions.assertArrayEquals(testVector, row.getVector(2).toFloatArray());

            row = rows.get(1);
            Assertions.assertEquals(2, row.getInt(0));
            Assertions.assertEquals("paimon", row.getString(1).toString());
            Assertions.assertArrayEquals(testVector, row.getVector(2).toFloatArray());
        }

        checkTableSchema();
    }

    @Test
    public void testNullValues() throws Exception {
        batchSql("SELECT * FROM %s", testTblName);
        batchSql("INSERT INTO %s VALUES %s", testTblName, makeValueStr(1, false));
        batchSql("INSERT INTO %s VALUES %s", testTblName, makeValueStr(2, true));
        batchSql("INSERT INTO %s VALUES %s", testTblName, makeValueStr(3, false));
        batchSql("INSERT INTO %s VALUES %s", testTblName, makeValueStr(4, true));

        { // Check by Flink SQL.
            List<Row> rows = batchSql("SELECT * FROM %s ORDER BY id ASC", testTblName);
            Assertions.assertEquals(4, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals(1, (int) row.getFieldAs("id"));
            Assertions.assertEquals("paimon", row.getFieldAs("data"));
            Assertions.assertNotNull(row.getFieldAs("embed"));
            Assertions.assertArrayEquals(ArrayUtils.toObject(testVector), row.getFieldAs("embed"));

            row = rows.get(1);
            Assertions.assertEquals(2, (int) row.getFieldAs("id"));
            Assertions.assertEquals("paimon", row.getFieldAs("data"));
            Assertions.assertNull(row.getFieldAs("embed"));
        }

        { // Check by Paimon API.
            List<InternalRow> rows = innerReadData();
            Assertions.assertEquals(4, rows.size());
            rows.sort(Comparator.comparingInt(r -> r.getInt(0)));
            Assertions.assertEquals(4, rows.size());

            InternalRow row = rows.get(0);
            Assertions.assertEquals(1, row.getInt(0));
            Assertions.assertEquals("paimon", row.getString(1).toString());
            Assertions.assertFalse(row.isNullAt(2));
            Assertions.assertArrayEquals(testVector, row.getVector(2).toFloatArray());

            row = rows.get(1);
            Assertions.assertEquals(2, row.getInt(0));
            Assertions.assertEquals("paimon", row.getString(1).toString());
            Assertions.assertTrue(row.isNullAt(2));
        }

        checkTableSchema();
    }

    private void checkTableSchema() throws Exception {
        DataType vectorType = DataTypes.VECTOR(testVector.length, DataTypes.FLOAT());
        List<DataField> fields = paimonTable(testTblName).schema().fields();
        Assertions.assertEquals(3, fields.size());
        Assertions.assertEquals(DataTypes.INT(), fields.get(0).type());
        Assertions.assertEquals(DataTypes.STRING(), fields.get(1).type());
        Assertions.assertEquals(vectorType, fields.get(2).type());
    }

    private List<InternalRow> innerReadData() throws Exception {
        ReadBuilder builder = paimonTable(testTblName).newReadBuilder();
        RecordReader<InternalRow> reader = builder.newRead().createReader(builder.newScan().plan());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    rows.add(row);
                    Assertions.assertTrue(rows.size() < 10);
                });
        return rows;
    }

    private String getCreateTableDdl() {
        return String.format(
                "CREATE TABLE IF NOT EXISTS `%s` ("
                        + "    `id` INT,"
                        + "    `data` STRING,"
                        + "    `embed` ARRAY<FLOAT>"
                        + ") WITH ("
                        + "    'file.format' = 'json',"
                        + "    'file.compression' = 'none',"
                        + "    'field.embed.vector-dim' = '%d'"
                        + ")",
                testTblName, testVector.length);
    }

    private String makeValueStr(int id, boolean nullVector) {
        String vectorValueStr =
                nullVector ? "CAST(NULL AS ARRAY<FLOAT>)" : ("ARRAY" + Arrays.toString(testVector));
        return String.format("(%d, '%s', %s)", id, "paimon", vectorValueStr);
    }

    private float[] randomVector() {
        byte[] randomBytes = new byte[RANDOM.nextInt(1024) + 1];
        RANDOM.nextBytes(randomBytes);
        float[] vector = new float[randomBytes.length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomBytes[i];
        }
        return vector;
    }
}
