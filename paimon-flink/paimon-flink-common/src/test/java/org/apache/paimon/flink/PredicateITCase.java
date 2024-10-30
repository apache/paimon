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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Predicate ITCase. */
public class PredicateITCase extends CatalogITCaseBase {

    @Test
    public void testPkFilterBucket() throws Exception {
        sql("CREATE TABLE T (a INT PRIMARY KEY NOT ENFORCED, b INT) WITH ('bucket' = '5')");
        writeRecords();
        innerTestSingleField();
        innerTestAllFields();
    }

    @Test
    public void testNoPkFilterBucket() throws Exception {
        sql("CREATE TABLE T (a INT, b INT) WITH ('bucket' = '5', 'bucket-key'='a')");
        writeRecords();
        innerTestSingleField();
        innerTestAllFields();
    }

    @Test
    public void testAppendFilterBucket() throws Exception {
        sql("CREATE TABLE T (a INT, b INT) WITH ('bucket' = '5', 'bucket-key'='a')");
        writeRecords();
        innerTestSingleField();
        innerTestAllFields();
    }

    @Test
    public void testIntegerFilter() {
        int rand = ThreadLocalRandom.current().nextInt(3);
        String fileFormat;
        if (rand == 0) {
            fileFormat = "avro";
        } else if (rand == 1) {
            fileFormat = "parquet";
        } else {
            fileFormat = "orc";
        }

        sql(
                "CREATE TABLE T ("
                        + "a TINYINT,"
                        + "b SMALLINT,"
                        + "c INT,"
                        + "d BIGINT"
                        + ") WITH ("
                        + "'file.format' = '%s'"
                        + ")",
                fileFormat);
        sql(
                "INSERT INTO T VALUES (CAST (1 AS TINYINT), CAST (1 AS SMALLINT), 1, 1), "
                        + "(CAST (2 AS TINYINT), CAST (2 AS SMALLINT), 2, 2)");

        Row expectedResult = Row.of((byte) 1, (short) 1, 1, 1L);
        assertThat(sql("SELECT * FROM T WHERE a = CAST (1 AS TINYINT)"))
                .containsExactly(expectedResult);
        assertThat(sql("SELECT * FROM T WHERE b = CAST (1 AS SMALLINT)"))
                .containsExactly(expectedResult);
        assertThat(sql("SELECT * FROM T WHERE c = 1")).containsExactly(expectedResult);
        assertThat(sql("SELECT * FROM T WHERE d = CAST (1 AS BIGINT)"))
                .containsExactly(expectedResult);
    }

    private void writeRecords() throws Exception {
        sql("INSERT INTO T VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)");
    }

    private void innerTestSingleField() throws Exception {
        assertThat(sql("SELECT * FROM T WHERE a = 1")).containsExactlyInAnyOrder(Row.of(1, 2));
        assertThat(sql("SELECT * FROM T WHERE a = 3")).containsExactlyInAnyOrder(Row.of(3, 4));
        assertThat(sql("SELECT * FROM T WHERE a = 5")).containsExactlyInAnyOrder(Row.of(5, 6));
        assertThat(sql("SELECT * FROM T WHERE a = 7")).containsExactlyInAnyOrder(Row.of(7, 8));
        assertThat(sql("SELECT * FROM T WHERE a = 9")).containsExactlyInAnyOrder(Row.of(9, 10));
    }

    private void innerTestAllFields() throws Exception {
        assertThat(sql("SELECT * FROM T WHERE a = 1 and b = 2"))
                .containsExactlyInAnyOrder(Row.of(1, 2));
        assertThat(sql("SELECT * FROM T WHERE a = 3 and b = 4"))
                .containsExactlyInAnyOrder(Row.of(3, 4));
        assertThat(sql("SELECT * FROM T WHERE a = 5 and b = 6"))
                .containsExactlyInAnyOrder(Row.of(5, 6));
        assertThat(sql("SELECT * FROM T WHERE a = 7 and b = 8"))
                .containsExactlyInAnyOrder(Row.of(7, 8));
        assertThat(sql("SELECT * FROM T WHERE a = 9 and b = 10"))
                .containsExactlyInAnyOrder(Row.of(9, 10));
    }
}
