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

import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link CastExecutors#safelyCastLiteralsWithNumericEvolution}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FilterPushdownWithSchemaChangeITCase extends CatalogITCaseBase {

    private final String fileFormat;

    public FilterPushdownWithSchemaChangeITCase(String fileFormat) {
        this.fileFormat = fileFormat;
    }

    @SuppressWarnings("unused")
    @Parameters(name = "file-format = {0}")
    public static List<String> fileFormat() {
        return Arrays.asList("parquet", "orc", "avro");
    }

    @TestTemplate
    public void testDecimalToDecimal() {
        // to higher precision
        sql(
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f DECIMAL(5, 2)"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ")",
                fileFormat);
        sql("INSERT INTO T VALUES (1, 111.32)");
        sql("ALTER TABLE T MODIFY (f DECIMAL(6, 3))");
        assertThat(sql("SELECT * FROM T WHERE f < 111.321"))
                .containsExactly(Row.of(1, new BigDecimal("111.320")));
        assertThat(sql("SELECT * FROM T WHERE f = 111.321")).isEmpty();
        assertThat(sql("SELECT * FROM T WHERE f = 111.320"))
                .containsExactly(Row.of(1, new BigDecimal("111.320")));
        assertThat(sql("SELECT * FROM T WHERE f <> 111.321"))
                .containsExactly(Row.of(1, new BigDecimal("111.320")));

        sql("DROP TABLE T");

        // to lower precision
        sql(
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f DECIMAL(6, 3)"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ")",
                fileFormat);
        sql("INSERT INTO T VALUES (1, 111.321), (2, 111.331)");
        sql("ALTER TABLE T MODIFY (f DECIMAL(5, 2))");
        assertThat(sql("SELECT * FROM T WHERE f > 111.32"))
                .containsExactly(Row.of(2, new BigDecimal("111.33")));
        assertThat(sql("SELECT * FROM T WHERE f = 111.32"))
                .containsExactly(Row.of(1, new BigDecimal("111.32")));
        assertThat(sql("SELECT * FROM T WHERE f <> 111.32"))
                .containsExactly(Row.of(2, new BigDecimal("111.33")));
    }

    @TestTemplate
    public void testNumericPrimitiveToDecimal() {
        String ddl =
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f DECIMAL(5, 2)"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ")";

        // to higher precision
        sql(ddl, fileFormat);
        sql("INSERT INTO T VALUES (1, 111.32)");
        sql("ALTER TABLE T MODIFY (f DOUBLE)");
        assertThat(sql("SELECT * FROM T WHERE f < 111.321")).containsExactly(Row.of(1, 111.32));
        assertThat(sql("SELECT * FROM T WHERE f = 111.321")).isEmpty();
        assertThat(sql("SELECT * FROM T WHERE f = 111.320")).containsExactly(Row.of(1, 111.32));
        assertThat(sql("SELECT * FROM T WHERE f <> 111.321")).containsExactly(Row.of(1, 111.32));

        sql("DROP TABLE T");

        // to lower precision
        sql(ddl, fileFormat);
        sql("INSERT INTO T VALUES (1, 111.32), (2, 112.33)");
        sql("ALTER TABLE T MODIFY (f INT)");
        assertThat(sql("SELECT * FROM T WHERE f < 112")).containsExactly(Row.of(1, 111));
        assertThat(sql("SELECT * FROM T WHERE f > 112")).isEmpty();
        assertThat(sql("SELECT * FROM T WHERE f = 111")).containsExactly(Row.of(1, 111));
        assertThat(sql("SELECT * FROM T WHERE f <> 111")).containsExactly(Row.of(2, 112));
    }

    @TestTemplate
    public void testDecimalToNumericPrimitive() {
        // to higher precision
        sql(
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f INT"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ")",
                fileFormat);
        sql("INSERT INTO T VALUES (1, 111)");
        sql("ALTER TABLE T MODIFY (f DECIMAL(5, 2))");
        assertThat(sql("SELECT * FROM T WHERE f < 111.01"))
                .containsExactly(Row.of(1, new BigDecimal("111.00")));
        assertThat(sql("SELECT * FROM T WHERE f = 111.01")).isEmpty();
        assertThat(sql("SELECT * FROM T WHERE f = 111.00"))
                .containsExactly(Row.of(1, new BigDecimal("111.00")));
        assertThat(sql("SELECT * FROM T WHERE f <> 111.01"))
                .containsExactly(Row.of(1, new BigDecimal("111.00")));

        sql("DROP TABLE T");

        // to lower precision
        sql(
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f DOUBLE"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ")",
                fileFormat);
        sql("INSERT INTO T VALUES (1, 111.321), (2, 111.331)");
        sql("ALTER TABLE T MODIFY (f DECIMAL(5, 2))");
        assertThat(sql("SELECT * FROM T WHERE f > 111.32"))
                .containsExactly(Row.of(2, new BigDecimal("111.33")));
        assertThat(sql("SELECT * FROM T WHERE f = 111.32"))
                .containsExactly(Row.of(1, new BigDecimal("111.32")));
        assertThat(sql("SELECT * FROM T WHERE f <> 111.32"))
                .containsExactly(Row.of(2, new BigDecimal("111.33")));
    }

    @TestTemplate
    public void testNumericPrimitive() {
        // no checks for high scale to low scale because we don't pushdown it

        // integer to higher scale integer
        sql(
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f TINYINT"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ")",
                fileFormat);
        sql("INSERT INTO T VALUES (1, CAST (127 AS TINYINT))");
        sql("ALTER TABLE T MODIFY (f INT)");
        // (byte) 383 == 127
        assertThat(sql("SELECT * FROM T WHERE f < 128")).containsExactly(Row.of(1, 127));
        assertThat(sql("SELECT * FROM T WHERE f < 383")).containsExactly(Row.of(1, 127));
        assertThat(sql("SELECT * FROM T WHERE f = 127")).containsExactly(Row.of(1, 127));
        assertThat(sql("SELECT * FROM T WHERE f = 383")).isEmpty();
        assertThat(sql("SELECT * FROM T WHERE f <> 127")).isEmpty();
        assertThat(sql("SELECT * FROM T WHERE f <> 383")).containsExactly(Row.of(1, 127));
    }

    @TestTemplate
    public void testNumericToString() {
        // no more string related tests because we don't push down it
        sql(
                "CREATE TABLE T ("
                        + "  id INT,"
                        + "  f STRING"
                        + ") with ("
                        + "  'file.format' = '%s'"
                        + ");",
                fileFormat);
        sql("INSERT INTO T VALUES (1, '1'), (2, '111')");
        sql("ALTER TABLE T MODIFY (f INT)");
        assertThat(sql("SELECT * FROM T WHERE f > 2")).containsExactly(Row.of(2, 111));
        assertThat(sql("SELECT * FROM T WHERE f = 1")).containsExactly(Row.of(1, 1));
        assertThat(sql("SELECT * FROM T WHERE f <> 1")).containsExactly(Row.of(2, 111));
    }
}
