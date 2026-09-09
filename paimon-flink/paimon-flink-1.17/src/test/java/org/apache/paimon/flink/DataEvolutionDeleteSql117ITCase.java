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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Data Evolution SQL DELETE compatibility with Flink 1.17. */
public class DataEvolutionDeleteSql117ITCase extends CatalogITCaseBase {

    @Test
    public void testDelete() {
        sql(
                "CREATE TABLE T (id INT, name STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'deletion-vectors.enabled' = 'true')");
        sql("INSERT INTO T VALUES (1, 'one'), (2, 'two')");

        sql("DELETE FROM T WHERE id = 2");

        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "one"));
    }
}
