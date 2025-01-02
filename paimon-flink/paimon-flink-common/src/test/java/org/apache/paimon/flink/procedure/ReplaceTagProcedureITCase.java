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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link ReplaceTagProcedure}. */
public class ReplaceTagProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testExpireTagsByTagCreateTimeAndTagTimeRetained() throws Exception {
        sql(
                "CREATE TABLE T (id INT, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1')");

        sql("INSERT INTO T VALUES (1, 'a')");
        sql("INSERT INTO T VALUES (2, 'b')");
        assertThat(paimonTable("T").snapshotManager().snapshotCount()).isEqualTo(2);

        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.replace_tag(`table` => 'default.T', tag => 'test_tag')"))
                .hasMessageContaining("Tag 'test_tag' doesn't exist.");

        sql("CALL sys.create_tag(`table` => 'default.T', tag => 'test_tag')");
        assertThat(sql("select tag_name,snapshot_id,time_retained from `T$tags`"))
                .containsExactly(Row.of("test_tag", 2L, null));

        // replace tag with new time_retained
        sql(
                "CALL sys.replace_tag(`table` => 'default.T', tag => 'test_tag',"
                        + " time_retained => '1 d')");
        assertThat(sql("select tag_name,snapshot_id,time_retained from `T$tags`"))
                .containsExactly(Row.of("test_tag", 2L, "PT24H"));

        // replace tag with new snapshot and time_retained
        sql(
                "CALL sys.replace_tag(`table` => 'default.T', tag => 'test_tag',"
                        + " snapshot => 1, time_retained => '2 d')");
        assertThat(sql("select tag_name,snapshot_id,time_retained from `T$tags`"))
                .containsExactly(Row.of("test_tag", 2L, "PT48H"));
    }
}
