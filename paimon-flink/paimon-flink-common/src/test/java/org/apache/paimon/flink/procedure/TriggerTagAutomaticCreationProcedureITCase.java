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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link PurgeFilesProcedure}. */
public class TriggerTagAutomaticCreationProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testAutoTagWithData() {
        testTriggerTagAutomaticCreation(true);
    }

    @Test
    public void testAutoTagWithoutData() {
        testTriggerTagAutomaticCreation(false);
    }

    public void testTriggerTagAutomaticCreation(boolean haveData) {
        sql(
                "CREATE TABLE T (id INT, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1')");

        if (haveData) {
            sql("INSERT INTO T VALUES (1, 'a')");
            assertThat(sql("select * from `T`")).containsExactly(Row.of(1, "a"));
        }
        assertThat(sql("select tag_name from `T$tags`").stream().map(Row::toString))
                .isNullOrEmpty();

        sql(
                "alter table T set ("
                        + "'tag.automatic-creation'='process-time',"
                        + "'tag.creation-period'='daily',"
                        + "'tag.creation-delay'='0 m',"
                        + "'tag.num-retained-max'='90')");

        sql("CALL sys.trigger_tag_automatic_creation(`table` => 'default.T', force => true)");

        assertThat(sql("select tag_name from `T$tags`").stream().map(Row::toString))
                .isNotNull()
                .isNotEmpty();
    }
}
