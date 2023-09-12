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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableActionFactory;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link ActionFactoryTest}. */
public class ActionFactoryTest {

    @Test
    public void testOptionalConfigMap() {
        String[] args = {
            "--table-conf", "bucket=2",
            "--table-conf", "changelog-producer=input",
            "--table-conf", "hive.business_owner=zhangsan",
            "--table-conf", "hive.partition.retention.period=90d"
        };
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        ActionFactory action = new MySqlSyncTableActionFactory();

        // Test case 1: without "hive." prefix
        Map<String, String> tableConfig = action.optionalConfigMap(params, "table-conf", false);
        assertEquals(2, tableConfig.size());
        assertEquals("2", tableConfig.get("bucket"));
        assertEquals("input", tableConfig.get("changelog-producer"));
        assertNull(tableConfig.get("hive.business_owner"));
        assertNull(tableConfig.get("hive.partition.retention.period"));

        // Test case 2: with "hive." prefix
        Map<String, String> hiveProperties = action.optionalConfigMap(params, "table-conf", true);
        assertEquals(2, hiveProperties.size());
        assertEquals("zhangsan", hiveProperties.get("hive.business_owner"));
        assertEquals("90d", hiveProperties.get("hive.partition.retention.period"));
        assertNull(hiveProperties.get("bucket"));
        assertNull(hiveProperties.get("changelog-producer"));
    }
}
