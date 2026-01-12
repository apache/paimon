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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class MyTest extends ActionITCaseBase {

    @Test
    public void foo() throws Exception {
        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new Path("/Users/yuzelin/Documents/scan")));
        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("warehouse", "/Users/yuzelin/Documents/scan");
        CompactDuplicatedManifestsAction action =
                new CompactDuplicatedManifestsAction("default", "test_table", catalogConfig);
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }
}
