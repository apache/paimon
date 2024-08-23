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

import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestCatalogFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkGenericCatalogFactory}. */
public class FlinkGenericCatalogFactoryTest {

    private final FlinkGenericCatalogFactory genericCatalogFactory =
            new FlinkGenericCatalogFactory();

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testGenericCatalogOptionsFilter() {
        String path1 = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        String path2 = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();

        TestCatalogFactory testCatalogFactory = new TestCatalogFactory();
        String catalogName = "test-catalog";
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", path1);
        options.put(TestCatalogFactory.DEFAULT_DATABASE.key(), path2);
        CatalogFactory.Context context =
                new FactoryUtil.DefaultCatalogContext(
                        catalogName,
                        options,
                        null,
                        FlinkGenericCatalogFactoryTest.class.getClassLoader());

        CatalogFactory.Context flinkContext =
                genericCatalogFactory.filterContextOptions(context, testCatalogFactory);

        Map<String, String> flinkOptions = flinkContext.getOptions();
        assertThat(flinkOptions.get(TestCatalogFactory.DEFAULT_DATABASE.key()))
                .isEqualTo(options.get(TestCatalogFactory.DEFAULT_DATABASE.key()));
        assertThat(flinkOptions.get("warehouse")).isNull();
    }
}
