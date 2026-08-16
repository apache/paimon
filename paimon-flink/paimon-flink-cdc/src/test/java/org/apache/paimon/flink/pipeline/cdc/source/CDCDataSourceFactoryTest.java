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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.table.api.ValidationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CDCDataSourceFactory}. */
public class CDCDataSourceFactoryTest {
    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testCreateDataSink() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSourceFactory.class);
        assertThat(sourceFactory).isInstanceOf(CDCDataSourceFactory.class);

        Map<String, String> catalogOptionsMap = new HashMap<>();
        catalogOptionsMap.put("catalog.properties.warehouse", temporaryFolder.toUri().toString());
        Configuration conf = Configuration.fromMap(catalogOptionsMap);
        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        assertThat(dataSource).isInstanceOf(CDCDataSource.class);
    }

    @Test
    void testUnsupportedOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSourceFactory.class);
        assertThat(sourceFactory).isInstanceOf(CDCDataSourceFactory.class);

        Map<String, String> catalogOptionsMap = new HashMap<>();
        catalogOptionsMap.put("warehouse", temporaryFolder.toUri().toString());
        Configuration conf = Configuration.fromMap(catalogOptionsMap);
        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'paimon'.")
                .hasMessageContaining("Unsupported options:")
                .hasMessageContaining("warehouse");
    }
}
