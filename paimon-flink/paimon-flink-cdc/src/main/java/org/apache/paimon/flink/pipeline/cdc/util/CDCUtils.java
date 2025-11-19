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

package org.apache.paimon.flink.pipeline.cdc.util;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.utils.ReflectionUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Utility methods for CDC class. */
public class CDCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CDCUtils.class);

    public static Catalog createCatalog(CatalogContext catalogContext) {
        Configuration flinkConfig;
        try {
            Method method = ReflectionUtils.getMethod(catalogContext.getClass(), "getFlinkConf", 0);
            flinkConfig = (Configuration) method.invoke(catalogContext);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            LOG.info(
                    "Cannot get Flink configuration from catalog context. It is possibly due to compatibility with Flink CDC 3.5. Using empty Flink configuration to create catalog instead.");
            flinkConfig = new Configuration();
        }

        FlinkCatalogFactory flinkCatalogFactory =
                FactoryUtil.discoverFactory(
                        FlinkCatalogFactory.class.getClassLoader(),
                        FlinkCatalogFactory.class,
                        FlinkCatalogFactory.IDENTIFIER);
        FlinkCatalog flinkCatalog =
                flinkCatalogFactory.createCatalog(
                        new FactoryUtil.DefaultCatalogContext(
                                "flink-catalog",
                                catalogContext.options().toMap(),
                                flinkConfig,
                                FlinkCatalog.class.getClassLoader()));
        return flinkCatalog.catalog();
    }
}
