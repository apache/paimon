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

package org.apache.flink.table.store.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.catalog.CatalogFactory;
import org.apache.flink.util.Preconditions;

/** Factory to create {@link HiveCatalog}. */
public class HiveCatalogFactory implements CatalogFactory {

    private static final String IDENTIFIER = "hive";

    private static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Uri of Hive metastore's thrift server.");

    private static final ConfigOption<String> WAREHOUSE =
            ConfigOptions.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The warehouse root path of catalog.");

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog create(ReadableConfig options) {
        String uri = checkNotNull(options, URI);
        String warehouse = checkNotNull(options, WAREHOUSE);
        return new HiveCatalog(uri, warehouse);
    }

    private <T> T checkNotNull(ReadableConfig conf, ConfigOption<T> option) {
        return Preconditions.checkNotNull(
                conf.get(option),
                option.key() + " must be set for table store " + IDENTIFIER + " catalog");
    }
}
