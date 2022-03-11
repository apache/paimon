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

package org.apache.flink.table.store.connector.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.connector.TableStoreFactoryOptions;
import org.apache.flink.table.store.log.LogStoreTableFactory;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.CHANGE_TRACKING;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_PATH;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.apache.flink.table.store.log.LogStoreTableFactory.discoverLogStoreFactory;

/** Utils for {@link org.apache.flink.table.store.connector.TableStoreFactory}. */
public class TableStoreUtils {

    public static Map<String, String> filterLogStoreOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(LOG_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().substring(LOG_PREFIX.length()),
                                Map.Entry::getValue));
    }

    public static Map<String, String> filterFileStoreOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(LOG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static DynamicTableFactory.Context createLogStoreContext(
            DynamicTableFactory.Context context) {
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                context.getCatalogTable()
                        .copy(filterLogStoreOptions(context.getCatalogTable().getOptions())),
                filterLogStoreOptions(context.getEnrichmentOptions()),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    public static LogStoreTableFactory createLogStoreTableFactory() {
        return discoverLogStoreFactory(
                Thread.currentThread().getContextClassLoader(),
                TableStoreFactoryOptions.LOG_SYSTEM.defaultValue());
    }

    public static Path tablePath(Map<String, String> options, ObjectIdentifier identifier) {
        return new Path(
                new Path(options.get(FILE_PATH.key())),
                String.format(
                        "root/%s.catalog/%s.db/%s",
                        identifier.getCatalogName(),
                        identifier.getDatabaseName(),
                        identifier.getObjectName()));
    }

    public static boolean enableChangeTracking(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(
                        CHANGE_TRACKING.key(), CHANGE_TRACKING.defaultValue().toString()));
    }
}
