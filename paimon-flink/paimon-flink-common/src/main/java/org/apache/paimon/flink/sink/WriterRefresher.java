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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS;
import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS;
import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** Writer refresher for refresh write when configs changed. */
public class WriterRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(WriterRefresher.class);

    private FileStoreTable table;
    private final Refresher refresher;
    private final Set<String> configGroups;

    private WriterRefresher(FileStoreTable table, Refresher refresher, Set<String> configGroups) {
        this.table = table;
        this.refresher = refresher;
        this.configGroups = configGroups;
    }

    @Nullable
    public static WriterRefresher create(
            boolean isStreaming, FileStoreTable table, Refresher refresher) {
        if (!isStreaming) {
            return null;
        }

        String refreshDetectors =
                Options.fromMap(table.options())
                        .get(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS);
        Set<String> configGroups =
                isNullOrWhitespaceOnly(refreshDetectors)
                        ? null
                        : Arrays.stream(refreshDetectors.split(",")).collect(Collectors.toSet());
        if (configGroups == null || configGroups.isEmpty()) {
            return null;
        }
        return new WriterRefresher(table, refresher, configGroups);
    }

    public void tryRefresh() {
        Optional<TableSchema> latestSchema = table.schemaManager().latest();
        if (!latestSchema.isPresent()) {
            return;
        }

        TableSchema latest = latestSchema.get();
        if (latest.id() > table.schema().id()) {
            try {
                Map<String, String> currentOptions =
                        configGroups(configGroups, table.coreOptions());
                Map<String, String> newOptions =
                        configGroups(configGroups, CoreOptions.fromMap(latest.options()));

                if (!Objects.equals(newOptions, currentOptions)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "table schema has changed, current schema-id:{}, try to update write with new schema-id:{}. "
                                        + "current options:{}, new options:{}.",
                                table.schema().id(),
                                latestSchema.get().id(),
                                currentOptions,
                                newOptions);
                    }
                    table = table.copy(newOptions);
                    refresher.refresh(table);
                }
            } catch (Exception e) {
                throw new RuntimeException("update write failed.", e);
            }
        }
    }

    /** Refresher when configs changed. */
    public interface Refresher {
        void refresh(FileStoreTable table) throws Exception;
    }

    public static Map<String, String> configGroups(Set<String> groups, CoreOptions options) {
        Map<String, String> configs = new HashMap<>();
        // external-paths config group
        String externalPaths = "external-paths";
        if (groups.contains(externalPaths)) {
            configs.put(DATA_FILE_EXTERNAL_PATHS.key(), options.dataFileExternalPaths());
            configs.put(
                    DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                    options.externalPathStrategy().toString());
            configs.put(DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(), options.externalSpecificFS());
        }
        return configs;
    }
}
