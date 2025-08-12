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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Writer refresher for refresh write when configs changed. */
public class WriterRefresher<T> {

    private static final Logger LOG = LoggerFactory.getLogger(WriterRefresher.class);

    @Nullable private final Set<String> configGroups;
    private final Refresher<T> refresher;
    private FileStoreTable table;
    private T write;

    public WriterRefresher(FileStoreTable table, T write, Refresher<T> refresher) {
        this.table = table;
        this.write = write;
        this.refresher = refresher;
        String refreshDetectors =
                Options.fromMap(table.options())
                        .get(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS);
        if (refreshDetectors == null) {
            configGroups = null;
        } else {
            configGroups = Arrays.stream(refreshDetectors.split(",")).collect(Collectors.toSet());
        }
    }

    public void tryRefresh() {
        if (configGroups == null) {
            return;
        }

        Optional<TableSchema> latestSchema = table.schemaManager().latest();
        if (latestSchema.isPresent() && latestSchema.get().id() > table.schema().id()) {
            try {
                Map<String, String> currentOptions =
                        CoreOptions.fromMap(table.schema().options()).configGroups(configGroups);
                Map<String, String> newOptions =
                        CoreOptions.fromMap(latestSchema.get().options())
                                .configGroups(configGroups);

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
                    refresher.refresh(table, write);
                }
            } catch (Exception e) {
                throw new RuntimeException("update write failed.", e);
            }
        }
    }

    public FileStoreTable updatedTable() {
        return table;
    }

    /**
     * Refresher for refresh write when configs changed.
     *
     * @param <T> the type of writer.
     */
    public interface Refresher<T> {
        void refresh(FileStoreTable table, T writer) throws Exception;
    }
}
