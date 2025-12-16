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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.flink.sink.WriterRefresher.configGroups;

/** Writer refresher for dedicated compaction. */
public class CompactWriterRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(CompactWriterRefresher.class);

    private FileStoreTable table;
    private final WriterRefresher.Refresher refresher;
    private final WriterRefresher writerRefresher;

    private CompactWriterRefresher(FileStoreTable table, WriterRefresher.Refresher refresher) {
        this.table = table;
        this.refresher = refresher;
        this.writerRefresher = WriterRefresher.create(true, table, refresher);
    }

    @Nullable
    public static CompactWriterRefresher create(
            boolean isStreaming, FileStoreTable table, WriterRefresher.Refresher refresher) {
        if (!isStreaming) {
            return null;
        }
        return new CompactWriterRefresher(table, refresher);
    }

    /**
     * This is used for dedicated compaction in streaming mode. When the schema-id of newly added
     * data files exceeds the current schema-id, the writer needs to be refreshed to prevent data
     * loss.
     */
    public void tryRefresh(List<DataFileMeta> files) {
        long fileSchemaId =
                files.stream().mapToLong(DataFileMeta::schemaId).max().orElse(table.schema().id());
        if (fileSchemaId > table.schema().id()) {
            Optional<TableSchema> latestSchema = table.schemaManager().latest();
            if (!latestSchema.isPresent()) {
                return;
            }
            TableSchema latest = latestSchema.get();

            try {
                // here we cannot use table.copy(lastestSchema), because table used for
                // dedicated compaction has some dynamic options, we should not overwrite them.
                // we just copy the lastest fields and options allowed to be refreshed.
                table = table.copyWithLatestSchema();

                // refresh configs allowed to be updated by the way
                if (writerRefresher != null) {
                    table =
                            table.copy(
                                    configGroups(
                                            writerRefresher.configGroups(),
                                            CoreOptions.fromMap(latest.options())));
                    writerRefresher.updateTable(table);
                }

                refresher.refresh(table);
                LOG.info(
                        "write has been refreshed due to schema in data files changed. new schema id:{}.",
                        table.schema().id());
            } catch (Exception e) {
                throw new RuntimeException("update write failed.", e);
            }

        } else {
            // try refresh for configs
            if (writerRefresher != null) {
                writerRefresher.tryRefresh();
            }
        }
    }
}
