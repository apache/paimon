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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** {@link ChannelComputer} for {@link CdcMultiplexRecord}. */
public class CdcMultiplexRecordChannelComputer implements ChannelComputer<CdcMultiplexRecord> {

    private static final long serialVersionUID = 1L;

    private static final int TABLE_LOOKUP_MAX_RETRIES = 10;
    private static final long TABLE_LOOKUP_RETRY_INTERVAL_MILLIS = 500L;

    private final CatalogLoader catalogLoader;

    private transient int numChannels;

    private Map<Identifier, CdcRecordChannelComputer> channelComputers;

    public CdcMultiplexRecordChannelComputer(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.channelComputers = new HashMap<>();
    }

    @Override
    public int channel(CdcMultiplexRecord multiplexRecord) {
        ChannelComputer<CdcRecord> channelComputer = computeChannelComputer(multiplexRecord);
        int recordChannel = channelComputer.channel(multiplexRecord.record());
        return mixTableIntoChannel(
                multiplexRecord.databaseName(),
                multiplexRecord.tableName(),
                recordChannel,
                numChannels);
    }

    /**
     * Computes the channel a given bucket is routed to, without needing a record. This mirrors
     * {@link #channel}, so that {@link CdcRecordStoreMultiWriteOperator} can decide which subtask
     * owns the state of a bucket.
     */
    static int computeChannel(
            String databaseName,
            String tableName,
            BinaryRow partition,
            int bucket,
            int numChannels) {
        return mixTableIntoChannel(
                databaseName,
                tableName,
                ChannelComputer.select(partition, bucket, numChannels),
                numChannels);
    }

    /** Offsets the per-table channel by the table identity, so that tables are spread out. */
    private static int mixTableIntoChannel(
            String databaseName, String tableName, int recordChannel, int numChannels) {
        return Math.floorMod(Objects.hash(databaseName, tableName) + recordChannel, numChannels);
    }

    private ChannelComputer<CdcRecord> computeChannelComputer(CdcMultiplexRecord record) {
        return channelComputers.computeIfAbsent(
                Identifier.create(record.databaseName(), record.tableName()),
                id -> {
                    try (Catalog catalog = catalogLoader.load()) {
                        FileStoreTable table = getTable(catalog, id);
                        if (table.bucketMode() != BucketMode.HASH_FIXED) {
                            throw new UnsupportedOperationException(
                                    String.format(
                                            "Combine mode Sink only supports FIXED bucket mode, but %s is %s",
                                            table.name(), table.bucketMode()));
                        }

                        CdcRecordChannelComputer channelComputer =
                                new CdcRecordChannelComputer(table.schema());
                        channelComputer.setup(numChannels);
                        return channelComputer;
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private FileStoreTable getTable(Catalog catalog, Identifier tableId) {
        Catalog.TableNotExistException lastException = null;
        for (int retry = 0; retry <= TABLE_LOOKUP_MAX_RETRIES; retry++) {
            try {
                return (FileStoreTable) catalog.getTable(tableId);
            } catch (Catalog.TableNotExistException e) {
                lastException = e;
                // Records of a newly added table can arrive before the table is visible here. Do
                // not use a temporary channel: the writer-state restore filter must calculate the
                // exact same owner from the real partition and bucket.
                if (retry == TABLE_LOOKUP_MAX_RETRIES) {
                    break;
                }
                try {
                    Thread.sleep(TABLE_LOOKUP_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                            "Interrupted while waiting for table " + tableId.getFullName(),
                            interrupted);
                }
            }
        }
        throw new RuntimeException(
                String.format(
                        "Table %s is still unavailable after %s retries (%s ms total wait).",
                        tableId.getFullName(),
                        TABLE_LOOKUP_MAX_RETRIES,
                        TABLE_LOOKUP_MAX_RETRIES * TABLE_LOOKUP_RETRY_INTERVAL_MILLIS),
                lastException);
    }

    @Override
    public String toString() {
        return "shuffle by bucket";
    }
}
