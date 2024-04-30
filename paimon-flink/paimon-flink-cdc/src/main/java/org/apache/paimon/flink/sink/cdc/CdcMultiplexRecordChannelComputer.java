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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** {@link ChannelComputer} for {@link CdcMultiplexRecord}. */
public class CdcMultiplexRecordChannelComputer implements ChannelComputer<CdcMultiplexRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CdcMultiplexRecordChannelComputer.class);

    private static final long serialVersionUID = 1L;
    private final Catalog.Loader catalogLoader;

    private transient int numChannels;

    private Map<Identifier, CdcRecordChannelComputer> channelComputers;
    private Catalog catalog;

    public CdcMultiplexRecordChannelComputer(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.catalog = catalogLoader.load();
        this.channelComputers = new HashMap<>();
    }

    @Override
    public int channel(CdcMultiplexRecord multiplexRecord) {
        ChannelComputer<CdcRecord> channelComputer = computeChannelComputer(multiplexRecord);
        int recordChannel =
                channelComputer != null ? channelComputer.channel(multiplexRecord.record()) : 0;
        return Math.floorMod(
                Objects.hash(multiplexRecord.databaseName(), multiplexRecord.tableName())
                        + recordChannel,
                numChannels);
    }

    private ChannelComputer<CdcRecord> computeChannelComputer(CdcMultiplexRecord record) {
        return channelComputers.computeIfAbsent(
                Identifier.create(record.databaseName(), record.tableName()),
                id -> {
                    FileStoreTable table;
                    try {
                        table = (FileStoreTable) catalog.getTable(id);
                    } catch (Catalog.TableNotExistException e) {
                        LOG.error("Failed to get table " + id.getFullName());
                        return null;
                    }

                    if (table.bucketMode() != BucketMode.FIXED) {
                        throw new UnsupportedOperationException(
                                "Combine mode Sink only supports FIXED bucket mode, but is "
                                        + table.bucketMode());
                    }

                    CdcRecordChannelComputer channelComputer =
                            new CdcRecordChannelComputer(table.schema());
                    channelComputer.setup(numChannels);
                    return channelComputer;
                });
    }

    @Override
    public String toString() {
        return "shuffle by bucket";
    }
}
