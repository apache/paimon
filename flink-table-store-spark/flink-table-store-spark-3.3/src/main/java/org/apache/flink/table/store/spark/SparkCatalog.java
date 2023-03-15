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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.Table;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark {@link TableCatalog} for table store.
 *
 * <p>NOTE: If table exists but target snapshot for time travel does not, don't throw {@link
 * NoSuchTableException} because Spark will throw misleading exception stack.
 */
public class SparkCatalog extends SparkCatalogBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCatalog.class);

    @Override
    public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
        Table table = loadAndCheck(ident);
        long snapshotId;

        try {
            snapshotId = Long.parseUnsignedLong(version);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Version for time travel should be a LONG value representing snapshot id but was '%s'.",
                            version),
                    e);
        }

        Options option = new Options().set(CoreOptions.TIME_TRAVEL_SNAPSHOT_ID, snapshotId);
        return new SparkTable(
                table.copy(option.toMap()),
                Lock.factory(catalog.lockFactory().orElse(null), toIdentifier(ident)));
    }

    /**
     * NOTE: Time unit of timestamp here is microsecond (see {@link
     * TableCatalog#loadTable(Identifier, long)}). But in SQL you should use seconds.
     */
    @Override
    public SparkTable loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        FileStoreTable table = loadAndCheck(ident);
        SnapshotManager snapshotManager = table.snapshotManager();
        // Paimon's snapshot use millisecond
        timestamp = timestamp / 1000;
        Long snapshotId = snapshotManager.earlierOrEqualTimeMills(timestamp);

        if (snapshotId == null) {
            LOG.info("Time travel target snapshot id for timestamp '{}' doesn't exist.", timestamp);
            throw new RuntimeException(
                    String.format(
                            "Time travel target snapshot id for timestamp '%s' doesn't exist.",
                            timestamp));
        }

        LOG.info(
                "Time travel target snapshot id is '{}' (for timestamp '{}').",
                snapshotId,
                timestamp);

        Options option = new Options().set(CoreOptions.TIME_TRAVEL_SNAPSHOT_ID, snapshotId);
        return new SparkTable(
                table.copy(option.toMap()),
                Lock.factory(catalog.lockFactory().orElse(null), toIdentifier(ident)));
    }

    private FileStoreTable loadAndCheck(Identifier ident) throws NoSuchTableException {
        try {
            Table table = load(ident);
            if (!(table instanceof FileStoreTable)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Only FileStoreTable supports time travel but given table type is '%s'.",
                                table.getClass().getName()));
            }
            return (FileStoreTable) table;
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
        }
    }
}
