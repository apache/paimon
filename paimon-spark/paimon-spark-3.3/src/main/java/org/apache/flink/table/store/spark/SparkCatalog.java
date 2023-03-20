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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.SparkCatalogBase;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spark {@link TableCatalog} for table store. */
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

        LOG.info("Time travel target snapshot id is {}.", snapshotId);

        Options dynamicOptions =
                new Options()
                        .set(CoreOptions.SCAN_MODE, CoreOptions.StartupMode.FROM_SNAPSHOT)
                        .set(CoreOptions.SCAN_SNAPSHOT_ID, snapshotId);
        return new SparkTable(
                table.copy(dynamicOptions.toMap()),
                Lock.factory(catalog.lockFactory().orElse(null), toIdentifier(ident)));
    }

    /**
     * NOTE: Time unit of timestamp here is microsecond (see {@link
     * TableCatalog#loadTable(Identifier, long)}). But in SQL you should use seconds.
     */
    @Override
    public SparkTable loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        FileStoreTable table = loadAndCheck(ident);
        // Paimon's timestamp use millisecond
        timestamp = timestamp / 1000;

        LOG.info("Time travel target timestamp is {} milliseconds.", timestamp);

        Options option =
                new Options()
                        .set(CoreOptions.SCAN_MODE, CoreOptions.StartupMode.FROM_TIMESTAMP)
                        .set(CoreOptions.SCAN_TIMESTAMP_MILLIS, timestamp);
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
