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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;

/** A lookup table which provides get and refresh. */
public interface LookupTable {

    List<InternalRow> get(InternalRow key) throws IOException;

    static FullCacheLookupTable create(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> primaryKey,
            List<String> joinKey,
            Predicate<InternalRow> recordFilter,
            long lruCacheSize)
            throws IOException {
        if (primaryKey.isEmpty()) {
            return new NoPrimaryKeyLookupTable(
                    stateFactory, rowType, joinKey, recordFilter, lruCacheSize);
        } else {
            if (new HashSet<>(primaryKey).equals(new HashSet<>(joinKey))) {
                return new PrimaryKeyLookupTable(
                        stateFactory, rowType, joinKey, recordFilter, lruCacheSize);
            } else {
                return new SecondaryIndexLookupTable(
                        stateFactory, rowType, primaryKey, joinKey, recordFilter, lruCacheSize);
            }
        }
    }

    static PartialCacheLookupTable create(
            Table table, int[] projection, List<String> joinKey, File tempPath) {
        validatePartialCacheMode(table, joinKey);
        return new PrimaryKeyPartialLookupTable((FileStoreTable) table, projection, tempPath);
    }

    static void validatePartialCacheMode(Table table, List<String> joinKey) {
        CoreOptions options = new CoreOptions(table.options());
        if (options.toConfiguration().get(LOOKUP_CACHE_MODE) != LookupCacheMode.PARTIAL) {
            return;
        }
        List<String> primaryKey = table.primaryKeys();
        if (!new HashSet<>(primaryKey).equals(new HashSet<>(joinKey))) {
            throw new UnsupportedOperationException(
                    String.format(
                            "For partial cache mode, the join key: %s must equal to the primary key: %s",
                            joinKey, primaryKey));
        }

        if (table.partitionKeys().size() > 0) {
            throw new UnsupportedOperationException(
                    "The partitioned table are not supported in partial cache mode.");
        }

        if (new CoreOptions(table.options()).bucket() <= 0) {
            throw new UnsupportedOperationException(
                    "Only support fixed bucket mode in partial cache mode.");
        }

        if (options.changelogProducer() != CoreOptions.ChangelogProducer.LOOKUP) {
            if (options.sequenceField().isPresent()) {
                throw new UnsupportedOperationException(
                        "Not support sequence field definition, but is: "
                                + options.sequenceField().get());
            }

            if (options.mergeEngine() != DEDUPLICATE) {
                throw new UnsupportedOperationException(
                        "Only support deduplicate merge engine, but is: " + options.mergeEngine());
            }
        }
    }
}
