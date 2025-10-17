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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.ChainMergeFileSplitRead;
import org.apache.paimon.operation.ChainRawFileSplitRead;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.Comparator;

/** Factory to scan data table. */
public class ScanFactory {

    public static DataTableBatchScan createBatchDataTableScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        if (ChainTableUtils.isScanFallbackChainRead(options.toMap())) {
            return new IdempotentReuseDataTableBatchScan(
                    schema, schemaManager, options, snapshotReader, queryAuth);
        }
        return new DataTableBatchScan(schema, schemaManager, options, snapshotReader, queryAuth);
    }

    public static MergeFileSplitRead createMergeFileSplitRead(
            CoreOptions options,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder) {
        MergeFileSplitRead fileSplitRead =
                new MergeFileSplitRead(
                        options,
                        schema,
                        keyType,
                        valueType,
                        keyComparator,
                        mfFactory,
                        readerFactoryBuilder);
        if (ChainTableUtils.isScanFallbackChainRead(options.toMap())) {
            return new ChainMergeFileSplitRead(fileSplitRead);
        }
        return fileSplitRead;
    }

    public static RawFileSplitRead createBatchRawFileRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType valueType,
            FileFormatDiscover fileFormatDiscover,
            FileStorePathFactory fileStorePathFactory,
            CoreOptions options,
            boolean rowTrackingEnabled) {
        RawFileSplitRead rawFileSplitRead =
                new RawFileSplitRead(
                        fileIO,
                        schemaManager,
                        schema,
                        valueType,
                        fileFormatDiscover,
                        fileStorePathFactory,
                        options.fileIndexReadEnabled(),
                        rowTrackingEnabled);
        if (ChainTableUtils.isScanFallbackChainRead(options.toMap())) {
            return new ChainRawFileSplitRead(rawFileSplitRead);
        }
        return rawFileSplitRead;
    }
}
