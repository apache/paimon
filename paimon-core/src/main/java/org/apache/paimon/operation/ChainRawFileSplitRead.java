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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.CoreOptions.SCAN_FALLBACK_DELTA_BRANCH;
import static org.apache.paimon.CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH;

/** A specified {@link RawFileSplitRead} for chain table. */
public class ChainRawFileSplitRead extends RawFileSplitRead {

    private static final Logger LOG = LoggerFactory.getLogger(ChainRawFileSplitRead.class);

    private final String snapshotBranch;
    private final String deltaBranch;
    private final SchemaManager snapshotSchemaManager;
    private final SchemaManager deltaSchemaManager;
    private final String currentBranch;

    public ChainRawFileSplitRead(RawFileSplitRead rawFileSplitRead) {
        super(rawFileSplitRead);
        this.currentBranch = super.schema().options().get(BRANCH.key());
        this.snapshotBranch = super.schema().options().get(SCAN_FALLBACK_SNAPSHOT_BRANCH.key());
        this.deltaBranch = super.schema().options().get(SCAN_FALLBACK_DELTA_BRANCH.key());
        this.snapshotSchemaManager =
                snapshotBranch.equalsIgnoreCase(currentBranch)
                        ? super.schemaManager()
                        : super.schemaManager().copyWithBranch(snapshotBranch);
        this.deltaSchemaManager =
                deltaBranch.equalsIgnoreCase(currentBranch)
                        ? super.schemaManager()
                        : super.schemaManager().copyWithBranch(deltaBranch);
    }

    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        if (!split.beforeFiles().isEmpty()) {
            LOG.info("Ignore split before files: {}", split.beforeFiles());
        }
        return createReader(
                split.partition(),
                split.bucket(),
                split.dataFiles(),
                createDvFactories(split),
                split);
    }

    @Override
    protected BinaryRow getReadPartition(BinaryRow partition, DataSplit dataSplit) {
        return dataSplit.readPartition();
    }

    @Override
    protected SchemaManager getSchemaManager(DataSplit dataSplit, String fileName) {
        if (snapshotBranch.equalsIgnoreCase(dataSplit.fileBranchMapping().get(fileName))) {
            return snapshotSchemaManager;
        } else {
            return deltaSchemaManager;
        }
    }

    @Override
    public TableSchema getDataSchema(DataSplit dataSplit, DataFileMeta fileMeta) {
        if (currentBranch.equalsIgnoreCase(
                dataSplit.fileBranchMapping().get(fileMeta.fileName()))) {
            super.getDataSchema(dataSplit, fileMeta);
        }
        return getSchemaManager(dataSplit, fileMeta.fileName()).schema(fileMeta.schemaId());
    }
}
