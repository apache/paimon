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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Lookup table for primary key which supports to read the LSM tree directly. */
public class PrimaryKeyPartialLookupTable implements PartialCacheLookupTable {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyPartialLookupTable.class);

    private final FixedBucketKeyExtractor extractor;

    private final TableQuery tableQuery;

    public PrimaryKeyPartialLookupTable(FileStoreTable table, int[] projection, File tempPath) {
        this.tableQuery =
                table.newTableQuery()
                        .withValueProjection(Projection.of(projection).toNestedIndexes())
                        .withIOManager(new IOManagerImpl(tempPath.toString()));
        this.extractor = new FixedBucketKeyExtractor(table.schema());
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        extractor.setRecord(key);
        int bucket = extractor.bucket();
        BinaryRow partition = extractor.partition();

        InternalRow kv = tableQuery.lookup(partition, bucket, key);
        if (kv == null) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(kv);
        }
    }

    @Override
    public void refresh(List<Split> splits) {
        for (Split split : splits) {
            if (!(split instanceof DataSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            BinaryRow partition = ((DataSplit) split).partition();
            int bucket = ((DataSplit) split).bucket();
            List<DataFileMeta> before = ((DataSplit) split).beforeFiles();
            List<DataFileMeta> after = ((DataSplit) split).dataFiles();

            tableQuery.refreshFiles(partition, bucket, before, after);
        }
    }
}
