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

package org.apache.paimon.table.query;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.paimon.utils.Preconditions.checkArgument;

public class TableQueryImpl implements TableQuery {

    private final KeyValueFileStoreWrite write;

    private final Map<BinaryRow, Map<Integer, LookupLevels>> lookups;

    public TableQueryImpl(KeyValueFileStoreWrite write) {
        this.write = write;
        this.lookups = new HashMap<>();
    }

    @Override
    public void refreshFiles(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        Map<Integer, LookupLevels> buckets = lookups.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            lookups.put(partition.copy(), buckets);
        }

        LookupLevels lookup = buckets.get(bucket);
        if (lookup == null) {
            checkArgument(beforeFiles.isEmpty());
            lookup = write.createLookupLevels(partition, bucket, dataFiles);
            buckets.put(bucket, lookup);
        } else {
            lookup.levels().update(beforeFiles, dataFiles);
        }
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        Map<Integer, LookupLevels> map = lookups.get(partition);
        if (map == null) {
            return null;
        }

        LookupLevels lookup = map.get(bucket);
        if (lookup == null) {
            return null;
        }

        KeyValue kv = lookup.lookup(key, 0);
        if (kv == null) {
            return null;
        }

        return kv.value();
    }

    @Override
    public void close() throws IOException {
        Stream<LookupLevels> lookups =
                this.lookups.values().stream().map(Map::values).flatMap(Collection::stream);
        lookups.forEach(IOUtils::closeQuietly);
        this.lookups.clear();
    }
}
