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

package org.apache.paimon.flink.source.shardread;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.table.connector.source.DynamicFilteringData;

import java.util.Objects;

/** {@link SourceSplit} with DynamicPartitionPruning info of file store. */
public class FileStoreSourceSplitWithDpp extends FileStoreSourceSplit {

    private final Projection partitionRowProjection;
    private final DynamicFilteringData dynamicFilteringData;

    private FileStoreSourceSplitWithDpp(
            String id,
            Split split,
            long recordsToSkip,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        super(id, split, recordsToSkip);
        this.partitionRowProjection = partitionRowProjection;
        this.dynamicFilteringData = dynamicFilteringData;
    }

    public static FileStoreSourceSplitWithDpp fromFileStoreSourceSplit(
            FileStoreSourceSplit fileStoreSourceSplit,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        return new FileStoreSourceSplitWithDpp(
                fileStoreSourceSplit.splitId(),
                fileStoreSourceSplit.split(),
                fileStoreSourceSplit.recordsToSkip(),
                partitionRowProjection,
                dynamicFilteringData);
    }

    public Projection getPartitionRowProjection() {
        return partitionRowProjection;
    }

    public DynamicFilteringData getDynamicFilteringData() {
        return dynamicFilteringData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FileStoreSourceSplitWithDpp other = (FileStoreSourceSplitWithDpp) o;
        return partitionRowProjection.equals(other.partitionRowProjection)
                && DynamicFilteringData.isEqual(dynamicFilteringData, other.dynamicFilteringData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partitionRowProjection, dynamicFilteringData);
    }
}
