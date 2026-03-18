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

package org.apache.paimon.spark.copy;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/** DataFileInfo. */
public class DataFileInfo {

    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final List<DataFileMeta> dataFileMetas;

    public DataFileInfo(
            BinaryRow partition, int bucket, int totalBuckets, List<DataFileMeta> dataFileMetas) {
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.dataFileMetas = dataFileMetas;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public List<DataFileMeta> dataFileMetas() {
        return dataFileMetas;
    }
}
