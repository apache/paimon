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

import java.io.Serializable;

/** The information of copy data file. */
public class CopyFileInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sourceFilePath;

    private final String targetFilePath;

    private final byte[] partition;

    private final int bucket;

    private final int totalBuckets;

    private final byte[] dataFileMeta;

    public CopyFileInfo(
            String sourceFilePath,
            String targetFilePath,
            byte[] partition,
            int bucket,
            byte[] dataFileMeta) {
        this.sourceFilePath = sourceFilePath;
        this.targetFilePath = targetFilePath;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = 0;
        this.dataFileMeta = dataFileMeta;
    }

    public CopyFileInfo(
            String sourceFilePath,
            String targetFilePath,
            byte[] partition,
            int bucket,
            int totalBuckets,
            byte[] dataFileMeta) {
        this.sourceFilePath = sourceFilePath;
        this.targetFilePath = targetFilePath;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.dataFileMeta = dataFileMeta;
    }

    public String sourceFilePath() {
        return sourceFilePath;
    }

    public String targetFilePath() {
        return targetFilePath;
    }

    public byte[] partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public byte[] dataFileMeta() {
        return dataFileMeta;
    }
}
