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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

/** Write request to initial data files for partition and bucket. */
public class ScanCoordinationRequest implements CoordinationRequest {

    private static final long serialVersionUID = 1L;

    private final byte[] partition;
    private final int bucket;
    private final boolean scanDynamicBucketIndex;
    private final boolean scanDeleteVectorsIndex;

    public ScanCoordinationRequest(
            byte[] partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
        this.partition = partition;
        this.bucket = bucket;
        this.scanDynamicBucketIndex = scanDynamicBucketIndex;
        this.scanDeleteVectorsIndex = scanDeleteVectorsIndex;
    }

    public byte[] partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public boolean scanDynamicBucketIndex() {
        return scanDynamicBucketIndex;
    }

    public boolean scanDeleteVectorsIndex() {
        return scanDeleteVectorsIndex;
    }
}
