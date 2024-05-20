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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryRow;

/** Assigner a bucket for a record, just used in dynamic bucket table. */
public interface BucketAssigner {

    int assign(BinaryRow partition, int hash);

    void prepareCommit(long commitIdentifier);

    static boolean isMyBucket(int bucket, int numAssigners, int assignId) {
        return bucket % numAssigners == assignId % numAssigners;
    }

    static int computeHashKey(int partitionHash, int keyHash, int numChannels, int numAssigners) {
        int start = Math.abs(partitionHash % numChannels);
        int id = Math.abs(keyHash % numAssigners);
        return start + id;
    }

    static int computeAssigner(int partitionHash, int keyHash, int numChannels, int numAssigners) {
        return computeHashKey(partitionHash, keyHash, numChannels, numAssigners) % numChannels;
    }
}
