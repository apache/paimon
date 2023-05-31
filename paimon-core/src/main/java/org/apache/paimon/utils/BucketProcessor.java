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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

/** Processor for bucket, including calculate bucket num, handle non bucket, etc. */
public class BucketProcessor {
    public static final int NON_BUCKET_NUM = -1;
    public static final int NON_BUCKET_BUCKET = 0;

    public static int calculateBucket(BinaryRow bucketKey, int bucketNum) {
        if (checkNonBucket(bucketNum)) {
            return NON_BUCKET_BUCKET;
        } else {
            return KeyAndBucketExtractor.bucket(
                    KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), bucketNum);
        }
    }

    public static int calculateBucket(int hash, int bucketNum) {
        if (checkNonBucket(bucketNum)) {
            return NON_BUCKET_BUCKET;
        } else {
            return KeyAndBucketExtractor.bucket(hash, bucketNum);
        }
    }

    public static boolean checkNonBucket(int bucketNum) {
        return bucketNum == NON_BUCKET_NUM;
    }
}
