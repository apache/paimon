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

package org.apache.paimon.table;

import org.apache.paimon.annotation.Experimental;

/**
 * Bucket mode of the table, it affects the writing process and also affects the bucket skipping in
 * reading.
 *
 * @since 0.9
 */
@Experimental
public enum BucketMode {

    /**
     * The fixed number of buckets configured by the user can only be modified through offline
     * commands. The data is distributed to the corresponding buckets according to the hash value of
     * bucket key (default is primary key), and the reading end can perform bucket skipping based on
     * the filtering conditions of the bucket key.
     */
    HASH_FIXED,

    /**
     * The dynamic bucket mode records which bucket the key corresponds to through the index files.
     * The index records the correspondence between the hash value of the primary-key and the
     * bucket. This mode cannot support multiple concurrent writes or bucket skipping for reading
     * filter conditions. This mode only works for changelog table.
     */
    HASH_DYNAMIC,

    /**
     * The cross partition mode is for cross partition upsert (primary keys not contain all
     * partition fields). It directly maintains the mapping of primary keys to partition and bucket,
     * uses local disks, and initializes indexes by reading all existing keys in the table when
     * starting stream write job.
     */
    CROSS_PARTITION,

    /**
     * Ignoring bucket concept, although all data is written to bucket-0, the parallelism of reads
     * and writes is unrestricted. This mode only works for append-only table.
     */
    BUCKET_UNAWARE;

    public static final int UNAWARE_BUCKET = 0;
}
