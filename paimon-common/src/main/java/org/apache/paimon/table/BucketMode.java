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
     * Hash-Dynamic mode records the correspondence between the hash of the primary key and the
     * bucket number. It is used to simplify the distribution of primary keys to buckets, but cannot
     * support large amounts of data. It cannot support multiple concurrent writes or bucket
     * skipping for reading filter. This mode only works for primary key table.
     */
    HASH_DYNAMIC,

    /**
     * Key-Dynamic mode records the correspondence between the primary key and the partition +
     * bucket number. It is used to cross partition upsert (primary keys not contain all partition
     * fields). It directly maintains the mapping of primary keys to partition and bucket using
     * local disks, and initializes indexes by reading all existing keys in the table when starting
     * write job.
     */
    KEY_DYNAMIC,

    /**
     * Ignoring bucket concept, although all data is written to bucket-0, the parallelism of reads
     * and writes is unrestricted. This mode only works for append-only table.
     */
    BUCKET_UNAWARE,

    /**
     * Configured by 'bucket' = '-2' (postpone bucket) for primary key table. This mode aims to
     * solve the difficulty to determine a fixed number of buckets and support different buckets for
     * different partitions. The bucket will be adaptively adjusted to the appropriate value in the
     * background.
     */
    POSTPONE_MODE;

    public static final int UNAWARE_BUCKET = 0;

    public static final int POSTPONE_BUCKET = -2;
}
