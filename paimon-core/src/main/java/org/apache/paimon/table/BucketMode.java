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

/**
 * Bucket mode of the table, it affects the writing process and also affects the data skipping in
 * reading.
 */
public enum BucketMode {

    /**
     * The fixed number of buckets configured by the user can only be modified through offline
     * commands. The data is distributed to the corresponding buckets according to bucket key
     * (default is primary key), and the reading end can perform data skipping based on the
     * filtering conditions of the bucket key.
     */
    FIXED,

    /**
     * The Dynamic bucket mode records which bucket the key corresponds to through the index files.
     * This mode cannot support multiple concurrent writes or data skipping for reading filter
     * conditions. This mode only works for changelog table.
     */
    DYNAMIC,

    /**
     * Ignoring buckets can be equivalent to understanding that all data enters the global bucket,
     * and data is randomly written to the table. The data in the bucket has no order relationship
     * at all. This mode only works for append-only table.
     */
    UNAWARE
}
