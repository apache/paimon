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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Common interface for partition expiration strategies.
 *
 * <p>Implementations include {@link NormalPartitionExpire} for normal tables and {@link
 * ChainTablePartitionExpire} for chain tables that require segment-based expiration across snapshot
 * and delta branches.
 */
public interface PartitionExpire {

    /**
     * Expire partitions that are older than the configured expiration time.
     *
     * @return the list of expired partition specs, or null if the check interval has not elapsed
     */
    @Nullable
    List<Map<String, String>> expire(long commitIdentifier);

    /** Whether this expiration uses values-time strategy. */
    boolean isValueExpiration();

    /**
     * Check whether all given partitions are expired according to the values-time strategy.
     *
     * <p>Only valid when {@link #isValueExpiration()} returns true.
     */
    boolean isValueAllExpired(Collection<BinaryRow> partitions);
}
