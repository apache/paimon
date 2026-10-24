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

package org.apache.paimon.bucket;

import org.apache.paimon.factories.Factory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

/**
 * Factory for a bucket function shared by table writers and predicate pruning.
 *
 * <p>The identifier is persisted as part of the table's physical layout. Implementations must
 * produce the same bucket for the same logical key and bucket count across processes and releases.
 * Incompatible algorithm changes require a new identifier.
 */
public interface BucketFunctionFactory extends Factory {

    /**
     * Creates the bucket function for a table.
     *
     * <p>The function must not retain or modify the supplied key row and must return a bucket in
     * {@code [0, numBuckets)}. A factory can be cached by the runtime; it must create functions
     * without relying on per-call mutable factory state. Returned functions are not assumed to be
     * safe to share between concurrent callers.
     *
     * @return a non-null bucket function
     */
    BucketFunction create(RowType bucketKeyType, Options tableOptions);
}
