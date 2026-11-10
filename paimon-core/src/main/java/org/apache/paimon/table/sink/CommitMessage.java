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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Commit message for partition and bucket.
 *
 * @since 0.4.0
 */
@Public
public interface CommitMessage extends Serializable {

    /** Partition of this commit message. */
    BinaryRow partition();

    /** Bucket of this commit message. */
    int bucket();

    /** Total number of buckets in this partition. */
    @Nullable
    Integer totalBuckets();

    /**
     * Writer's base snapshot, or the operation's read snapshot for a Data Evolution row-id conflict
     * check.
     */
    @Nullable
    default Long checkFromSnapshot() {
        return null;
    }

    /** Ordering-history generation observed when the bucket writer restored its inputs. */
    @Nullable
    default String deleteOrderingGuardGeneration() {
        return null;
    }

    /** Guard file names observed by the bucket writer before its pending compactions. */
    @Nullable
    default List<String> deleteOrderingGuardSources() {
        return null;
    }
}
