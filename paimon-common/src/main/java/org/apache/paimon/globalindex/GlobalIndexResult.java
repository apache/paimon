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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.RoaringNavigableMap64;

/** Global index result represents row ids as a compressed bitmap. */
public interface GlobalIndexResult {

    /** Returns the bitmap representing row ids. */
    RoaringNavigableMap64 results();

    static GlobalIndexResult createEmpty() {
        return RoaringNavigableMap64::new;
    }

    /**
     * Returns the intersection of this result and the other result.
     *
     * <p>Uses native bitmap AND operation for optimal performance.
     */
    default GlobalIndexResult and(GlobalIndexResult other) {
        return () -> RoaringNavigableMap64.and(this.results(), other.results());
    }

    /**
     * Returns the union of this result and the other result.
     *
     * <p>Uses native bitmap OR operation for optimal performance.
     */
    default GlobalIndexResult or(GlobalIndexResult other) {
        return () -> RoaringNavigableMap64.or(this.results(), other.results());
    }
}
