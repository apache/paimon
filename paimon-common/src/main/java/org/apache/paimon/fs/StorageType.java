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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;

/**
 * Storage tier types for object stores.
 *
 * <p>Represents different storage classes/tiers available in object stores like S3 and OSS. These
 * tiers have different cost and access characteristics:
 *
 * <ul>
 *   <li>Standard: Standard storage with normal access times and costs
 *   <li>Archive: Lower cost storage with longer access times (requires restore)
 *   <li>ColdArchive: Lowest cost storage with longest access times (requires restore)
 * </ul>
 *
 * @since 0.9.0
 */
@Public
public enum StorageType {
    /** Standard storage tier with normal access times and costs. */
    Standard("Standard"),

    /** Archive storage tier with lower costs but longer access times. */
    Archive("Archive"),

    /** Cold archive storage tier with lowest costs but longest access times. */
    ColdArchive("ColdArchive");

    private final String name;

    StorageType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
