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

package org.apache.paimon.lumina.index;

/** Enumeration of supported Lumina index types. */
public enum LuminaIndexType {

    /** Brute-force exact search (flat index). */
    BRUTEFORCE("bruteforce"),

    /** HNSW graph-based approximate nearest neighbor search. */
    HNSW("hnsw"),

    /** IVF (Inverted File) index with clustering. */
    IVF("ivf"),

    /** DiskANN graph-based index optimized for disk-resident data. */
    DISKANN("diskann"),

    /** Unknown index type (e.g., loaded from serialized data). */
    UNKNOWN("unknown");

    private final String luminaName;

    LuminaIndexType(String luminaName) {
        this.luminaName = luminaName;
    }

    /** Get the Lumina native option string for this index type. */
    public String getLuminaName() {
        return luminaName;
    }

    public static LuminaIndexType fromString(String name) {
        for (LuminaIndexType type : values()) {
            if (type.luminaName.equalsIgnoreCase(name) || type.name().equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown index type: " + name);
    }
}
