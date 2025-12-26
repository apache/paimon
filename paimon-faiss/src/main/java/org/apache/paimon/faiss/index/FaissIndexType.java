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

package org.apache.paimon.faiss.index;

/** Enumeration of supported FAISS index types. */
public enum FaissIndexType {

    /** Flat index - exact brute-force search. */
    FLAT("Flat"),

    /** HNSW (Hierarchical Navigable Small World) graph-based index. */
    HNSW("HNSW"),

    /** IVF (Inverted File) index with flat vectors. */
    IVF("IVF"),

    /** IVF-PQ (Inverted File with Product Quantization) index. */
    IVF_PQ("IVF_PQ"),

    /** Unknown index type (e.g., loaded from serialized data). */
    UNKNOWN("Unknown");

    private final String name;

    FaissIndexType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static FaissIndexType fromString(String name) {
        for (FaissIndexType type : values()) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown index type: " + name);
    }
}

