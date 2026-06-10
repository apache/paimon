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

package org.apache.paimon.vector.index;

/** Enumeration of supported vector distance metrics. */
public enum VectorMetric {
    L2("l2"),
    COSINE("cosine"),
    INNER_PRODUCT("inner_product");

    private final String configName;

    VectorMetric(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }

    public static VectorMetric fromConfigName(String configName) {
        for (VectorMetric m : values()) {
            if (m.configName.equals(configName)) {
                return m;
            }
        }
        throw new IllegalArgumentException("Unknown metric: " + configName);
    }
}
