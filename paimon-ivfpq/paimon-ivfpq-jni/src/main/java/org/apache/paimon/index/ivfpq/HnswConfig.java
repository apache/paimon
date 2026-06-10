// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.paimon.index.ivfpq;

public final class HnswConfig {

    public static final HnswConfig DEFAULT = new HnswConfig(20, 150, 7);

    private final int m;
    private final int efConstruction;
    private final int maxLevel;

    public HnswConfig(int m, int efConstruction, int maxLevel) {
        validatePositive(m, "m");
        validatePositive(efConstruction, "efConstruction");
        validatePositive(maxLevel, "maxLevel");
        this.m = m;
        this.efConstruction = efConstruction;
        this.maxLevel = maxLevel;
    }

    public int m() {
        return m;
    }

    public int efConstruction() {
        return efConstruction;
    }

    public int maxLevel() {
        return maxLevel;
    }

    private static void validatePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }
}
