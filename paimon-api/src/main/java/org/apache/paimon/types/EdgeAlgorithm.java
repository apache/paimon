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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

import java.util.Locale;

/** Algorithm used to interpolate geography edges. */
@Public
public enum EdgeAlgorithm {
    SPHERICAL,
    VINCENTY,
    THOMAS,
    ANDOYER,
    KARNEY;

    public static EdgeAlgorithm fromName(String algorithmName) {
        if (algorithmName == null) {
            throw new IllegalArgumentException("Invalid edge interpolation algorithm: null");
        }

        try {
            return valueOf(algorithmName.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid edge interpolation algorithm: " + algorithmName, e);
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
