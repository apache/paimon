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

package org.apache.paimon.lookup;

/** Strategy for lookup. */
public enum LookupStrategy {
    NO_LOOKUP(false, false),

    CHANGELOG_ONLY(true, false),

    DELETION_VECTOR_ONLY(false, true),

    CHANGELOG_AND_DELETION_VECTOR(true, true);

    public final boolean needLookup;

    public final boolean produceChangelog;

    public final boolean deletionVector;

    LookupStrategy(boolean produceChangelog, boolean deletionVector) {
        this.produceChangelog = produceChangelog;
        this.deletionVector = deletionVector;
        this.needLookup = produceChangelog || deletionVector;
    }

    public static LookupStrategy from(boolean produceChangelog, boolean deletionVector) {
        for (LookupStrategy strategy : values()) {
            if (strategy.produceChangelog == produceChangelog
                    && strategy.deletionVector == deletionVector) {
                return strategy;
            }
        }
        throw new IllegalArgumentException(
                "Invalid combination of produceChangelog : "
                        + produceChangelog
                        + " and deletionVector : "
                        + deletionVector);
    }
}
