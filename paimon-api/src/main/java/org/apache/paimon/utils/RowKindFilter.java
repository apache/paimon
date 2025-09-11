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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

/** A class to filter row kinds. */
public class RowKindFilter {

    private final boolean ignoreAllRetracts;
    private final boolean ignoreUpdateBefore;

    public RowKindFilter(boolean ignoreAllRetracts, boolean ignoreUpdateBefore) {
        this.ignoreAllRetracts = ignoreAllRetracts;
        this.ignoreUpdateBefore = ignoreUpdateBefore;
    }

    @Nullable
    public static RowKindFilter of(CoreOptions options) {
        boolean ignoreAllRetracts = options.ignoreDelete();
        boolean ignoreUpdateBefore = options.ignoreUpdateBefore();
        if (!ignoreAllRetracts && !ignoreUpdateBefore) {
            return null;
        }
        return new RowKindFilter(ignoreAllRetracts, ignoreUpdateBefore);
    }

    public boolean test(RowKind rowKind) {
        switch (rowKind) {
            case DELETE:
                if (ignoreAllRetracts) {
                    return false;
                }
                break;
            case UPDATE_BEFORE:
                if (ignoreUpdateBefore || ignoreAllRetracts) {
                    return false;
                }
                break;
            default:
                break;
        }
        return true;
    }
}
