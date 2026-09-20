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

import javax.annotation.Nullable;

/** Signals that an index supports a lookup but abandoned it after exceeding its query budget. */
public class GlobalIndexLookupDeclinedException extends RuntimeException {

    public GlobalIndexLookupDeclinedException(String message) {
        super(message);
    }

    @Nullable
    public static GlobalIndexLookupDeclinedException find(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof GlobalIndexLookupDeclinedException) {
                return (GlobalIndexLookupDeclinedException) current;
            }
            current = current.getCause();
        }
        return null;
    }
}
