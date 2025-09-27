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

package org.apache.paimon.predicate;

import java.util.Objects;

/** Limit with direction. */
public class RichLimit {

    private final int limit;
    private final LimitDirection direction;

    public RichLimit(int limit, LimitDirection direction) {
        this.limit = limit;
        this.direction = direction;
    }

    public int limit() {
        return limit;
    }

    public LimitDirection direction() {
        return direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichLimit that = (RichLimit) o;
        return this.limit == that.limit && Objects.equals(this.direction, that.direction);
    }
}
