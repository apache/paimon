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

import org.apache.paimon.data.InternalRow;

/** An counter that sums up {@code long} values. */
public class LongCounter implements SequenceNumberCounter {

    private static final long serialVersionUID = 2L;

    private long value;

    public LongCounter(long value) {
        this.value = value;
    }

    public Long getValue() {
        return this.value;
    }

    @Override
    public void add(InternalRow row) {
        this.value += 1;
    }

    @Override
    public long minSequenceNumber(long recordCount) {
        return this.value - recordCount;
    }

    @Override
    public long maxSequenceNumber() {
        return this.value - 1;
    }

    @Override
    public String toString() {
        return "LongCounter " + this.value;
    }
}
