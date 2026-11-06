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

package org.apache.paimon.index;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Ordering metadata of an explicit retract, without its value payload.
 *
 * <p>The ordering row contains only the source sequence and sequence-group ordering fields. The
 * schema that produced the guard defines their comparison and whole-row deletion semantics.
 * Internal sequence numbers, input levels, and retract kinds are retained independently of those
 * fields. Guards must not be exposed as ordinary data or changelog records.
 */
public class DeleteOrderingGuard {

    private final InternalRow key;
    private final InternalRow ordering;
    private final long sequenceNumber;
    private final RowKind valueKind;
    private final int level;

    public DeleteOrderingGuard(
            InternalRow key,
            InternalRow ordering,
            long sequenceNumber,
            RowKind valueKind,
            int level) {
        checkArgument(valueKind.isRetract(), "An ordering guard must describe a retract.");
        this.key = key;
        this.ordering = ordering;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.level = level;
    }

    public InternalRow key() {
        return key;
    }

    public InternalRow ordering() {
        return ordering;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public RowKind valueKind() {
        return valueKind;
    }

    public int level() {
        return level;
    }
}
