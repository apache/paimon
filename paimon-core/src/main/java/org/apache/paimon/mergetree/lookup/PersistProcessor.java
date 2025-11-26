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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/** Processor to process value. */
public interface PersistProcessor<T> {

    boolean withPosition();

    byte[] persistToDisk(KeyValue kv);

    default byte[] persistToDisk(KeyValue kv, long rowPosition) {
        throw new UnsupportedOperationException();
    }

    T readFromDisk(InternalRow key, int level, byte[] valueBytes, String fileName);

    /** Factory to create {@link PersistProcessor}. */
    interface Factory<T> {

        String identifier();

        PersistProcessor<T> create(
                String fileSerVersion,
                LookupSerializerFactory serializerFactory,
                @Nullable RowType fileSchema);
    }
}
