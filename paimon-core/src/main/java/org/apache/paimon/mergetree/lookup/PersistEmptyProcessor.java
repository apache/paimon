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

/** A {@link PersistProcessor} to return {@link Boolean} only. */
public class PersistEmptyProcessor implements PersistProcessor<Boolean> {

    private static final byte[] EMPTY_BYTES = new byte[0];

    @Override
    public boolean withPosition() {
        return false;
    }

    @Override
    public byte[] persistToDisk(KeyValue kv) {
        return EMPTY_BYTES;
    }

    @Override
    public Boolean readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        return Boolean.TRUE;
    }

    public static Factory<Boolean> factory() {
        return new Factory<Boolean>() {
            @Override
            public String identifier() {
                return "empty";
            }

            @Override
            public PersistProcessor<Boolean> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistEmptyProcessor();
            }
        };
    }
}
