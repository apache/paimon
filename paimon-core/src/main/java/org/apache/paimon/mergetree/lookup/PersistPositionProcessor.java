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

import java.util.Arrays;

import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_LONG_SIZE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeLong;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/** A {@link PersistProcessor} to return {@link FilePosition}. */
public class PersistPositionProcessor implements PersistProcessor<FilePosition> {

    @Override
    public boolean withPosition() {
        return true;
    }

    @Override
    public byte[] persistToDisk(KeyValue kv) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] persistToDisk(KeyValue kv, long rowPosition) {
        byte[] bytes = new byte[MAX_VAR_LONG_SIZE];
        int len = encodeLong(bytes, rowPosition);
        return Arrays.copyOf(bytes, len);
    }

    @Override
    public FilePosition readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        long rowPosition = decodeLong(bytes, 0);
        return new FilePosition(fileName, rowPosition);
    }

    public static Factory<FilePosition> factory() {
        return new Factory<FilePosition>() {
            @Override
            public String identifier() {
                return "position";
            }

            @Override
            public PersistProcessor<FilePosition> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistPositionProcessor();
            }
        };
    }
}
