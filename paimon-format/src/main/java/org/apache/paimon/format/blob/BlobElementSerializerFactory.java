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

package org.apache.paimon.format.blob;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.utils.Preconditions;

/** Selects a blob element serializer for a field type. */
final class BlobElementSerializerFactory {

    private BlobElementSerializerFactory() {}

    static boolean supports(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BLOB:
                return true;
            case ARRAY:
                return ((ArrayType) dataType).getElementType().getTypeRoot() == DataTypeRoot.BLOB;
            case MAP:
                MapType mapType = (MapType) dataType;
                return MapBlobElementSerializer.supportKeyType(mapType.getKeyType())
                        && mapType.getValueType().getTypeRoot() == DataTypeRoot.BLOB;
            default:
                return false;
        }
    }

    static BlobElementSerializer create(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BLOB:
                return new RawBlobElementSerializer();
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Preconditions.checkArgument(
                        arrayType.getElementType().getTypeRoot() == DataTypeRoot.BLOB);
                return new ArrayBlobElementSerializer();
            case MAP:
                MapType mapType = (MapType) dataType;
                Preconditions.checkArgument(
                        MapBlobElementSerializer.supportKeyType(mapType.getKeyType()),
                        "Unsupported key type [%s] for nested Map-Blob type.",
                        mapType.getKeyType());
                Preconditions.checkArgument(
                        mapType.getValueType().getTypeRoot() == DataTypeRoot.BLOB,
                        "Map-Blob value type must be BLOB, but is [%s].",
                        mapType.getValueType());
                return new MapBlobElementSerializer(mapType.getKeyType());
            default:
                throw new RuntimeException("Unsupported Element Type for Blob File Format");
        }
    }
}
