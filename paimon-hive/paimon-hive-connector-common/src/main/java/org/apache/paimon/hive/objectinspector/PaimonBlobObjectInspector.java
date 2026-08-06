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

package org.apache.paimon.hive.objectinspector;

import org.apache.paimon.data.Blob;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/** {@link AbstractPrimitiveJavaObjectInspector} for BLOB type. */
public class PaimonBlobObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements BinaryObjectInspector, WriteableObjectInspector {

    public PaimonBlobObjectInspector() {
        super(TypeInfoFactory.binaryTypeInfo);
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
        if (o == null) {
            return null;
        }
        return o instanceof Blob ? ((Blob) o).toData() : (byte[]) o;
    }

    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
        byte[] bytes = getPrimitiveJavaObject(o);
        return bytes == null ? null : new BytesWritable(bytes);
    }

    @Override
    public Blob convert(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof Blob ? (Blob) value : Blob.fromData((byte[]) value);
    }
}
