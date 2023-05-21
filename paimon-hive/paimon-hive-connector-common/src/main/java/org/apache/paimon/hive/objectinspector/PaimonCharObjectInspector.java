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

import org.apache.paimon.data.BinaryString;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/** {@link AbstractPrimitiveJavaObjectInspector} for CHAR type. */
public class PaimonCharObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements HiveCharObjectInspector, WriteableObjectInspector {

    private final int len;

    public PaimonCharObjectInspector(int len) {
        super(TypeInfoFactory.getCharTypeInfo(len));
        this.len = len;
    }

    @Override
    public HiveChar getPrimitiveJavaObject(Object o) {
        return o == null ? null : new HiveChar(o.toString(), len);
    }

    @Override
    public HiveCharWritable getPrimitiveWritableObject(Object o) {
        HiveChar hiveChar = getPrimitiveJavaObject(o);
        return hiveChar == null ? null : new HiveCharWritable(hiveChar);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof HiveChar) {
            HiveChar hiveChar = (HiveChar) o;
            return new HiveChar(hiveChar, len);
        } else if (o instanceof BinaryString) {
            return BinaryString.fromString(o.toString());
        } else {
            return o;
        }
    }

    @Override
    public BinaryString convert(Object value) {
        return value == null ? null : BinaryString.fromString(value.toString());
    }
}
