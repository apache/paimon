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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/** {@link AbstractPrimitiveJavaObjectInspector} for STRING type. */
public class PaimonStringObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements StringObjectInspector, WriteableObjectInspector {

    public PaimonStringObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo);
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {
        return o == null ? null : o.toString();
    }

    @Override
    public Text getPrimitiveWritableObject(Object o) {
        String s = getPrimitiveJavaObject(o);
        return s == null ? null : new Text(s);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof BinaryString) {
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
