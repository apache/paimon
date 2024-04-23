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

import org.apache.paimon.data.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.time.LocalDateTime;

/** {@link AbstractPrimitiveJavaObjectInspector} for TIMESTAMP type. The precision is maintained. */
public class PaimonTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements TimestampObjectInspector, WriteableObjectInspector {

    public PaimonTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    @Override
    public java.sql.Timestamp getPrimitiveJavaObject(Object o) {
        return o == null ? null : ((Timestamp) o).toSQLTimestamp();
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
        java.sql.Timestamp ts = getPrimitiveJavaObject(o);
        return ts == null ? null : new TimestampWritable(ts);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof Timestamp) {
            // immutable
            return o;
        } else if (o instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) o;
            return new java.sql.Timestamp(timestamp.getTime());
        } else {
            return o;
        }
    }

    @Override
    public Timestamp convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Timestamp) {
            return Timestamp.fromSQLTimestamp((java.sql.Timestamp) value);
        } else if (value instanceof TimestampWritable) {
            return Timestamp.fromSQLTimestamp(((TimestampWritable) value).getTimestamp());
        } else {
            return Timestamp.fromLocalDateTime((LocalDateTime) value);
        }
    }
}
