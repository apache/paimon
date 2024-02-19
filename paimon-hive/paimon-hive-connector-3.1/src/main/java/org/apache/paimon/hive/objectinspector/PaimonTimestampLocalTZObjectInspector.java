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

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.time.ZoneId;

/**
 * {@link AbstractPrimitiveJavaObjectInspector} for TIMESTAMP WITH LOCAL TIME ZONE type. The
 * precision is maintained.
 */
public class PaimonTimestampLocalTZObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements TimestampLocalTZObjectInspector, WriteableObjectInspector {

    public PaimonTimestampLocalTZObjectInspector() {
        super(TypeInfoFactory.timestampLocalTZTypeInfo);
    }

    @Override
    public TimestampTZ getPrimitiveJavaObject(Object o) {
        if (o == null) {
            return null;
        }

        return new TimestampTZ(((Timestamp) o).toLocalDateTime().atZone(ZoneId.systemDefault()));
    }

    @Override
    public TimestampLocalTZWritable getPrimitiveWritableObject(Object o) {
        TimestampTZ timestampTZ = getPrimitiveJavaObject(o);
        return timestampTZ == null ? null : new TimestampLocalTZWritable(timestampTZ);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof Timestamp) {
            // immutable
            return o;
        } else if (o instanceof TimestampTZ) {
            return new TimestampTZ(((TimestampTZ) o).getZonedDateTime());
        } else {
            return o;
        }
    }

    @Override
    public Timestamp convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof TimestampTZ) {
            return Timestamp.fromLocalDateTime(
                    ((TimestampTZ) value).getZonedDateTime().toLocalDateTime());
        } else {
            return null;
        }
    }
}
