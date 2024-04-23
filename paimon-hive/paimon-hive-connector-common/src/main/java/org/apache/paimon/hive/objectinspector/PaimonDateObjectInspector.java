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

import org.apache.paimon.utils.DateTimeUtils;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.sql.Date;
import java.time.LocalDate;

/** {@link AbstractPrimitiveJavaObjectInspector} for DATE type. */
public class PaimonDateObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements DateObjectInspector, WriteableObjectInspector {

    public PaimonDateObjectInspector() {
        super(TypeInfoFactory.dateTypeInfo);
    }

    @Override
    public Date getPrimitiveJavaObject(Object o) {
        // Paimon stores date as an integer (epoch day, 1970-01-01 = day 0)
        // while constructor of Date accepts epoch millis
        return o == null ? null : DateTimeUtils.toSQLDate((Integer) o);
    }

    @Override
    public DateWritable getPrimitiveWritableObject(Object o) {
        Date date = getPrimitiveJavaObject(o);
        return date == null ? null : new DateWritable(date);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof Date) {
            Date date = (Date) o;
            return new Date(date.getTime());
        } else {
            return o;
        }
    }

    @Override
    public Integer convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return DateTimeUtils.toInternal((Date) value);
        } else if (value instanceof DateWritable) {
            return (((DateWritable) value).getDays());
        } else {
            return DateTimeUtils.toInternal((LocalDate) value);
        }
    }
}
