/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive.objectinspector;

import org.apache.flink.table.store.data.Decimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/** {@link AbstractPrimitiveJavaObjectInspector} for DECIMAL type. */
public class TableStoreDecimalObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements HiveDecimalObjectInspector {

    public TableStoreDecimalObjectInspector(int precision, int scale) {
        super(TypeInfoFactory.getDecimalTypeInfo(precision, scale));
    }

    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o) {
        return o == null ? null : HiveDecimal.create(((Decimal) o).toBigDecimal());
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
        HiveDecimal decimal = getPrimitiveJavaObject(o);
        return decimal == null ? null : new HiveDecimalWritable(decimal);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof Decimal) {
            return ((Decimal) o).copy();
        } else if (o instanceof HiveDecimal) {
            HiveDecimal hiveDecimal = (HiveDecimal) o;
            return HiveDecimal.create(hiveDecimal.bigDecimalValue());
        } else {
            return o;
        }
    }
}
