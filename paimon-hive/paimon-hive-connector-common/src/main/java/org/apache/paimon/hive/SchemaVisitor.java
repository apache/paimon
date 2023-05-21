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

package org.apache.paimon.hive;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import java.util.List;

/**
 * SchemaVisitor to visitor the sourceInspector and writerInspector to get the FieldDeserializer.
 */
public abstract class SchemaVisitor<P, R> {

    /** PartnerAccessors. */
    public interface PartnerAccessors<P> {

        P fieldPartner(P partnerRow, String name);

        P mapKeyPartner(P partnerMap);

        P mapValuePartner(P partnerMap);

        P listPartner(P partnerList);
    }

    public static <P, T> T visit(
            HiveSchema schema,
            P partner,
            SchemaVisitor<P, T> visitor,
            PartnerAccessors<P> accessors) {
        return visit(schema.rowType(), partner, visitor, accessors);
    }

    public static <P, T> T visit(
            DataType type, P partner, SchemaVisitor<P, T> visitor, PartnerAccessors<P> accessors) {
        switch (type.getTypeRoot()) {
            case ROW:
                List<DataField> fields = ((RowType) type).getFields();
                List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
                for (DataField field : fields) {
                    P fieldPartner =
                            partner != null ? accessors.fieldPartner(partner, field.name()) : null;
                    T result;
                    result = visit(field.type(), fieldPartner, visitor, accessors);

                    results.add(result);
                }
                return visitor.rowType((RowType) type, partner, results);

            case ARRAY:
                ArrayType list = (ArrayType) type;
                T result;
                DataType elementType = list.getElementType();
                P partnerElement = partner != null ? accessors.listPartner(partner) : null;
                result = visit(elementType, partnerElement, visitor, accessors);

                return visitor.list(list, partner, result);

            case MAP:
                MapType map = (MapType) type;
                T keyResult;
                T valueResult;

                P keyPartner = partner != null ? accessors.mapKeyPartner(partner) : null;
                keyResult = visit(map.getKeyType(), keyPartner, visitor, accessors);

                P valuePartner = partner != null ? accessors.mapValuePartner(partner) : null;
                valueResult = visit(map.getValueType(), valuePartner, visitor, accessors);
                return visitor.map(map, partner, keyResult, valueResult);

            default:
                return visitor.primitive(type, partner);
        }
    }

    public R rowType(RowType rowType, P partner, List<R> fieldResults) {
        return null;
    }

    public R list(ArrayType list, P partner, R elementResult) {
        return null;
    }

    public R map(MapType map, P partner, R keyResult, R valueResult) {
        return null;
    }

    public R primitive(DataType primitive, P partner) {
        return null;
    }
}
