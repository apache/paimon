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

package org.apache.paimon.predicate;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.List;

/** Represents a transform function. */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = Transform.Types.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = FieldTransform.class, name = Transform.Types.FIELD),
    @JsonSubTypes.Type(value = CastTransform.class, name = Transform.Types.CAST),
    @JsonSubTypes.Type(value = ConcatTransform.class, name = Transform.Types.CONCAT),
    @JsonSubTypes.Type(value = ConcatWsTransform.class, name = Transform.Types.CONCAT_WS),
    @JsonSubTypes.Type(value = UpperTransform.class, name = Transform.Types.UPPER)
})
public interface Transform extends Serializable {

    String name();

    List<Object> inputs();

    DataType outputType();

    Object transform(InternalRow row);

    Transform copyWithNewInputs(List<Object> inputs);

    /** Types for transform. */
    class Types {
        public static final String FIELD_TYPE = "type";
        public static final String FIELD = "field";
        public static final String CAST = "cast";
        public static final String CONCAT = "concat";
        public static final String CONCAT_WS = "concat_ws";
        public static final String UPPER = "upper";

        private Types() {}
    }
}
