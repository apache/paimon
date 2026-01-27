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
        property = Transform.FIELD_NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = FieldTransform.class, name = FieldTransform.NAME),
    @JsonSubTypes.Type(value = CastTransform.class, name = CastTransform.NAME),
    @JsonSubTypes.Type(value = ConcatTransform.class, name = ConcatTransform.NAME),
    @JsonSubTypes.Type(value = ConcatWsTransform.class, name = ConcatWsTransform.NAME),
    @JsonSubTypes.Type(value = UpperTransform.class, name = UpperTransform.NAME),
    @JsonSubTypes.Type(value = LowerTransform.class, name = LowerTransform.NAME)
})
public interface Transform extends Serializable {
    String FIELD_NAME = "name";

    String name();

    List<Object> inputs();

    DataType outputType();

    Object transform(InternalRow row);

    Transform copyWithNewInputs(List<Object> inputs);
}
